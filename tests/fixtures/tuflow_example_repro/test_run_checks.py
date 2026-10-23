"""Regression coverage for physical identity versus network-path I/O spelling."""

import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.input_preflight import (
    PreflightReport,
    RunRequest,
    _FileReference,  # pyright: ignore[reportPrivateUsage] - diagnostic contract test
)
from tests.fixtures.tuflow_example_repro import run_checks


def test_physical_identity_does_not_replace_inventory_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A mapped-drive alias may resolve to an unavailable SQLite URI authority."""
    database = tmp_path / "model.gpkg"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE gpkg_contents (table_name TEXT)")
        connection.execute("INSERT INTO gpkg_contents VALUES ('2d_code_test')")
    alias = Path("//synthetic-server/share/model.gpkg")
    alternate_spelling = Path("//SYNTHETIC-SERVER/share/model.gpkg")
    original = Path.resolve
    resolutions: Counter[str] = Counter()

    def resolve(path: Path, strict: bool = False) -> Path:
        spelling = str(path)
        if spelling in {str(database), str(alternate_spelling), str(alias)}:
            resolutions[spelling] += 1
            return alias
        return original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)
    run = RunRequest(1, {})
    found = [
        _FileReference(run, "Read GIS Code", path, "2d_code_test")
        for path in (database, alternate_spelling, alias, database, alternate_spelling, alias)
    ]

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return found, {database: {"2d_code_test"}}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    keys, stats = run_checks.collect(tmp_path / "model.tcf", [run], "eager", factory)
    assert stats["inventory_issues"] == 0
    assert stats["files"] == 1
    assert {key[3] for key in keys} == {str(alias).casefold()}
    assert resolutions == {str(database): 1, str(alternate_spelling): 1, str(alias): 1}


def test_physical_identity_resolves_each_spelling_once_for_all_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deduplicate physical work without losing any run/command provenance."""
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")
    (tmp_path / "subdirectory").mkdir()
    alternate = tmp_path / "subdirectory" / ".." / "source.txt"
    original = Path.resolve
    resolutions: Counter[str] = Counter()
    spellings = {str(source), str(alternate)}

    def resolve(path: Path, strict: bool = False) -> Path:
        if str(path) in spellings:
            resolutions[str(path)] += 1
        return original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)
    runs = [RunRequest(index, {"e1": f"TP{index:02d}"}) for index in range(1, 6)]

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [
            _FileReference(run, origin, path)
            for run in runs
            for origin, path in (("Read GIS Code", source), ("Read Grid Zpts", alternate))
        ], {}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    keys, stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
    assert resolutions == {str(source): 1, str(alternate): 1}
    assert stats["inventory_issues"] == 0
    assert stats["files"] == 1
    assert len(keys) == len(runs) * 2
    assert {(key[0], key[2]) for key in keys} == {
        (run.index, origin) for run in runs for origin in ("read gis code", "read grid zpts")
    }
    assert {key[3] for key in keys} == {str(source).casefold()}


def test_new_collection_rechecks_a_changed_link_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No successful identity is retained across TCF/mode collection calls."""
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("first", encoding="utf-8")
    second.write_text("second", encoding="utf-8")
    link = tmp_path / "input.txt"
    try:
        link.symlink_to(first)
    except OSError as exc:
        pytest.skip(f"Symbolic links are unavailable in this test environment: {exc}")
    original = Path.resolve
    resolution_count = 0

    def resolve(path: Path, strict: bool = False) -> Path:
        nonlocal resolution_count
        if str(path) == str(link):
            resolution_count += 1
        return original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)
    runs = [RunRequest(index, {"e1": f"TP{index:02d}"}) for index in range(1, 4)]

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [_FileReference(run, "Read GIS Code", link) for run in runs], {}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    first_keys, first_stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
    assert resolution_count == 1
    link.unlink()
    link.symlink_to(second)
    second_keys, second_stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
    assert resolution_count == 2
    assert {key[3] for key in first_keys} == {str(first).casefold()}
    assert {key[3] for key in second_keys} == {str(second).casefold()}
    assert first_stats["inventory_issues"] == second_stats["inventory_issues"] == 0


def test_identity_resolution_failure_still_fails_collection_and_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed physical comparison must not produce an accepted partial set."""
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")
    original = Path.resolve
    resolution_count = 0

    def resolve(path: Path, strict: bool = False) -> Path:
        nonlocal resolution_count
        if str(path) == str(source):
            resolution_count += 1
            if resolution_count == 1:
                msg = "Cannot resolve physical identity"
                raise PermissionError(msg)
        return original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)
    runs = [RunRequest(index, {}) for index in range(1, 4)]

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [_FileReference(run, "Read GIS Code", source) for run in runs], {}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    with pytest.raises(PermissionError, match="Cannot resolve physical identity"):
        run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
    assert resolution_count == 1
    keys, stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
    assert resolution_count == 2
    assert len(keys) == len(runs)
    assert stats["inventory_issues"] == 0


def test_layer_enumeration_is_shared_but_each_selector_and_run_remains_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-query each spelling once on a new call, including changed DB contents."""
    database = tmp_path / "model.gpkg"
    (tmp_path / "subdirectory").mkdir()
    alternate = tmp_path / "subdirectory" / ".." / "model.gpkg"
    original = sqlite3.connect
    with closing(original(database)) as connection:
        connection.execute("CREATE TABLE gpkg_contents (table_name TEXT)")
        connection.executemany(
            "INSERT INTO gpkg_contents VALUES (?)", [(name,) for name in ("2d_code_a", "2d_code_b", "2d_mat")]
        )
        connection.commit()
    connections: list[sqlite3.Connection] = []
    io_spellings: Counter[str] = Counter()

    def connect(path: str, *, uri: bool = False) -> sqlite3.Connection:
        result = original(path, uri=uri)
        connections.append(result)
        io_spellings[path] += 1
        return result

    monkeypatch.setattr(sqlite3, "connect", connect)
    runs = [RunRequest(index, {"e1": f"TP{index:02d}"}) for index in range(1, 4)]

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [
            _FileReference(run, origin, path, selector)
            for run in runs
            for path in (database, alternate)
            for origin, selector in (
                ("Read GIS Code", "2D_CODE_*"),
                ("Read GIS Z Shape", "all"),
                ("Read GIS Mat", "2d_m?t"),
            )
        ], {}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    try:
        first_keys, first_stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
        # One enumeration per spelling, plus one separate final inventory
        # connection using the original spelling of the shared physical file.
        assert io_spellings == {database.as_uri() + "?mode=ro": 2, alternate.as_uri() + "?mode=ro": 1}
        assert {(key[0], key[2], key[4]) for key in first_keys} == {
            (run.index, origin, layer)
            for run in runs
            for origin, layers in (
                ("read gis code", ("2d_code_a", "2d_code_b")),
                ("read gis z shape", ("2d_code_a", "2d_code_b", "2d_mat")),
                ("read gis mat", ("2d_mat",)),
            )
            for layer in layers
        }
        for opened in connections[:2]:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                opened.execute("SELECT 1")
        with closing(original(database)) as connection:
            connection.execute("INSERT INTO gpkg_contents VALUES ('2d_code_c')")
            connection.commit()
        second_keys, second_stats = run_checks.collect(tmp_path / "model.tcf", runs, "eager", factory)
        assert io_spellings == {database.as_uri() + "?mode=ro": 4, alternate.as_uri() + "?mode=ro": 2}
        assert second_keys - first_keys == {
            (run.index, run.description, origin, str(database).casefold(), "2d_code_c")
            for run in runs
            for origin in ("read gis code", "read gis z shape")
        }
        assert first_keys <= second_keys
        assert first_stats["inventory_issues"] == second_stats["inventory_issues"] == 0
        for opened in connections[3:5]:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                opened.execute("SELECT 1")
    finally:
        for opened in connections:
            opened.close()


def test_failed_layer_enumeration_closes_connection_and_does_not_certify_partial_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "model.gpkg"
    original = sqlite3.connect
    original(database).close()
    connections: list[sqlite3.Connection] = []

    def connect(path: str, *, uri: bool = False) -> sqlite3.Connection:
        result = original(path, uri=uri)
        connections.append(result)
        return result

    monkeypatch.setattr(sqlite3, "connect", connect)
    run = RunRequest(1, {})

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [
            _FileReference(run, "Read GIS Code", database, "all"),
            _FileReference(run, "Read GIS Mat", database, "2d_*"),
        ], {}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    try:
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            run_checks.collect(tmp_path / "model.tcf", [run], "eager", factory)
        assert len(connections) == 1
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connections[0].execute("SELECT 1")
        with closing(original(database)) as connection:
            connection.execute("CREATE TABLE gpkg_contents (table_name TEXT)")
            connection.execute("INSERT INTO gpkg_contents VALUES ('2d_code_test')")
            connection.commit()
        keys, stats = run_checks.collect(tmp_path / "model.tcf", [run], "eager", factory)
        assert len(connections) == 3  # failed query, fresh enumeration, final inventory
        assert {(key[2], key[4]) for key in keys} == {
            ("read gis code", "2d_code_test"),
            ("read gis mat", "2d_code_test"),
        }
        assert stats["inventory_issues"] == 0
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connections[1].execute("SELECT 1")
    finally:
        for opened in connections:
            opened.close()
