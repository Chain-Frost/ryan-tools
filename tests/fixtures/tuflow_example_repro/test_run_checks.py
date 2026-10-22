"""Regression coverage for physical identity versus network-path I/O spelling."""

import sqlite3
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
    original = Path.resolve

    def resolve(path: Path, strict: bool = False) -> Path:
        return alias if path == database else original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)
    run = RunRequest(1, {})
    reference = _FileReference(run, "Read GIS Code", database, "2d_code_test")

    def references(
        _tcf: Path, _runs: list[RunRequest], _model: object, _report: PreflightReport
    ) -> tuple[list[_FileReference], dict[Path, set[str]]]:
        return [reference], {database: {"2d_code_test"}}

    def factory(_path: str) -> object:
        return object()

    monkeypatch.setattr(run_checks, "_collect_references", references)
    keys, stats = run_checks.collect(tmp_path / "model.tcf", [run], "eager", factory)
    assert stats["inventory_issues"] == 0
    assert {key[3] for key in keys} == {str(alias).casefold()}
