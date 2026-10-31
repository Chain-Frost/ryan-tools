"""Focused public-API integration and fail-closed PyTUFLOW input checks."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from ryan_library.functions.tuflow.pytuflow_compatibility import require_validation_api
from ryan_library.orchestrators.tuflow import input_preflight
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs


def _public_api() -> None:
    """A skipped real smoke is not acceptance; CI calls the guard before pytest."""
    pytest.importorskip("pytuflow.batch_validation")
    require_validation_api()


def _gpkg(path: Path, layer: str) -> None:
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY)")
        database.execute("INSERT INTO gpkg_contents VALUES (?)", (layer,))


def test_real_public_batch_validation_checks_run_specific_missing_files(tmp_path: Path) -> None:
    _public_api()
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read Grid Zpts == grid_<<~s1~>>.tif\n", encoding="utf-8")
    (tmp_path / "grid_DEV.tif").write_bytes(b"not a raster header")
    runs = [RunRequest(17, {"s1": "DEV"}), RunRequest(42, {"s1": "EXG"})]
    report = check_simulation_inputs(tcf, runs)
    assert not report.passed
    assert report.run_count == report.completed_runs == 2
    assert {issue.run_index for issue in report.issues} == {42}
    assert {issue.status for issue in report.issues} == {"missing"}
    assert report.issues[0].path == tmp_path / "grid_EXG.tif"
    assert report.files_examined >= 1
    # A present non-raster is sufficient: local raster header/pixel checking is removed.
    assert check_simulation_inputs(tcf, runs[:1]).passed
    report_file = report.write_csv(tmp_path / "missing.csv")
    assert "grid_EXG.tif" in report_file.read_text(encoding="utf-8-sig")
    with pytest.raises(FileExistsError):
        report.write_csv(report_file)


def test_real_public_gpkg_selected_layer_and_provenance(tmp_path: Path) -> None:
    _public_api()
    gpkg = tmp_path / "model.gpkg"
    _gpkg(gpkg, "other_layer")
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read GIS Code == model.gpkg >> 2d_code\n", encoding="utf-8")
    report = check_simulation_inputs(tcf, [RunRequest(8, {"s1": "EXG"})])
    assert not report.passed
    assert any(issue.run_index == 8 and issue.kind == "layer" and issue.status == "missing" for issue in report.issues)
    assert any(issue.layer == "2d_code" and issue.owner_control_file is not None for issue in report.issues)


def test_real_public_bc_csv_header_option_is_explicitly_enabled(tmp_path: Path) -> None:
    _public_api()
    (tmp_path / "bc.csv").write_text(
        "Name,Source,Column 1,Column 2\nflow,inflow.csv,Time,Flow\n", encoding="utf-8"
    )
    (tmp_path / "inflow.csv").write_text("Time,WrongColumn\n0,10\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text("BC Database == bc.csv\n", encoding="utf-8")
    report = check_simulation_inputs(tcf, [RunRequest(3, {"e1": "Q100"})])
    assert not report.passed
    assert report.csv_headers_checked == 1
    assert any(issue.kind == "csv_header" and issue.run_index == 3 for issue in report.issues)


def test_real_public_gis_linked_source_is_checked(tmp_path: Path) -> None:
    _public_api()
    gpkg = tmp_path / "links.gpkg"
    with sqlite3.connect(gpkg) as database:
        database.execute("CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY)")
        database.execute("INSERT INTO gpkg_contents VALUES ('1d_xs_link')")
        database.execute(
            "CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT)"
        )
        database.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_xs_link', 'geom', 'LINESTRING')")
        database.execute('CREATE TABLE "1d_xs_link" (fid INTEGER PRIMARY KEY, geom BLOB, Source TEXT)')
        database.execute('INSERT INTO "1d_xs_link" (Source) VALUES ("missing_xs.csv")')
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read GIS Code == links.gpkg >> 1d_xs_link\n", encoding="utf-8")
    report = check_simulation_inputs(tcf, [RunRequest(33, {"s1": "EXG"})])
    assert not report.passed
    assert any(issue.run_index == 33 and issue.reference.endswith("missing_xs.csv") for issue in report.issues)
    assert report.gis_sources_added >= 1


def _fake_result(
    positions: list[int],
    *,
    findings: list[SimpleNamespace] | None = None,
    passed: bool = True,
    physical_count: int | None = None,
) -> SimpleNamespace:
    findings = findings or []
    return SimpleNamespace(
        dependencies=SimpleNamespace(
            runs=[SimpleNamespace(index=index, error=None) for index in positions], passed=True
        ),
        validation=SimpleNamespace(
            run_count=len(positions) if physical_count is None else physical_count,
            findings=tuple(findings), passed=passed, files_examined=1,
            geopackages_examined=0, csv_headers_examined=0, directories_scanned=0,
        ),
        passed=passed, parse_seconds=0.1, collection_seconds=0.2, inspection_seconds=0.3,
        gis_sources_added=0, gis_files_scanned=0,
    )


def _finding(position: int, path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        run_index=position, flags="-s1 Future Works", status="missing", kind="layer",
        path=path, detail="Missing selected layer", layer="design", origin="Read GIS Code == file.gpkg >> design",
        input_uuid="uuid-1", command_index=4, owner_control_file=path.with_suffix(".tgc"),
        database_kind="bc", source_pattern=path, csv_columns=("Time", "Flow"),
    )


def test_structured_flags_spaces_and_reordered_public_results_are_mapped_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "file.gpkg"
    seen: list[object] = []

    def validate(model: object, flags: list[dict[str, str]], *, options: object) -> object:
        seen.extend((model, flags, options))
        return _fake_result([1, 0], findings=[_finding(1, missing)], passed=False)

    def options(**kwargs: object) -> object:
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        input_preflight, "require_validation_api",
        lambda: SimpleNamespace(validate_batch_inputs=validate, options_factory=options),
    )
    requests = [RunRequest(19, {"s1": "Future Works"}), RunRequest(5, {"s1": "Existing Model"})]
    report = check_simulation_inputs(tmp_path / "model.tcf", requests)
    assert seen[1] == [{"s1": "Future Works"}, {"s1": "Existing Model"}]
    assert cast(SimpleNamespace, seen[2]).check_bc_csv_headers is True
    assert report.issues[0].run_index == 5
    assert report.issues[0].owner_control_file == missing.with_suffix(".tgc")
    assert report.issues[0].csv_columns == ("Time", "Flow")
    assert not report.passed


@pytest.mark.parametrize(
    ("positions", "physical_count"),
    [([0], None), ([0, 0], None), ([0, 2], None), ([0, 1], 1)],
)
def test_incomplete_public_results_cannot_pass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, positions: list[int], physical_count: int | None
) -> None:
    def validate(_model: object, _flags: object, *, options: object) -> object:
        del options
        return _fake_result(positions, physical_count=physical_count)

    monkeypatch.setattr(
        input_preflight, "require_validation_api",
        lambda: SimpleNamespace(validate_batch_inputs=validate, options_factory=lambda **_kwargs: object()),
    )
    with pytest.raises(ValueError, match=r"incomplete|missing|duplicate|invalid"):
        check_simulation_inputs(tmp_path / "model.tcf", [RunRequest(1, {}), RunRequest(2, {})])


def test_failed_group_without_findings_cannot_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        input_preflight, "require_validation_api",
        lambda: SimpleNamespace(
            validate_batch_inputs=lambda *_args, **_kwargs: _fake_result([0], passed=False),
            options_factory=lambda **_kwargs: object(),
        ),
    )
    with pytest.raises(ValueError, match="without findings"):
        check_simulation_inputs(tmp_path / "model.tcf", [RunRequest(1, {})])


def test_importing_preflight_does_not_import_pytuflow() -> None:
    result = subprocess.run(
        [
            sys.executable, "-c",
            ("import sys; from ryan_library.orchestrators.tuflow.input_preflight import RunRequest; "
            "assert not any(k == 'pytuflow' or k.startswith('pytuflow.') for k in sys.modules)"),
        ],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
