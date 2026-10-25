"""Exact-source integration for batch existence, separate from data validation."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow import input_preflight as preflight


def test_batch_file_existence_skips_raster_validation_and_keeps_run_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pytuflow import TCF

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    # An existing file need not be a valid raster for an existence-only check.
    (tmp_path / "terrain.tif").write_bytes(b"existence only")
    (tmp_path / "geometry.tgc").write_text("Read Grid Zpts == terrain.tif\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text(
        "Geometry Control File == geometry.tgc\nIF Scenario == BAD\n Read Grid Zpts == missing.tif\nEND IF\n",
        encoding="utf-8",
    )

    def forbidden(*args: object, **kwargs: object) -> None:
        del args, kwargs
        message = "Existence checking must not validate raster data"
        raise AssertionError(message)

    monkeypatch.setattr(preflight, "inspect_raster_metadata", forbidden)
    result = preflight.check_batch_files(
        tcf, [preflight.RunRequest(1, {"s1": "GOOD"}), preflight.RunRequest(2, {"s1": "BAD"})]
    )
    assert not result.passed
    assert {issue.run_index for issue in result.issues} == {2}
    assert all(issue.reference.endswith("missing.tif") for issue in result.issues)
    assert result.raster_files_scanned == 0
    assert str(tmp_path / "terrain.tif") in result.checked_paths


def test_fragment_mode_requires_matching_parser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import pytuflow

    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read Grid Zpts == terrain.tif\n", encoding="utf-8")

    class OldModel:
        def __init__(self, source: str) -> None:
            self.source = source

    monkeypatch.setattr(pytuflow, "TCF", OldModel)
    with pytest.raises(ValueError, match="collect_file_dependencies"):
        preflight.check_batch_files(tcf, [preflight.RunRequest(1, {"s1": "GOOD"})])


def test_linked_files_and_geopackage_layers_remain_required(tmp_path: Path) -> None:
    from pytuflow import TCF

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    database = tmp_path / "sections.gpkg"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA application_id=1196444487")
        connection.execute("PRAGMA user_version=10200")
        connection.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        connection.execute("INSERT INTO gpkg_contents VALUES ('1d_xs_example')")
        connection.execute(
            "CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)"
        )
        connection.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_xs_example', 'geom', 'LINESTRING')")
        connection.execute('CREATE TABLE "1d_xs_example" (fid INTEGER PRIMARY KEY, geom BLOB, source TEXT)')
        connection.execute('INSERT INTO "1d_xs_example" (source) VALUES (?)', ("missing_section.csv",))
        connection.commit()
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read GIS Table Links == sections.gpkg >> 1d_xs_example\n", encoding="utf-8")
    result = preflight.check_batch_files(tcf, [preflight.RunRequest(i, {"e1": f"E{i}"}) for i in range(1, 5)])
    assert not result.passed
    assert result.attribute_databases_scanned == 1
    assert {issue.run_index for issue in result.issues} == {1, 2, 3, 4}
    assert all(issue.reference.endswith("missing_section.csv") for issue in result.issues)
    # The file exists, but a nonexistent requested layer must still be reported.
    tcf.write_text("Read GIS Code == sections.gpkg >> missing_code\n", encoding="utf-8")
    layers = preflight.check_batch_files(tcf, [preflight.RunRequest(1, {"e1": "E1"})])
    assert not layers.passed
    assert any("GeoPackage layer does not exist" in issue.detail for issue in layers.issues)
