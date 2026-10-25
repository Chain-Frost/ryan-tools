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


def test_bc_sources_shared_by_columns_keep_every_run_dependency(tmp_path: Path) -> None:
    from pytuflow import TCF

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    (tmp_path / "events.tef").write_text(
        "Define Event == A\n BC Event Source == _source_ | present\nEnd Define\n"
        "Define Event == B\n BC Event Source == _source_ | missing\nEnd Define\n"
        "Define Event == P1\n BC Event Source == _column_ | pattern1\nEnd Define\n"
        "Define Event == P2\n BC Event Source == _column_ | pattern2\nEnd Define\n",
        encoding="utf-8",
    )
    (tmp_path / "bc_dbase.csv").write_text(
        "Name,Source,Column 1,Column 2\nrain,rain__source_.csv,Time,_column_\n", encoding="utf-8"
    )
    (tmp_path / "rain_present.csv").write_text("Time,pattern1,pattern2\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Event File == events.tef\nBC Database == bc_dbase.csv\n", encoding="utf-8")
    runs = [
        preflight.RunRequest(i, {"e1": source, "e2": column})
        for i, (source, column) in enumerate((("A", "P1"), ("A", "P2"), ("B", "P1"), ("B", "P2")), 1)
    ]
    result = preflight.check_batch_files(tcf, runs)
    assert result.run_count == 4
    assert result.checked_paths == {
        str(tmp_path / name) for name in ("events.tef", "bc_dbase.csv", "rain_present.csv", "rain_missing.csv")
    }
    assert not result.passed
    assert {issue.run_index for issue in result.issues} == {3, 4}
    assert all(issue.reference.endswith("rain_missing.csv") for issue in result.issues)


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


def test_batch_headers_open_once_and_route_missing_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from pytuflow import TCF

    from ryan_library.functions.tuflow import dependency_index
    from ryan_library.functions.tuflow.csv_headers import CsvHeader

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    (tmp_path / "bc.csv").write_text("Name,Source,Column 1,Column 2\nrain,rain.csv,Time,<<~e1~>>\n", encoding="utf-8")
    (tmp_path / "rain.csv").write_text("Time,Good\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text("BC Database == bc.csv\n", encoding="utf-8")
    calls: list[Path] = []
    original = dependency_index.read_csv_header

    def observe(path: Path) -> CsvHeader:
        calls.append(path)
        return original(path)

    monkeypatch.setattr(dependency_index, "read_csv_header", observe)
    result = preflight.check_batch_files(
        tcf, [preflight.RunRequest(i, {"e1": col}) for i, col in enumerate(("Good", "Bad", "Bad"), 1)]
    )
    assert calls == [tmp_path / "rain.csv"]
    assert result.csv_headers_checked == 1
    assert result.csv_column_selections_checked == 3
    assert {issue.run_index for issue in result.issues} == {2, 3}
    assert all("missing required column" in issue.detail and "Bad" in issue.detail for issue in result.issues)
    # Invocation-local cache: changing the header must be observed next time.
    (tmp_path / "rain.csv").write_text("Time,Bad\n", encoding="utf-8")
    assert preflight.check_batch_files(tcf, [preflight.RunRequest(1, {"e1": "Bad"})]).passed


def test_480_column_selections_share_24_header_reads(tmp_path: Path) -> None:
    from pytuflow import TCF

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    header = "Time," + ",".join(f"P{i}" for i in range(20)) + "\n"
    for i in range(24):
        (tmp_path / f"rain_F{i}.csv").write_text(header, encoding="utf-8")
    (tmp_path / "bc.csv").write_text(
        "Name,Source,Column 1,Column 2\nrain,rain_<<~e1~>>.csv,Time,<<~e2~>>\n", encoding="utf-8"
    )
    tcf = tmp_path / "model.tcf"
    tcf.write_text("BC Database == bc.csv\n", encoding="utf-8")
    runs = [preflight.RunRequest(i * 20 + j + 1, {"e1": f"F{i}", "e2": f"P{j}"}) for i in range(24) for j in range(20)]
    result = preflight.check_batch_files(tcf, runs)
    assert result.passed
    assert result.csv_headers_checked == 24
    assert result.csv_column_selections_checked == 480


def test_selected_multi_database_columns_reach_header_checks(tmp_path: Path) -> None:
    import shapefile
    from pytuflow import TCF

    if not callable(getattr(TCF, "collect_file_dependencies", None)):
        pytest.skip("Requires matching fragment-capable PyTUFLOW build")
    for label in ("A", "B"):
        shape = tmp_path / f"2d_rf_{label}.shp"
        with shapefile.Writer(str(shape), shapeType=shapefile.POLYLINE) as writer:
            writer.field("Name", "C")
            writer.line([[[0, 0], [1, 1]]])
            writer.record("rain")
        (tmp_path / f"bc_{label}.csv").write_text(
            f"Name,Source,Column 1,Column 2\nrain,rain.csv,Time,{label}\n", encoding="utf-8"
        )
    (tmp_path / "rain.csv").write_text("Time,A\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text(
        "BC Database == bc_A.csv\nRead GIS RF == 2d_rf_A.shp\nBC Database == bc_B.csv\nRead GIS RF == 2d_rf_B.shp\n",
        encoding="utf-8",
    )
    result = preflight.check_batch_files(tcf, [preflight.RunRequest(1, {"e1": "E"})])
    assert result.csv_headers_checked == 1
    assert result.csv_column_selections_checked == 2
    assert len(result.issues) == 1
    assert "missing required column(s): 'B'" in result.issues[0].detail
