"""Focused tests for the promoted standalone utilities."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATHS = {
    "check_flt_tif": REPO_ROOT / "ryan-scripts" / "gdal-python" / "check_flt_tif.py",
    "gdalwarp_clip_to_polygon": REPO_ROOT / "ryan-scripts" / "gdal-python" / "gdalwarp_clip_to_polygon.py",
    "remove_excel_protection": REPO_ROOT / "ryan-scripts" / "misc-python" / "remove_excel_protection.py",
    "archive_extract": REPO_ROOT / "ryan-scripts" / "file-management-python" / "archive_extract.py",
    "convert_gdb_to_gpkg": REPO_ROOT / "ryan-scripts" / "gdal-python" / "convert_gdb_to_gpkg.py",
}
gdal: Any = importlib.import_module("osgeo.gdal")
ogr: Any = importlib.import_module("osgeo.ogr")
gdal.UseExceptions()


def _load_script(name: str) -> ModuleType:
    script_path = SCRIPT_PATHS[name]
    spec = importlib.util.spec_from_file_location(f"test_{name}", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _change_working_directory_succeeds(*, target_dir: Path) -> bool:
    del target_dir
    return True


def _gdalwarp_fails(
    input_file: Path,
    output_file: Path,
    cutline_path: Path,
    cutline_layer: str,
    crs: str,
) -> bool:
    del input_file, output_file, cutline_path, cutline_layer, crs
    return False


def _select_cutline_layer_succeeds(cutline_path: Path) -> str:
    del cutline_path
    return "clip"


def _excel_processing_fails(file_path: Path, output_dir: Path) -> bool:
    del file_path, output_dir
    return False


def _create_gdb(path: Path, layer_names: tuple[str, ...]) -> None:
    driver = ogr.GetDriverByName("OpenFileGDB")
    dataset = driver.CreateDataSource(str(path))
    if dataset is None:
        raise RuntimeError("Could not create synthetic File Geodatabase")

    for index, layer_name in enumerate(layer_names):
        layer = dataset.CreateLayer(layer_name, geom_type=ogr.wkbPoint)
        feature = ogr.Feature(layer.GetLayerDefn())
        geometry = ogr.Geometry(ogr.wkbPoint)
        geometry.AddPoint_2D(float(index), float(index))
        feature.SetGeometry(geometry)
        if layer.CreateFeature(feature) != 0:
            raise RuntimeError(f"Could not add a feature to {layer_name}")

    dataset = None


def test_check_flt_tif_matches_counterpart_case_insensitively(tmp_path: Path) -> None:
    script = _load_script("check_flt_tif")
    primary = tmp_path / "Result.FLT"
    secondary = tmp_path / "RESULT.TIF"
    primary.write_text("primary", encoding="utf-8")
    secondary.write_text("secondary", encoding="utf-8")

    result = script.check_files_in_directory((tmp_path, ".flt", ".tif"))

    assert result == []


def test_gdalwarp_selects_only_cutline_layer(tmp_path: Path) -> None:
    script = _load_script("gdalwarp_clip_to_polygon")
    source = tmp_path / "cutlines.gdb"
    _create_gdb(source, ("boundary",))

    assert script.select_cutline_layer(source) == "boundary"


def test_gdalwarp_selects_filename_matched_cutline_layer(tmp_path: Path) -> None:
    script = _load_script("gdalwarp_clip_to_polygon")
    source = tmp_path / "cutlines.gdb"
    _create_gdb(source, ("other", "CUTLINES"))

    assert script.select_cutline_layer(source) == "CUTLINES"


def test_gdalwarp_rejects_ambiguous_cutline_layers(tmp_path: Path) -> None:
    script = _load_script("gdalwarp_clip_to_polygon")
    source = tmp_path / "cutlines.gdb"
    _create_gdb(source, ("first", "second"))

    assert script.select_cutline_layer(source) is None


def test_gdalwarp_main_reports_processing_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load_script("gdalwarp_clip_to_polygon")
    (tmp_path / "input.tif").touch()
    shapefile = tmp_path / "clip.shp"
    shapefile.touch()
    monkeypatch.setattr(script, "DEFAULT_SHAPEFILE", shapefile)
    monkeypatch.setattr(script, "DEFAULT_OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(script, "change_working_directory", _change_working_directory_succeeds)
    monkeypatch.setattr(script, "select_cutline_layer", _select_cutline_layer_succeeds)
    monkeypatch.setattr(script, "run_gdalwarp", _gdalwarp_fails)

    assert script.main(input_directories=tmp_path) == 1


def test_remove_excel_protection_main_reports_processing_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = _load_script("remove_excel_protection")
    (tmp_path / "input.xlsx").touch()
    monkeypatch.setattr(script, "DEFAULT_OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(script, "change_working_directory", _change_working_directory_succeeds)
    monkeypatch.setattr(script, "process_excel_file", _excel_processing_fails)

    assert script.main(input_directories=tmp_path) == 1


def test_remove_excel_protection_writes_valid_output(tmp_path: Path) -> None:
    script = _load_script("remove_excel_protection")
    source = tmp_path / "protected.xlsx"
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    with zipfile.ZipFile(source, "w") as workbook:
        workbook.writestr("xl/workbook.xml", '<workbook><workbookProtection lockStructure="1"/></workbook>')
        workbook.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet><sheetProtection sheet="1"/><sheetData/></worksheet>',
        )

    assert script.process_excel_file(source, output_dir) is True

    output = output_dir / "protected_unprotected.xlsx"
    with zipfile.ZipFile(output) as workbook:
        assert "workbookProtection" not in workbook.read("xl/workbook.xml").decode("utf-8")
        assert "sheetProtection" not in workbook.read("xl/worksheets/sheet1.xml").decode("utf-8")


def test_archive_extract_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load_script("archive_extract")
    monkeypatch.setattr(script, "SEVEN_ZIP_EXE", None)
    archive_path = tmp_path / "example.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("payload.txt", "original")

    assert script._extract_archive(archive_path) == (archive_path, True, "")
    extracted_file = tmp_path / "example" / "payload.txt"
    extracted_file.write_text("keep this", encoding="utf-8")

    archive, success, message = script._extract_archive(archive_path)

    assert archive == archive_path
    assert success is True
    assert "already exists" in message
    assert extracted_file.read_text(encoding="utf-8") == "keep this"


def test_convert_gdb_cli_accepts_input_output_format_and_database_mode(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    output_root = tmp_path / "outputs"
    _create_gdb(source, ("roads", "structures"))
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPO_ROOT)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATHS["convert_gdb_to_gpkg"]),
            "--input-paths",
            str(source),
            "--output-directory",
            str(output_root),
            "--output-format",
            "gpkg",
            "--single-database",
            "--no-pause",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Script failed:\n{result.stderr}\n{result.stdout}"
    assert (output_root / "source.gpkg").is_file()
