"""Tests for File Geodatabase export orchestration."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from ryan_library.orchestrators.gdal.file_geodatabase_export import (
    discover_file_geodatabases,
    export_file_geodatabase,
    export_file_geodatabases,
)

gdal: Any = importlib.import_module("osgeo.gdal")
ogr: Any = importlib.import_module("osgeo.ogr")
gdal.UseExceptions()


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


def test_discover_file_geodatabases_accepts_roots_and_explicit_gdbs(tmp_path: Path) -> None:
    first = tmp_path / "first.gdb"
    second = tmp_path / "nested" / "second.gdb"
    second.parent.mkdir()
    _create_gdb(first, ("roads",))
    _create_gdb(second, ("structures",))

    assert discover_file_geodatabases([tmp_path, first]) == [first.resolve(), second.resolve()]


def test_export_file_geodatabase_defaults_to_separate_layer_files(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    output_root = tmp_path / "outputs"
    _create_gdb(source, ("roads", "structures"))

    result = export_file_geodatabase(source, output_root)

    assert (result.converted, result.skipped, result.errors) == (2, 0, ())
    assert sorted(path.name for path in (output_root / "source").glob("*.gpkg")) == [
        "roads.gpkg",
        "structures.gpkg",
    ]


def test_export_file_geodatabase_can_retain_one_database(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    output_root = tmp_path / "outputs"
    _create_gdb(source, ("roads", "structures"))

    result = export_file_geodatabase(source, output_root, single_database=True)

    assert (result.converted, result.skipped, result.errors) == (1, 0, ())
    output_dataset = gdal.OpenEx(str(output_root / "source.gpkg"), gdal.OF_VECTOR)
    assert output_dataset is not None
    assert output_dataset.GetLayerCount() == 2
    output_dataset = None


def test_export_file_geodatabases_returns_batch_summary(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    output_root = tmp_path / "outputs"
    _create_gdb(source, ("roads", "structures"))

    summary = export_file_geodatabases(source, output_root, max_workers=1)

    assert summary.source_count == 1
    assert summary.converted == 2
    assert summary.skipped == 0
    assert summary.succeeded is True
