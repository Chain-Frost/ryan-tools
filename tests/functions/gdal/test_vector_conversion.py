"""Tests for reusable GDAL vector conversion helpers."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

from ryan_library.functions.gdal.vector_conversion import (
    get_unique_attribute_values,
    get_vector_layer_names,
    resolve_vector_format,
    translate_vector_dataset,
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


def test_resolve_vector_format_accepts_names_and_extensions() -> None:
    assert resolve_vector_format("GPKG")[0] == "gpkg"
    assert resolve_vector_format(".fgb")[0] == "fgb"

    with pytest.raises(ValueError, match="Unsupported vector format"):
        resolve_vector_format("invalid")


def test_get_vector_layer_names_preserves_source_order(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    _create_gdb(source, ("roads", "structures"))

    assert get_vector_layer_names(source) == ["roads", "structures"]


def test_translate_vector_dataset_publishes_completed_output(tmp_path: Path) -> None:
    source = tmp_path / "source.gdb"
    output = tmp_path / "roads.fgb"
    _create_gdb(source, ("roads",))

    published = translate_vector_dataset(source, output, vector_format="fgb", layer_name="roads")

    assert output in published
    assert output.is_file()
    assert not list(tmp_path.glob(".*.converting-*"))

    with pytest.raises(FileExistsError):
        translate_vector_dataset(source, output, vector_format="fgb", layer_name="roads")


def test_unique_values_and_filtered_translation(tmp_path: Path) -> None:
    source = tmp_path / "source.gpkg"
    driver = ogr.GetDriverByName("GPKG")
    dataset = driver.CreateDataSource(str(source))
    layer = dataset.CreateLayer("assets", geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("Owner", ogr.OFTString))
    for index, owner in enumerate(("Alpha", "O'Brien", "Alpha")):
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("Owner", owner)
        geometry = ogr.Geometry(ogr.wkbPoint)
        geometry.AddPoint_2D(float(index), float(index))
        feature.SetGeometry(geometry)
        assert layer.CreateFeature(feature) == 0
    dataset = None

    assert get_unique_attribute_values(source, "assets", "Owner") == ["Alpha", "O'Brien"]

    output = tmp_path / "obrien.gpkg"
    translate_vector_dataset(
        source,
        output,
        vector_format="gpkg",
        layer_name="assets",
        where="\"Owner\" = 'O''Brien'",
    )
    output_dataset = ogr.Open(str(output))
    assert output_dataset.GetLayer(0).GetFeatureCount() == 1
    output_dataset = None
