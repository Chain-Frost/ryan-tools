"""Catchment-only workflow and resilient vector input checks."""

import sqlite3
from pathlib import Path

import fiona
import pytest
from rasterio.crs import CRS
from shapely.geometry import LineString, Point, mapping

from ryan_library.orchestrators.tuflow.initialiser.catchment_workflow import (
    CatchmentConfig,
    build_catchments,
    read_interest_features,
    _publish_no_overwrite,
)


def test_line_interest_with_stale_geopackage_count_is_read(tmp_path: Path) -> None:
    path = tmp_path / "road.gpkg"
    schema: fiona.Schema = {"geometry": "LineString", "properties": {}}
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="road",
        schema=schema,
        crs_wkt=CRS.from_epsg(28351).to_wkt(),
    ) as output:
        output.write({"geometry": mapping(LineString([(0, 0), (10, 10)])), "properties": {}})
    with sqlite3.connect(path) as database:
        database.execute("UPDATE gpkg_ogr_contents SET feature_count = 0 WHERE table_name = 'road'")
        database.commit()

    crs, geometries = read_interest_features(path)

    assert crs == CRS.from_epsg(28351)
    assert len(geometries) == 1
    assert geometries[0].equals(LineString([(0, 0), (10, 10)]))


def test_point_interest_is_supported(tmp_path: Path) -> None:
    path = tmp_path / "outlets.gpkg"
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="outlets",
        schema={"geometry": "Point", "properties": {}},
        crs_wkt=CRS.from_epsg(28350).to_wkt(),
    ) as output:
        output.write({"geometry": mapping(Point(5, 6)), "properties": {}})

    crs, geometries = read_interest_features(path)

    assert crs == CRS.from_epsg(28350)
    assert geometries == [Point(5, 6)]


def test_catchments_explicit_duplicate_rasters_rejected(tmp_path: Path) -> None:
    raster = tmp_path / "source.tif"
    raster.touch()
    config = CatchmentConfig(
        terrains=(raster, raster),
        interest=tmp_path / "unused.gpkg",
        output=tmp_path / "catchments.gpkg",
        target_crs=CRS.from_epsg(28350),
    )
    with pytest.raises(ValueError, match="unique"):
        build_catchments(config)


def test_catchment_publication_does_not_replace_racing_output(tmp_path: Path) -> None:
    staged = tmp_path / "staged.gpkg.tmp"
    destination = tmp_path / "catchments.gpkg"
    staged.write_bytes(b"new complete result")
    destination.write_bytes(b"other process result")

    with pytest.raises(FileExistsError, match="protected"):
        _publish_no_overwrite(staged, destination)

    assert staged.read_bytes() == b"new complete result"
    assert destination.read_bytes() == b"other process result"


def test_catchment_publication_creates_new_output_exclusively(tmp_path: Path) -> None:
    staged = tmp_path / "staged.gpkg.tmp"
    destination = tmp_path / "catchments.gpkg"
    staged.write_bytes(b"complete result")

    _publish_no_overwrite(staged, destination)

    assert not staged.exists()
    assert destination.read_bytes() == b"complete result"
