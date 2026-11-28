"""Synthetic ellipsoidal intersection and normalization checks."""

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import fiona
import numpy as np
import pytest
from shapely.geometry import MultiPolygon, Polygon, box, mapping

from ryan_library.functions.hydrology.spatial_ifd import GridSpec, IFDSource, SpatialIFDGrid
from ryan_library.functions.hydrology.spatial_ifd_catchments import catchment_weights, read_catchment_polygon

GRID = GridSpec(Decimal(-23), Decimal(119), 2, 2)


def test_partial_cells_and_mean_normalization() -> None:
    # Full northern-left cell, half northern-right, half southern-left,
    # quarter southern-right. Physical weights also vary with latitude.
    geometry = box(118.9875, -23.025, 119.025, -22.9875)
    weights = catchment_weights(GRID, geometry)
    assert weights.covered_area_m2 == pytest.approx(weights.catchment_area_m2, rel=1e-6)
    assert weights.areas_m2[0, 1] / weights.areas_m2[0, 0] == pytest.approx(0.5, rel=1e-6)
    assert weights.areas_m2[1, 1] / weights.areas_m2[1, 0] == pytest.approx(0.5, rel=1e-6)
    assert weights.areas_m2[1, 0] / weights.areas_m2[0, 0] < 0.5

    grid = SpatialIFDGrid(
        GRID,
        IFDSource(("AllIFDDatasets", "Synthetic"), "fixture"),
        60,
        1,
        np.array([[100.0, 200.0], [300.0, 400.0]]),
        4,
        0,
    )
    expected = float(np.average(grid.depths_mm, weights=weights.areas_m2))
    assert weights.mean_depth(grid) == pytest.approx(expected)
    assert np.average(weights.multipliers(grid), weights=weights.areas_m2) == pytest.approx(1.0)
    shifted = replace(grid, spec=replace(GRID, west_lon=Decimal("119.025")))
    with pytest.raises(ValueError, match="aligned"):
        weights.mean_depth(shifted)


def test_multipart_holes_and_diagonal_edges() -> None:
    triangle = Polygon([(119, -23), (119.027, -23.023), (119.01, -23.028)])
    weights = catchment_weights(GRID, triangle)
    assert weights.covered_area_m2 == pytest.approx(weights.catchment_area_m2, rel=1e-6)
    assert np.count_nonzero(weights.areas_m2) >= 3

    outer = box(118.988, -23.011, 119.011, -22.988)
    hole = box(118.995, -23.005, 119.005, -22.995)
    ring = Polygon(outer.exterior.coords, [hole.exterior.coords])
    other = box(119.02, -23.03, 119.03, -23.02)
    multi = catchment_weights(GRID, MultiPolygon([ring, other]))
    assert multi.areas_m2[0, 1] == 0
    assert multi.areas_m2[1, 0] == 0
    assert multi.areas_m2[0, 0] < catchment_weights(GRID, outer).catchment_area_m2
    assert multi.areas_m2[1, 1] == pytest.approx(catchment_weights(GRID, other).catchment_area_m2, rel=1e-6)


def test_reject_partial_extent_and_invalid_polygon() -> None:
    with pytest.raises(ValueError, match="entire catchment"):
        catchment_weights(GRID, box(118.97, -23.02, 119.02, -22.99))
    invalid = Polygon([(119, -23), (119.02, -23.02), (119, -23.02), (119.02, -23)])
    with pytest.raises(ValueError, match="valid"):
        catchment_weights(GRID, invalid)


def test_vector_union_and_explicit_crs(tmp_path: Path) -> None:
    path = tmp_path / "catchment.gpkg"
    first = box(118.99, -23.01, 119.01, -22.99)
    second = box(119.02, -23.03, 119.03, -23.02)
    schema = {"geometry": "Polygon", "properties": {}}
    with fiona.open(path, "w", driver="GPKG", layer="basin", schema=schema, crs="EPSG:4326") as sink:
        sink.write({"geometry": mapping(first), "properties": {}})
        sink.write({"geometry": mapping(second), "properties": {}})
    result = read_catchment_polygon(path)
    assert result.equals(MultiPolygon([first, second]))
    assert catchment_weights(GRID, result).covered_area_m2 > 0

    wrong = tmp_path / "projected.gpkg"
    with fiona.open(wrong, "w", driver="GPKG", layer="basin", schema=schema, crs="EPSG:3577") as sink:
        sink.write({"geometry": mapping(first), "properties": {}})
    with pytest.raises(ValueError, match="WGS84"):
        read_catchment_polygon(wrong)
