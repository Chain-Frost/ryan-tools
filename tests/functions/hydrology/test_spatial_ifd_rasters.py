"""Synthetic raster interchange and comparison checks."""

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from ryan_library.functions.hydrology.spatial_ifd import GridSpec, IFDSource, SpatialIFDGrid, write_ascii_grid
from ryan_library.functions.hydrology.spatial_ifd_rasters import (
    compare_reference,
    read_reference_raster,
    weighted_depth,
    write_geotiff,
)


@pytest.fixture
def grid() -> SpatialIFDGrid:
    return SpatialIFDGrid(
        GridSpec(Decimal(-23), Decimal(119), 2, 2),
        IFDSource(("AllIFDDatasets", "Synthetic"), "fixture"),
        60,
        1.0,
        np.array([[100.0, 110.0], [120.0, 130.0]]),
        4,
        0,
    )


def test_geotiff_depth_and_multiplier(grid: SpatialIFDGrid, tmp_path: Path) -> None:
    rasterio = pytest.importorskip("rasterio")
    path = tmp_path / "depth.tif"
    write_geotiff(path, grid)
    original = path.read_bytes()
    reference = read_reference_raster(path)
    assert reference.spec == grid.spec
    np.testing.assert_array_equal(reference.depths_mm, grid.depths_mm)
    report = compare_reference(grid, reference)
    assert report.compared_cells == 4
    assert report.reference_nodata_cells == 0
    assert report.rmse_mm == 0.0
    with pytest.raises(FileExistsError):
        write_geotiff(path, grid)
    assert path.read_bytes() == original

    factor_path = tmp_path / "factor.tif"
    write_geotiff(factor_path, grid, reference_depth_mm=100.0)
    with rasterio.open(factor_path) as dataset:
        np.testing.assert_allclose(dataset.read(1), [[1, 1.1], [1.2, 1.3]])
        assert dataset.tags()["IFD_UNITS"] == "dimensionless"
        assert dataset.tags()["IFD_CACHE_SNAPSHOT"] == "fixture"
        assert dataset.crs.to_epsg() == 4326


def test_ascii_reference_requires_explicit_datum(grid: SpatialIFDGrid, tmp_path: Path) -> None:
    path = tmp_path / "depth.asc"
    write_ascii_grid(path, grid.spec, grid.depths_mm)
    with pytest.raises(ValueError, match="WGS84"):
        read_reference_raster(path)
    reference = read_reference_raster(path, assume_wgs84=True)
    assert reference.spec == grid.spec
    shifted = replace(grid, spec=replace(grid.spec, west_lon=Decimal("119.025")))
    with pytest.raises(ValueError, match="aligned"):
        compare_reference(shifted, reference)


def test_comparison_metrics_and_reference_nodata(grid: SpatialIFDGrid, tmp_path: Path) -> None:
    path = tmp_path / "reference.asc"
    path.write_text(
        "ncols 2\nnrows 2\nxllcorner 118.9875\nyllcorner -23.0375\n"
        "cellsize 0.025\nNODATA_value -9999\n99 -9999\n122 130\n",
        encoding="ascii",
    )
    reference = read_reference_raster(path, assume_wgs84=True)
    report = compare_reference(grid, reference)
    assert report.compared_cells == 3
    assert report.reference_nodata_cells == 1
    assert report.mean_bias_mm == pytest.approx(-1 / 3)
    assert report.mean_absolute_mm == 1.0
    assert report.max_absolute_mm == 2.0
    assert report.rmse_mm == pytest.approx((5 / 3) ** 0.5)
    assert report.max_absolute_percent == pytest.approx(200 / 122)


def test_area_weighted_mean_and_multipliers(grid: SpatialIFDGrid) -> None:
    areas = np.array([[1.0, 0.0], [1.0, 2.0]])
    mean = weighted_depth(grid, areas)
    assert mean == 120.0
    assert np.average(grid.multipliers(mean), weights=areas) == pytest.approx(1.0)
    with pytest.raises(ValueError, match="nonnegative"):
        weighted_depth(grid, -areas)
    with pytest.raises(ValueError, match="at least one valid"):
        weighted_depth(grid, np.zeros_like(areas))
