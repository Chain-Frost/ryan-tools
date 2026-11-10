"""Regression tests for priority mosaics and exclusions.

Small synthetic rasters run without the GRASS or TUFLOW executables.
"""

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ryan_library.functions.gdal.terrain_mosaic import compose_terrain
from ryan_library.orchestrators.tuflow.initialiser.geometry import Domain, detailed_analysis_cells, make_domain


def _raster(path: Path, pixels: np.ndarray, *, west: float, north: float) -> None:
    with rasterio.open(
        path,
        mode="w",
        driver="GTiff",
        count=1,
        width=pixels.shape[1],
        height=pixels.shape[0],
        crs="EPSG:28350",
        transform=from_origin(west, north, 1, 1),
        dtype="float32",
        nodata=-9999,
    ) as output:
        output.write(pixels.astype("float32"), 1)


def test_lidar_overrides_srtm_and_srtm_fills_void(tmp_path: Path) -> None:
    coarse = tmp_path / "srtm.tif"
    lidar = tmp_path / "lidar.tif"
    _raster(coarse, np.full((10, 10), 100), west=0, north=10)
    pixels = np.full((2, 2), 200.0)
    pixels[0, 0] = -9999
    _raster(lidar, pixels, west=4, north=6)
    merged = compose_terrain(
        [lidar, coarse],
        target_crs=CRS.from_epsg(28350),
        directory=tmp_path / "outputs",
        resolution_m=1,
    )
    with rasterio.open(merged.dem) as dataset, rasterio.open(merged.source_index) as source_index:
        elev = dataset.read(1)
        source = source_index.read(1)
    assert elev[4, 4] == 100.0
    assert np.all(elev[4:6, 4:6][pixels != -9999] == 200)
    assert source[4, 4] == 2
    assert np.all(source[4:6, 4:6][pixels != -9999] == 1)


def test_reverse_terrain_priority(tmp_path: Path) -> None:
    a, b = tmp_path / "a.tif", tmp_path / "b.tif"
    _raster(a, np.full((5, 5), 1), west=0, north=5)
    _raster(b, np.full((5, 5), 2), west=0, north=5)
    first = compose_terrain([a, b], target_crs=CRS.from_epsg(28350), directory=tmp_path / "first")
    last = compose_terrain([b, a], target_crs=CRS.from_epsg(28350), directory=tmp_path / "last")
    with rasterio.open(first.dem) as f, rasterio.open(last.dem) as last_dem:
        assert np.all(f.read(1) == 1)
        assert np.all(last_dem.read(1) == 2)


@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_single_dem_preserves_native_grid_values_and_masks(tmp_path: Path, dtype: str) -> None:
    source = tmp_path / "native.tif"
    pixels = (np.arange(25).reshape(5, 5) / 7.0).astype(dtype)
    pixels[0, 0] = -9999
    mask = np.full((5, 5), 255, dtype=np.uint8)
    mask[2, 2] = 0
    transform = from_origin(500000.25, 7000000.75, 1, 1)
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        count=1,
        width=5,
        height=5,
        dtype=dtype,
        nodata=-9999,
        crs="EPSG:28350",
        transform=transform,
    ) as dataset:
        dataset.write(pixels, 1)
        dataset.write_mask(mask)
    result = compose_terrain([source], target_crs=CRS.from_epsg(28350), directory=tmp_path / "native")
    with rasterio.open(source) as original, rasterio.open(result.dem) as actual:
        assert actual.transform == original.transform
        assert actual.shape == original.shape
        assert actual.dtypes == original.dtypes
        assert actual.nodata == original.nodata
        np.testing.assert_array_equal(actual.read(1), original.read(1))
        np.testing.assert_array_equal(actual.read_masks(1), original.read_masks(1))
    with rasterio.open(result.source_index) as provenance:
        assert provenance.transform == transform
        np.testing.assert_array_equal(provenance.read(1), (mask > 0).astype(np.uint16))


def test_explicit_resolution_still_anchors_to_source_origin(tmp_path: Path) -> None:
    source = tmp_path / "source.tif"
    _raster(source, np.ones((5, 5)), west=0.25, north=5.75)
    result = compose_terrain([source], target_crs=CRS.from_epsg(28350), directory=tmp_path / "aligned", resolution_m=2)
    with rasterio.open(result.dem) as actual:
        assert actual.transform == from_origin(0.25, 5.75, 2, 2)
        assert actual.shape == (3, 3)


@pytest.mark.parametrize("reverse_priority", [False, True])
def test_multi_dem_anchors_to_finest_grid_independent_of_priority(tmp_path: Path, reverse_priority: bool) -> None:
    fine, coarse = tmp_path / "fine.tif", tmp_path / "coarse.tif"
    _raster(fine, np.full((5, 5), 200), west=2.25, north=7.75)
    with rasterio.open(
        coarse,
        "w",
        driver="GTiff",
        count=1,
        width=5,
        height=5,
        dtype="float32",
        nodata=-9999,
        crs="EPSG:28350",
        transform=from_origin(0, 10, 2, 2),
    ) as dataset:
        dataset.write(np.full((5, 5), 100, dtype=np.float32), 1)
    sources = [coarse, fine] if reverse_priority else [fine, coarse]
    result = compose_terrain(sources, target_crs=CRS.from_epsg(28350), directory=tmp_path / "mosaic")
    with rasterio.open(result.dem) as actual, rasterio.open(result.source_index) as provenance:
        assert actual.res == (1, 1)
        assert actual.transform == from_origin(-0.75, 10.75, 1, 1)
        assert actual.shape == (11, 11)
        # A fine-source cell centre remains a centre on the mosaic grid.
        row, col = actual.index(2.75, 7.25)
        assert actual.xy(row, col) == (2.75, 7.25)
        assert actual.read(1)[row, col] == (100 if reverse_priority else 200)
        assert provenance.read(1)[row, col] == 1


def test_reject_unbounded_fine_grid(tmp_path: Path) -> None:
    a = tmp_path / "a.tif"
    _raster(a, np.ones((5, 5)), west=0, north=5)
    with pytest.raises(ValueError, match="Unsafe"):
        compose_terrain([a], target_crs=CRS.from_epsg(28350), directory=tmp_path / "out", max_cells=5)


def _domain(exclusion: BaseGeometry | None = None) -> Domain:
    directions = np.zeros((12, 12), dtype=np.int16)
    directions[3:8, 6] = 6  # downstream to the AOI, entirely within source DEM
    return make_domain(
        directions,
        aoi=box(6.2, 3.2, 6.8, 3.8),
        transform=from_origin(0, 12, 1, 1),
        raster_bounds=(0, 0, 12, 12),
        buffer_m=0.25,
        exclusions=exclusion,
    )


def test_exclusions_remove_only_upstream_area_without_changing_full_catchment() -> None:
    complete = _domain()
    cropped = _domain(box(6.1, 6.1, 6.9, 6.9))
    assert cropped.contributing.equals(complete.contributing)
    assert cropped.excluded_area is not None
    assert cropped.excluded_area.area > 0
    assert cropped.active.area == pytest.approx(complete.active.area - cropped.excluded_area.area)
    assert cropped.active.intersection(cropped.excluded_area).area == pytest.approx(0)
    assert cropped.location.covers(cropped.active)


def test_exclusions_must_not_remove_aoi_buffer() -> None:
    with pytest.raises(ValueError, match="buffer"):
        _domain(box(6.1, 3.1, 6.9, 3.9))


def test_exclusions_outside_upstream_area_rejected() -> None:
    with pytest.raises(ValueError, match="do not intersect"):
        _domain(box(1.2, 1.2, 1.8, 1.8))


def test_default_detailed_analysis_omits_excluded_cells() -> None:
    domain = _domain(box(6.1, 6.1, 6.9, 6.9))
    transform = from_origin(0, 12, 1, 1)
    default_cells = detailed_analysis_cells(domain, transform=transform)
    full_cells = detailed_analysis_cells(domain, transform=transform, analyse_exclusions=True)
    assert np.array_equal(full_cells, domain.contributing_cells)
    assert np.all(~default_cells | full_cells)
    assert np.count_nonzero(default_cells) < np.count_nonzero(full_cells)
    assert full_cells[5, 6]
    assert not default_cells[5, 6]
    assert domain.excluded_area is not None
    assert domain.active.intersection(domain.excluded_area).area == pytest.approx(0)
    # Detailed analysis preference does NOT change domain delineation.
    assert domain.contributing.equals(_domain().contributing)


def test_no_exclusions_analysis_switch_is_noop() -> None:
    domain = _domain()
    transform = from_origin(0, 12, 1, 1)
    default = detailed_analysis_cells(domain, transform=transform)
    explicit = detailed_analysis_cells(domain, transform=transform, analyse_exclusions=True)
    assert np.array_equal(default, explicit)


def test_exclusion_polygon_hole_preserves_an_active_island() -> None:
    outer = [(6.0, 6.0), (7.0, 6.0), (7.0, 7.0), (6.0, 7.0)]
    hole = [(6.25, 6.25), (6.75, 6.25), (6.75, 6.75), (6.25, 6.75)]
    clipped = _domain(Polygon(outer, holes=[hole]))
    base = _domain()
    assert clipped.excluded_area is not None
    assert clipped.contributing.equals(base.contributing)
    assert clipped.active.covers(box(6.3, 6.3, 6.7, 6.7))
    assert clipped.active.area == pytest.approx(base.active.area - clipped.excluded_area.area)


def test_overlapping_exclusions_are_union_subtracted_once() -> None:
    boxes = [box(6.1, 6.1, 6.7, 6.9), box(6.3, 6.3, 6.9, 6.95)]
    combined = unary_union(boxes)
    domain = _domain(combined)
    base = _domain()
    assert domain.excluded_area is not None
    assert domain.excluded_area.area < sum(item.area for item in boxes)
    assert base.active.area - domain.active.area == pytest.approx(domain.excluded_area.area)
    assert domain.active.intersection(domain.excluded_area).area == pytest.approx(0)
