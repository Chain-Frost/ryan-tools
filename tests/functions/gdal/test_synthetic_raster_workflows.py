"""Integration tests for reusable GDAL workflows against synthetic fixtures."""

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

from __future__ import annotations

from pathlib import Path
import shutil

import numpy as np
import rasterio  # pyright: ignore[reportMissingTypeStubs]

from ryan_library.functions.gdal.raster_processing import (
    build_external_overviews,
    calculate_flood_extent,
    create_raster_footprint,
    get_vector_extent,
    read_masked_raster_band,
    read_raster_band,
    sieve_raster,
)
from ryan_library.orchestrators.gdal.raster_mosaic import create_grouped_mosaics
from ryan_library.orchestrators.gdal.raster_merge import (
    merge_directory,
    merge_directory_by_vector_extent,
)


def test_conversion_sources_describe_the_same_surface(raster_test_data: Path) -> None:
    conversion = raster_test_data / "conversion"
    arrays: list[np.ndarray] = []
    for filename in ("surface.asc", "surface.flt", "surface.xyz"):
        with rasterio.open(conversion / filename) as dataset:
            assert dataset.shape == (64, 64)
            assert dataset.transform.a == 2.0
            assert dataset.transform.e == -2.0
            arrays.append(read_raster_band(conversion / filename))

    np.testing.assert_array_equal(arrays[0], arrays[1])
    np.testing.assert_array_equal(arrays[0], arrays[2])


def test_merge_directory_builds_expected_adjacent_mosaic(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "merge" / "adjacent"
    for path in source.glob("*.tif"):
        shutil.copy2(path, tmp_path / path.name)

    vrt, tif = merge_directory(
        tmp_path,
        output_vrt=tmp_path / "mosaic.vrt",
        output_tif=tmp_path / "mosaic.tif",
        nodata=-9999.0,
    )

    assert vrt.is_file()
    with rasterio.open(tif) as dataset:
        assert dataset.shape == (32, 64)
        assert dataset.dtypes == ("float32",)
        values = read_masked_raster_band(tif)
    assert set(np.unique(values.compressed())) == {1.0, 2.0}


def test_extent_selection_uses_georeferencing_not_filenames(raster_test_data: Path) -> None:
    selected = merge_directory_by_vector_extent(
        raster_test_data / "merge" / "adjacent",
        raster_test_data / "merge" / "select_west.gpkg",
        file_pattern="*.tif",
        list_only=True,
    )

    assert [path.name for path in selected] == ["tile_west.tif"]


def test_grouped_mosaic_uses_delimited_tile_field(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "grouped_mosaic"
    for path in source.glob("*.tif"):
        shutil.copy2(path, tmp_path / path.name)

    outputs = create_grouped_mosaics(tmp_path, overview_levels=(2, 4), workers=1)

    assert [path.name for path in outputs] == ["merged_01_DEV_d_HR_Max.tif"]
    with rasterio.open(outputs[0]) as dataset:
        assert dataset.shape == (16, 32)
        assert set(np.unique(read_raster_band(outputs[0]))) == {1.0, 2.0}
    assert outputs[0].with_suffix(".tif.ovr").is_file()


def test_flood_extent_and_sieve_remove_the_isolated_wet_cell(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "maintenance" / "Synthetic_EXG_01p_060m_TP01_d_HR_Max.tif"
    raw_mask = calculate_flood_extent(source, tmp_path / "flood_raw.tif", 0.05)
    sieved_mask = sieve_raster(raw_mask, tmp_path / "flood_sieved.tif", threshold_pixels=2)

    raw = read_raster_band(raw_mask)
    sieved = read_raster_band(sieved_mask)
    assert np.count_nonzero(raw == 1) == 1_025
    assert np.count_nonzero(sieved == 1) == 1_024


def test_flood_extent_can_select_band_four(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "maintenance" / "four_band_depth.tif"
    output = calculate_flood_extent(source, tmp_path / "band_four_flood.tif", 0.1, input_band=4)

    values = read_raster_band(output)
    assert np.count_nonzero(values == 1) == 512


def test_nodata_footprint_preserves_the_valid_outer_extent(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "maintenance" / "nodata_regions.tif"
    footprint = create_raster_footprint(source, tmp_path / "footprint.gpkg")

    assert footprint.is_file()
    assert get_vector_extent(footprint) == (400_000.0, 6_499_872.0, 400_128.0, 6_500_000.0)


def test_external_overviews_are_created_beside_a_copy(raster_test_data: Path, tmp_path: Path) -> None:
    source = raster_test_data / "maintenance" / "nodata_regions.tif"
    copied = Path(shutil.copy2(source, tmp_path / source.name))
    overview = build_external_overviews(copied, levels=(2, 4, 8), refresh=True)

    assert overview == copied.with_suffix(".tif.ovr")
    assert overview.is_file()
    with rasterio.open(copied) as dataset:
        assert dataset.overviews(1) == [2, 4, 8]
