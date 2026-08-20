"""Focused tests for raster sampling along profile lines."""

# Rasterio does not currently provide complete type information.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from pathlib import Path

import numpy as np
import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs]
from shapely.geometry import LineString

from ryan_library.functions.gdal.profiling import (
    RasterSamplingError,
    interpolate_short_nan_gaps,
    sample_raster_along_line,
)


def _write_raster(path: Path, values: np.ndarray, *, nodata: float = -999.0) -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype="float64",
        crs="EPSG:28351",
        transform=from_origin(0.0, float(values.shape[0]), 1.0, 1.0),
        nodata=nodata,
    ) as destination:
        destination.write(values.astype(np.float64), 1)
    return path


def test_bilinear_sampling_is_aligned_to_cell_centres(tmp_path: Path) -> None:
    raster = _write_raster(tmp_path / "centres.tif", np.arange(9, dtype=np.float64).reshape(3, 3))
    line = LineString(((0.5, 2.5), (2.5, 2.5)))

    distances, values = sample_raster_along_line(line, raster, spacing=1.0, method="bilinear")

    np.testing.assert_allclose(distances, [0.0, 1.0, 2.0])
    np.testing.assert_allclose(values, [0.0, 1.0, 2.0])


def test_station_spacing_never_exceeds_requested_maximum(tmp_path: Path) -> None:
    raster = _write_raster(tmp_path / "spacing.tif", np.ones((3, 3), dtype=np.float64))
    line = LineString(((0.5, 2.5), (1.6, 2.5)))

    distances, _ = sample_raster_along_line(line, raster, spacing=0.5)

    assert distances[0] == 0.0
    assert distances[-1] == pytest.approx(line.length)
    assert np.diff(distances).max() <= 0.5
    assert len(distances) == 4


def test_masked_bilinear_keeps_samples_inside_valid_cells(tmp_path: Path) -> None:
    values = np.array([[1.0, -999.0], [3.0, 5.0]], dtype=np.float64)
    raster = _write_raster(tmp_path / "nodata.tif", values)
    line = LineString(((0.9, 1.5), (0.9, 1.4)))

    _, strict = sample_raster_along_line(line, raster, spacing=0.1, method="bilinear")
    _, masked = sample_raster_along_line(line, raster, spacing=0.1, method="bilinear_masked")
    _, default = sample_raster_along_line(line, raster, spacing=0.1)

    assert np.isnan(strict[0])
    assert masked[0] == pytest.approx(1.0)
    np.testing.assert_array_equal(default, masked)


def test_masked_bilinear_does_not_expand_values_into_nodata_cells(tmp_path: Path) -> None:
    values = np.array([[1.0, -999.0], [3.0, 5.0]], dtype=np.float64)
    raster = _write_raster(tmp_path / "nodata.tif", values)
    line = LineString(((1.1, 1.5), (1.1, 1.4)))

    _, valid = sample_raster_along_line(line, raster, spacing=0.1, method="bilinear_valid")
    _, masked = sample_raster_along_line(line, raster, spacing=0.1, method="bilinear_masked")

    assert valid[0] == pytest.approx(1.0)
    assert np.isnan(masked[0])


def test_interpolate_short_nan_gaps_only_fills_complete_internal_runs() -> None:
    values = np.array(
        [np.nan, 0.0, np.nan, np.nan, 3.0, np.nan, np.nan, np.nan, 7.0, np.nan],
        dtype=np.float64,
    )

    result = interpolate_short_nan_gaps(values, max_gap=2)

    np.testing.assert_allclose(result[1:5], [0.0, 1.0, 2.0, 3.0])
    assert np.isnan(result[0])
    assert np.isnan(result[5:8]).all()
    assert np.isnan(result[-1])


@pytest.mark.parametrize("spacing", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_spacing_is_rejected(tmp_path: Path, spacing: float) -> None:
    raster = _write_raster(tmp_path / "invalid_spacing.tif", np.ones((2, 2), dtype=np.float64))

    with pytest.raises(ValueError, match="spacing"):
        sample_raster_along_line(LineString(((0.5, 1.5), (1.5, 1.5))), raster, spacing=spacing)


def test_sampling_errors_are_not_converted_to_nan_profiles(tmp_path: Path) -> None:
    with pytest.raises(RasterSamplingError, match="Failed to sample raster"):
        sample_raster_along_line(
            LineString(((0.0, 0.0), (1.0, 0.0))),
            tmp_path / "missing.tif",
            spacing=0.5,
        )
