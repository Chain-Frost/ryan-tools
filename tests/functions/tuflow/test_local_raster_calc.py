"""Focused tests for local TUFLOW raster statistics."""

from pathlib import Path

import numpy as np
import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs]

from ryan_library.functions.tuflow.local_raster_calc import compute_stat
from ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search import (
    ParsedRaster,
    discover_max_jobs,
    discover_mean_jobs,
)

NODATA = -9999.0


def _write_raster(path: Path, values: list[float]) -> None:
    data = np.asarray([values], dtype=np.float32)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        transform=from_origin(0.0, 1.0, 1.0, 1.0),
        nodata=NODATA,
    ) as destination:
        destination.write(data, 1)


def _read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as source:
        return source.read(1)


@pytest.mark.parametrize(
    ("stat_type", "expected"),
    [("-statMean", [1.0, 0.0]), ("-statMax", [2.0, 0.0])],
)
def test_compute_stat_treats_nodata_as_zero(tmp_path: Path, stat_type: str, expected: list[float]) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [2.0, NODATA])
    _write_raster(second, [NODATA, NODATA])

    compute_stat(stat_type, [str(first), str(second)], str(output), nodata_policy="zero")

    np.testing.assert_array_equal(_read_raster(output), np.asarray([expected], dtype=np.float32))


@pytest.mark.parametrize(
    ("stat_type", "expected"),
    [("-statMean", [103.0, 104.0, NODATA]), ("-statMax", [104.0, 104.0, NODATA])],
)
def test_compute_stat_excludes_nodata_and_preserves_all_nodata(
    tmp_path: Path, stat_type: str, expected: list[float]
) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [102.0, NODATA, NODATA])
    _write_raster(second, [104.0, 104.0, NODATA])

    compute_stat(stat_type, [str(first), str(second)], str(output), nodata_policy="exclude")

    np.testing.assert_array_equal(_read_raster(output), np.asarray([expected], dtype=np.float32))


def _parsed_raster(*, tmp_path: Path, result_type: str) -> ParsedRaster:
    return ParsedRaster(
        path=tmp_path / f"input_{result_type}.tif",
        grid_directory=tmp_path,
        scenario="EXG",
        aep="1%AEP",
        duration="60m",
        duration_minutes=60.0,
        tp_number=1,
        result_type=result_type,
        trim_run_code="model_EXG",
        mean_name=f"mean_{result_type}.tif",
        max_name=f"max_{result_type}.tif",
    )


def test_mean_and_max_jobs_use_result_type_nodata_policy(tmp_path: Path) -> None:
    expected_policies = {
        "d_HR_Max": "zero",
        "V_Max": "zero",
        "h_Max": "exclude",
        "h_HR_Max": "exclude",
    }

    for result_type, expected_policy in expected_policies.items():
        mean_jobs, incomplete = discover_mean_jobs(
            rasters=[_parsed_raster(tmp_path=tmp_path, result_type=result_type)],
            output_root=tmp_path / "output",
            expected_tps=frozenset({1}),
        )
        assert not incomplete
        assert mean_jobs[0].job.nodata_policy == expected_policy

        max_jobs = discover_max_jobs(mean_jobs=mean_jobs, output_root=tmp_path / "output")
        assert max_jobs[0][0].nodata_policy == expected_policy
