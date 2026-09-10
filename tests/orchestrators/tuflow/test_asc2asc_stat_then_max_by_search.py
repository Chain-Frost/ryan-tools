"""Tests for the generic temporal-pattern statistic then maximum workflow."""

from __future__ import annotations

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

import csv
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod, source_output_paths
from ryan_library.functions.tuflow.asc_to_asc_runner import run_python_raster_job
from ryan_library.orchestrators.tuflow.asc2asc_stat_then_max_by_search import (
    ParsedRaster,
    discover_max_jobs,
    discover_rasters,
    discover_stat_jobs,
)


def _write_cell(path: Path, value: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=1,
        height=1,
        count=1,
        dtype="float64",
        nodata=-9999.0,
        transform=from_origin(0.0, 1.0, 1.0, 1.0),
    ) as dataset:
        dataset.write(np.array([[value]], dtype=np.float64), 1)


def test_median_then_max_flattens_source_to_original_tp(tmp_path: Path) -> None:
    rasters: list[ParsedRaster] = []
    grid_directory = tmp_path / "results" / "grids"
    for duration_index, (duration, minutes) in enumerate((("00060m", 60.0), ("00120m", 120.0))):
        for tp in range(1, 11):
            source = grid_directory / f"Model_EXG_01.0p_{duration}_TP{tp:02d}_V_Max.tif"
            _write_cell(source, duration_index * 10.0 + tp)
            rasters.append(
                ParsedRaster(
                    source,
                    grid_directory,
                    "EXG",
                    "01.0p",
                    duration,
                    minutes,
                    tp,
                    "V_Max",
                    "Model_EXG",
                    f"Model_EXG_01.0p_{duration}_TPMedian_V_Max.tif",
                    "Model_EXG_01.0p_TPMedian-DurMax_V_Max.tif",
                )
            )

    stat_jobs, incomplete = discover_stat_jobs(
        rasters=rasters,
        output_root=tmp_path / "outputs",
        expected_tps=frozenset(range(1, 11)),
        first_stage_statistic="median",
        write_source=True,
    )
    max_jobs = discover_max_jobs(stat_jobs=stat_jobs, output_root=tmp_path / "outputs", write_source=True)

    assert incomplete == []
    assert [details.duration for details in stat_jobs] == ["00060m", "00120m"]
    assert all(details.job.operation == "-statMedian" for details in stat_jobs)
    assert all("TPMedian" in details.job.output_file.name for details in stat_jobs)
    assert len(max_jobs) == 1
    maximum_job, durations = max_jobs[0]
    assert durations == ["00060m", "00120m"]
    assert maximum_job.original_input_groups == tuple(details.job.input_files for details in stat_jobs)

    for details in stat_jobs:
        run_python_raster_job(job=details.job)
    run_python_raster_job(job=maximum_job)

    with rasterio.open(maximum_job.output_file) as dataset:
        assert dataset.read(1)[0, 0] == 16.0
    source_path, legend_path = source_output_paths(str(maximum_job.output_file))
    with rasterio.open(source_path) as dataset:
        source_id = int(dataset.read(1)[0, 0])
    with legend_path.open(newline="", encoding="utf-8") as stream:
        legend = {int(row["source_id"]): Path(row["source_file"]) for row in csv.DictReader(stream)}

    winning_source = legend[source_id]
    assert winning_source.name == "Model_EXG_01.0p_00120m_TP06_V_Max.tif"
    assert "TPMedian" not in winning_source.name


def test_median_discovery_uses_deterministic_tuflow_naming(raster_test_data: Path) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "mean_then_max"
    parsed = discover_rasters(
        search_root=search_root,
        input_glob="*.tif",
        scenarios=("EXG", "DEV"),
        result_types=("d_HR_Max", "h_HR_Max", "V_Max"),
        first_stage_statistic="median",
    )
    tmp_output = search_root / "test_outputs"
    stat_jobs, incomplete = discover_stat_jobs(
        rasters=parsed,
        output_root=tmp_output,
        expected_tps=frozenset(range(1, 11)),
        first_stage_statistic="median",
    )
    max_jobs = discover_max_jobs(stat_jobs=stat_jobs, output_root=tmp_output)

    assert incomplete == []
    assert len(stat_jobs) == 12
    assert len(max_jobs) == 6
    assert all("TPMedian" in details.job.output_file.name for details in stat_jobs)
    assert all("TPMedian-DurMax" in job.output_file.name for job, _ in max_jobs)
    grouping_keys = {(details.scenario, details.aep, details.result_type) for details in stat_jobs}
    assert all(
        [
            details.duration_minutes
            for details in stat_jobs
            if (details.scenario, details.aep, details.result_type) == key
        ]
        == sorted(
            details.duration_minutes
            for details in stat_jobs
            if (details.scenario, details.aep, details.result_type) == key
        )
        for key in grouping_keys
    )


@pytest.mark.parametrize("mean_value_method", ["asc_to_asc", "closest_source", "arithmetic"])
def test_mean_jobs_expose_value_selection_method(tmp_path: Path, mean_value_method: MeanValueMethod) -> None:
    raster = ParsedRaster(
        path=tmp_path / "Model_EXG_01.0p_00060m_TP01_V_Max.tif",
        grid_directory=tmp_path,
        scenario="EXG",
        aep="01.0p",
        duration="00060m",
        duration_minutes=60.0,
        tp_number=1,
        result_type="V_Max",
        trim_run_code="Model_EXG",
        mean_name="Model_EXG_01.0p_00060m_TPMean_V_Max.tif",
        max_name="Model_EXG_01.0p_TPMean-DurMax_V_Max.tif",
    )

    jobs, incomplete = discover_stat_jobs(
        rasters=[raster],
        output_root=tmp_path / "outputs",
        expected_tps=frozenset({1}),
        first_stage_statistic="mean",
        mean_value_method=mean_value_method,
    )

    assert incomplete == []
    assert jobs[0].job.mean_value_method == mean_value_method


def test_mean_job_default_uses_asc_to_asc_selection(tmp_path: Path) -> None:
    rasters: list[ParsedRaster] = []
    for tp, value in enumerate((1.0, 4.0, 10.0), start=1):
        source = tmp_path / f"Model_EXG_01.0p_00060m_TP{tp:02d}_V_Max.tif"
        _write_cell(source, value)
        rasters.append(
            ParsedRaster(
                source,
                tmp_path,
                "EXG",
                "01.0p",
                "00060m",
                60.0,
                tp,
                "V_Max",
                "Model_EXG",
                "mean.tif",
                "maximum.tif",
            )
        )

    jobs, _ = discover_stat_jobs(
        rasters=rasters,
        output_root=tmp_path,
        expected_tps=frozenset({1, 2, 3}),
        first_stage_statistic="mean",
        write_source=True,
    )

    assert jobs[0].job.mean_value_method == "asc_to_asc"
    run_python_raster_job(job=jobs[0].job)
    with rasterio.open(jobs[0].job.output_file) as dataset:
        assert dataset.read(1)[0, 0] == 5.0
    source_path, legend_path = source_output_paths(str(jobs[0].job.output_file))
    with rasterio.open(source_path) as dataset:
        assert dataset.read(1)[0, 0] == 3
    with legend_path.open(newline="", encoding="utf-8") as stream:
        legend = {int(row["source_id"]): Path(row["source_file"]) for row in csv.DictReader(stream)}
    assert legend[3].name == "Model_EXG_01.0p_00060m_TP03_V_Max.tif"
