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
    FirstStageStatistic,
    ParsedRaster,
    discover_max_jobs,
    discover_rasters,
    discover_stat_jobs,
    run_stat_then_max_workflow,
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


def _write_tp_bin(
    grid_directory: Path,
    *,
    duration_minutes: int,
    values: tuple[float | None, ...],
    result_type: str = "V_Max",
) -> tuple[Path, ...]:
    """Write a small TP bin, using None for a zero-byte model output."""
    grid_directory.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for tp, value in enumerate(values, start=1):
        path = grid_directory / f"Model_EXG_01.0p_{duration_minutes:05d}m_TP{tp:02d}_{result_type}.tif"
        if value is None:
            path.touch()
        else:
            _write_cell(path, value)
        paths.append(path)
    return tuple(paths)


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
    _write_cell(raster.path, 1.0)

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


@pytest.mark.parametrize("statistic", ["mean", "median"])
@pytest.mark.parametrize("allow_incomplete_tp_bins", [False, True])
def test_incomplete_bin_override_values_and_original_sources(
    statistic: FirstStageStatistic,
    allow_incomplete_tp_bins: bool,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    grid_directory = tmp_path / "results" / "grids"
    complete_inputs = _write_tp_bin(grid_directory, duration_minutes=60, values=(2.0, 4.0, 6.0, 8.0))
    partial_inputs = _write_tp_bin(grid_directory, duration_minutes=120, values=(10.0, None, 30.0))
    _write_tp_bin(grid_directory, duration_minutes=180, values=(None, None, None, None))
    _write_tp_bin(grid_directory, duration_minutes=60, values=(40.0, 50.0, 60.0, 70.0), result_type="h_HR_Max")
    output_root = tmp_path / "outputs"

    result = run_stat_then_max_workflow(
        first_stage_statistic=statistic,
        search_root=tmp_path,
        output_root=output_root,
        input_glob="*.tif",
        expected_tps=frozenset(range(1, 5)),
        scenarios=("EXG",),
        result_types=("V_Max", "h_HR_Max"),
        allow_incomplete_tp_bins=allow_incomplete_tp_bins,
        write_source=True,
        workers=1,
        use_live_dashboard=False,
    )

    report = capsys.readouterr().out
    assert "WARNING" in report
    assert "missing TP02, TP04" in report
    assert "00180m" in report
    assert "zero-byte" in report
    assert "no usable inputs" in report
    token = f"TP{statistic.title()}"
    statistic_directory = output_root / f"{statistic}s" / "EXG" / "01.0p"
    maximum_directory = output_root / f"max_of_{statistic}s" / "EXG" / "01.0p"
    complete_statistic = statistic_directory / f"Model_EXG_01.0p_00060m_{token}_V_Max.tif"
    partial_statistic = statistic_directory / f"Model_EXG_01.0p_00120m_{token}_V_Max.tif"
    unavailable_statistic = statistic_directory / f"Model_EXG_01.0p_00180m_{token}_V_Max.tif"
    velocity_maximum = maximum_directory / f"Model_EXG_01.0p_{token}-DurMax_V_Max.tif"
    unaffected_maximum = maximum_directory / f"Model_EXG_01.0p_{token}-DurMax_h_HR_Max.tif"

    assert complete_statistic.is_file()
    assert not unavailable_statistic.exists()
    with rasterio.open(unaffected_maximum) as dataset:
        assert dataset.read(1)[0, 0] == (55.0 if statistic == "mean" else 60.0)
    if not allow_incomplete_tp_bins:
        assert result == 1
        assert not partial_statistic.exists()
        assert not velocity_maximum.exists()
        return

    assert result == 0
    assert "explicit override" in report
    expected_value = 20.0 if statistic == "mean" else 30.0
    for output_file in (partial_statistic, velocity_maximum):
        with rasterio.open(output_file) as dataset:
            assert dataset.read(1)[0, 0] == expected_value
    source_path, legend_path = source_output_paths(str(velocity_maximum))
    with rasterio.open(source_path) as dataset:
        source_id = int(dataset.read(1)[0, 0])
    with legend_path.open(newline="", encoding="utf-8") as stream:
        legend = list(csv.DictReader(stream))

    available_inputs = [*complete_inputs, partial_inputs[0], partial_inputs[2]]
    assert [Path(row["source_file"]) for row in legend] == available_inputs
    assert [int(row["source_id"]) for row in legend] == list(range(1, 7))
    assert source_id == 6
    assert Path(legend[source_id - 1]["source_file"]) == partial_inputs[2]
    assert [int(row["cell_count"]) for row in legend] == [0, 0, 0, 0, 0, 1]


@pytest.mark.parametrize("expected_tps", [frozenset(), frozenset({0, 1}), frozenset({-1, 1})])
def test_expected_tp_configuration_must_be_nonempty_and_positive(expected_tps: frozenset[int], tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"nonempty.*positive"):
        discover_stat_jobs(
            rasters=[],
            output_root=tmp_path / "outputs",
            expected_tps=expected_tps,
            first_stage_statistic="mean",
            allow_incomplete_tp_bins=True,
        )


@pytest.mark.parametrize(
    ("tp_numbers", "message"), [((1, 1), "Duplicate temporal patterns"), ((1, 4), "Unexpected temporal patterns")]
)
def test_override_still_rejects_duplicate_or_unexpected_tps_before_inspecting_files(
    tp_numbers: tuple[int, int], message: str, tmp_path: Path
) -> None:
    rasters = [
        ParsedRaster(
            path=tmp_path / f"missing_{index}.tif",
            grid_directory=tmp_path,
            scenario="EXG",
            aep="01.0p",
            duration="00060m",
            duration_minutes=60.0,
            tp_number=tp,
            result_type="V_Max",
            trim_run_code="Model_EXG",
            mean_name="mean.tif",
            max_name="maximum.tif",
        )
        for index, tp in enumerate(tp_numbers)
    ]
    with pytest.raises(ValueError, match=message):
        discover_stat_jobs(
            rasters=rasters,
            output_root=tmp_path / "outputs",
            expected_tps=frozenset({1, 2, 3}),
            first_stage_statistic="mean",
            allow_incomplete_tp_bins=True,
        )


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("allow_incomplete_tp_bins", [False, True])
def test_no_usable_bins_fail_even_with_override_or_dry_run(
    dry_run: bool,
    allow_incomplete_tp_bins: bool,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_tp_bin(tmp_path / "grids", duration_minutes=60, values=(None, None))
    output_root = tmp_path / "outputs"

    assert (
        run_stat_then_max_workflow(
            first_stage_statistic="mean",
            search_root=tmp_path,
            output_root=output_root,
            input_glob="*.tif",
            expected_tps=frozenset({1, 2}),
            scenarios=("EXG",),
            result_types=("V_Max",),
            allow_incomplete_tp_bins=allow_incomplete_tp_bins,
            dry_run=dry_run,
            workers=1,
            use_live_dashboard=False,
        )
        == 1
    )

    assert "no usable" in capsys.readouterr().out
    assert not output_root.exists()


def test_strict_and_incomplete_override_conflict_before_discovery(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        run_stat_then_max_workflow(
            first_stage_statistic="mean",
            search_root=tmp_path,
            output_root=tmp_path / "outputs",
            input_glob="*.tif",
            expected_tps=frozenset({1, 2}),
            scenarios=("EXG",),
            result_types=("V_Max",),
            strict=True,
            allow_incomplete_tp_bins=True,
            dry_run=True,
            use_live_dashboard=False,
        )
        == 1
    )

    report = capsys.readouterr().out
    assert "--strict cannot be combined with --allow-incomplete-tp-bins" in report
    assert "No grids directories" not in report


def test_override_does_not_use_stale_statistics_after_raster_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inputs = _write_tp_bin(tmp_path / "grids", duration_minutes=60, values=(1.0,))
    inputs[0].write_text("Nonempty but invalid raster", encoding="utf-8")
    output_root = tmp_path / "outputs"
    stale_statistic = output_root / "means" / "EXG" / "01.0p" / "Model_EXG_01.0p_00060m_TPMean_V_Max.tif"
    _write_cell(stale_statistic, 999.0)

    assert (
        run_stat_then_max_workflow(
            first_stage_statistic="mean",
            search_root=tmp_path,
            output_root=output_root,
            input_glob="*.tif",
            expected_tps=frozenset({1, 2}),
            scenarios=("EXG",),
            result_types=("V_Max",),
            allow_incomplete_tp_bins=True,
            workers=1,
            use_live_dashboard=False,
        )
        == 1
    )

    assert "maximum stage was not started" in capsys.readouterr().out
    assert not (output_root / "max_of_means").exists()
    with rasterio.open(stale_statistic) as dataset:
        assert dataset.read(1)[0, 0] == 999.0
