"""Resource admission and real spawned-worker reporting for raster batches."""

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import get_context
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.tuflow.asc_to_asc_runner import RasterOperationJob
from ryan_library.functions.tuflow.raster_performance import RasterExecutionOptions, plan_raster_execution
from ryan_library.orchestrators.tuflow import asc_to_asc_batch
from ryan_library.orchestrators.tuflow.asc2asc_stat_then_max_by_search import run_stat_then_max_workflow
from ryan_library.orchestrators.tuflow.asc_to_asc_batch import (
    DashboardOptions,
    plan_raster_operation_stage,
    run_raster_operation_stage,
)


@pytest.fixture
def sixteen_cpus(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ryan_library.functions.gdal.raster_processing.os.cpu_count", lambda: 16)


@pytest.mark.usefixtures("sixteen_cpus")
def test_explicit_thread_count_bounds_outer_workers() -> None:
    plan = plan_raster_execution(
        job_count=20, max_input_count=10, requested_workers=12, threads=4, memory_budget_mb=8192
    )
    assert plan.workers == 4
    assert plan.options.threads == 4
    assert plan.workers * plan.options.threads <= plan.cpu_count
    assert plan.workers * plan.estimated_worker_mb <= plan.memory_budget_mb


@pytest.mark.usefixtures("sixteen_cpus")
def test_automatic_threads_fit_one_worker_into_small_budget() -> None:
    plan = plan_raster_execution(job_count=1, max_input_count=10, requested_workers=12, memory_budget_mb=550)
    assert plan.workers == 1
    assert plan.options.threads == 4
    assert plan.estimated_worker_mb <= 550


@pytest.mark.usefixtures("sixteen_cpus")
def test_automatic_memory_budget_reserves_available_ram() -> None:
    plan = plan_raster_execution(job_count=12, max_input_count=10, available_memory_bytes=4096 * 1024**2)
    assert plan.memory_budget_mb == 3072
    assert plan.workers * plan.estimated_worker_mb <= plan.memory_budget_mb
    assert plan.workers * plan.options.threads <= 16


@pytest.mark.usefixtures("sixteen_cpus")
def test_nested_provenance_inputs_count_towards_file_cache_budget(tmp_path: Path) -> None:
    inputs = tuple(tmp_path / f"mean_{index}.tif" for index in range(10))
    original_groups = tuple((tmp_path / f"original_{index}.tif",) for index in range(10))
    regular_jobs = [RasterOperationJob(str(i), "-statMax", inputs, tmp_path / f"regular_{i}.tif") for i in range(2)]
    nested_jobs = [
        RasterOperationJob(
            str(i),
            "-statMax",
            inputs,
            tmp_path / f"nested_{i}.tif",
            write_source=True,
            original_input_groups=original_groups,
        )
        for i in range(2)
    ]
    regular = plan_raster_operation_stage(
        jobs=regular_jobs, workers=2, gdal_threads=1, memory_budget_mb=1360, vsi_cache_mb=16
    )
    nested = plan_raster_operation_stage(
        jobs=nested_jobs, workers=2, gdal_threads=1, memory_budget_mb=1360, vsi_cache_mb=16
    )
    assert regular.workers == 2
    assert nested.workers == 1
    assert nested.estimated_worker_mb > regular.estimated_worker_mb


@pytest.mark.parametrize(
    ("setting", "value"),
    [("cache_mb", 0), ("chunk_mb", -1), ("threads", False), ("vsi_cache_mb", -1), ("cache_mb", 1.5)],
)
def test_invalid_execution_options_fail_explicitly(setting: str, value: object) -> None:
    with pytest.raises(ValueError, match="must be an integer"):
        RasterExecutionOptions(**{setting: value})  # type: ignore[arg-type]


def test_memory_rejection_happens_before_output_creation(raster_test_data: Path, tmp_path: Path) -> None:
    output = tmp_path / "uncreated"
    result = run_stat_then_max_workflow(
        first_stage_statistic="mean",
        search_root=raster_test_data / "tuflow_statistics" / "mean_then_max",
        output_root=output,
        input_glob="*.tif",
        expected_tps=frozenset(range(1, 11)),
        scenarios=("EXG", "DEV"),
        result_types=("d_HR_Max", "h_HR_Max", "V_Max"),
        workers=1,
        memory_budget_mb=1,
        write_source=True,
        use_live_dashboard=False,
    )
    assert result == 1
    assert not output.exists()


def test_spawned_batch_returns_phases_and_failure_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "input.tif"
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        nodata=-9999.0,
        transform=from_origin(100.0, 200.0, 1.0, 1.0),
    ) as dataset:
        dataset.write(np.full((8, 8), 3.0, dtype=np.float32), 1)
    monkeypatch.setattr(
        asc_to_asc_batch, "ProcessPoolExecutor", partial(ProcessPoolExecutor, mp_context=get_context("spawn"))
    )
    jobs = [
        RasterOperationJob("valid", "-statMean", (source,), tmp_path / "mean.tif", write_source=True),
        RasterOperationJob("missing", "-statMean", (tmp_path / "missing.tif",), tmp_path / "missing_mean.tif"),
    ]
    summary = run_raster_operation_stage(
        jobs=jobs,
        stage_name="test means",
        dashboard_title="Test",
        dashboard_subtitle="Synthetic",
        workers=2,
        dashboard_options=DashboardOptions(enabled=False),
        cache_mb=8,
        chunk_mb=8,
        memory_budget_mb=1024,
        gdal_threads=1,
        report_phases=True,
    )
    assert summary.total == 2
    assert summary.succeeded == 1
    assert summary.failed == 1
    assert not summary.ok
    assert summary.failed_jobs[0][0] == "missing"
    assert len(summary.job_timings) == 1
    label, timing = summary.job_timings[0]
    assert label == "valid"
    assert (timing.width, timing.height, timing.input_count) == (8, 8, 1)
    assert timing.gdal_cache_bytes == 8 * 1024**2
    assert timing.windows >= 1
    assert timing.total_seconds > 0
    assert timing.close_seconds >= 0
    assert sum(timing.phase_seconds().values()) <= timing.total_seconds + 0.001
    assert summary.elapsed_seconds > 0
    with rasterio.open(tmp_path / "mean.tif") as dataset:
        np.testing.assert_array_equal(dataset.read(1), np.full((8, 8), 3.0, dtype=np.float32))
    with rasterio.open(tmp_path / "mean_src.tif") as dataset:
        np.testing.assert_array_equal(dataset.read(1), np.ones((8, 8), dtype=np.uint8))
    assert (tmp_path / "mean_src_legend.csv").is_file()
    console = capsys.readouterr().out
    assert "Raster resources" in console
    assert "TIMING valid" in console
    assert "worker time" in console
    assert "missing" in console
