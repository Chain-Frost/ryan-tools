"""Coordinate bounded parallel execution of native ASC_to_ASC-style jobs."""

from __future__ import annotations

import os
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from time import perf_counter

from ...functions.live_dashboard import LiveWorkflowDashboard
from ...functions.tuflow.asc_to_asc_runner import RasterJobResult, RasterOperationJob, run_profiled_python_raster_job
from ...functions.tuflow.raster_performance import (
    RasterExecutionPlan,
    RasterOperationTimings,
    plan_raster_execution,
)


@dataclass(slots=True, frozen=True)
class DashboardOptions:
    """Live-dashboard configuration shared by native raster workflows."""

    enabled: bool = True
    refresh_per_second: float = 2.0
    max_rows: int = 10
    use_alternate_screen: bool = False


@dataclass(slots=True, frozen=True)
class StageExecutionSummary:
    """Completion counts and failures for one parallel processing stage."""

    total: int
    succeeded: int
    failed: int
    failed_jobs: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    job_timings: list[tuple[str, RasterOperationTimings]] = field(
        default_factory=list[tuple[str, RasterOperationTimings]]
    )
    elapsed_seconds: float = 0.0
    execution_plan: RasterExecutionPlan | None = None

    @property
    def ok(self) -> bool:
        """Return whether every job completed successfully."""
        return self.failed == 0


def run_raster_operation_stage(
    *,
    jobs: Sequence[RasterOperationJob],
    stage_name: str,
    dashboard_title: str,
    dashboard_subtitle: str,
    workers: int | None,
    dashboard_options: DashboardOptions,
    cache_mb: int = 128,
    chunk_mb: int = 256,
    memory_budget_mb: int | None = None,
    gdal_threads: int | None = None,
    vsi_cache_mb: int = 0,
    report_phases: bool = True,
) -> StageExecutionSummary:
    """Run native raster jobs in a bounded process pool and report progress."""
    if not jobs:
        return StageExecutionSummary(total=0, succeeded=0, failed=0)
    plan = plan_raster_operation_stage(
        jobs=jobs,
        workers=workers,
        cache_mb=cache_mb,
        chunk_mb=chunk_mb,
        memory_budget_mb=memory_budget_mb,
        gdal_threads=gdal_threads,
        vsi_cache_mb=vsi_cache_mb,
    )
    worker_count = plan.workers
    print(
        f"Raster resources ({stage_name}): {worker_count} workers "
        f"(requested {plan.requested_workers}), {plan.options.threads} GDAL threads/worker; "
        f"cache {plan.options.cache_mb} MiB/worker, chunks {plan.options.chunk_mb} MiB/worker, "
        f"VSI {plan.options.vsi_cache_mb} MiB/input; budget {plan.memory_budget_mb} MiB, "
        f"estimated {plan.estimated_worker_mb} MiB/worker."
    )

    dashboard = LiveWorkflowDashboard(
        title=dashboard_title,
        subtitle=f"{stage_name} | PID {os.getpid()} | {dashboard_subtitle}",
        enabled=dashboard_options.enabled,
        refresh_per_second=dashboard_options.refresh_per_second,
        max_rows=dashboard_options.max_rows,
        screen=dashboard_options.use_alternate_screen,
    )
    dashboard.set_tasks(labels=[job.label for job in jobs], files=[job.input_files for job in jobs])
    dashboard.set_extra_metrics(metrics={"stage": stage_name, "workers": worker_count, "PID": os.getpid()})

    failed_jobs: list[tuple[str, str]] = []
    job_timings: list[tuple[str, RasterOperationTimings]] = []
    indexed_jobs: enumerate[RasterOperationJob] = iter(enumerate(jobs, start=1))
    started = perf_counter()
    with dashboard, ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_jobs: dict[Future[RasterJobResult], tuple[int, RasterOperationJob]] = {}

        def submit_next() -> bool:
            try:
                index, job = next(indexed_jobs)
            except StopIteration:
                return False
            dashboard.mark_running(
                index=index,
                detail=f"{job.operation} using {len(job.input_files)} rasters",
                refresh=False,
            )
            future_jobs[executor.submit(run_profiled_python_raster_job, job=job, execution_options=plan.options)] = (
                index,
                job,
            )
            return True

        for _ in range(worker_count):
            submit_next()
        dashboard.set_active_count(count=len(future_jobs), refresh=False)
        dashboard.refresh(force=True)

        while future_jobs:
            completed, _ = wait(future_jobs, timeout=0.1, return_when=FIRST_COMPLETED)
            for future in completed:
                index, job = future_jobs.pop(future)
                try:
                    result = future.result()
                    job_timings.append((job.label, result.timings))
                    dashboard.mark_finished(index=index, status="OK", detail=result.output_file.name, refresh=False)
                    if report_phases:
                        dashboard.print(_format_job_timings(job.label, result.timings))
                except Exception as error:
                    error_message = str(error)
                    failed_jobs.append((job.label, error_message))
                    dashboard.mark_finished(index=index, status="FAIL", detail=error_message, refresh=False)
                    dashboard.print(f"ERROR: {job.label} failed: {error_message}")
                submit_next()
            dashboard.set_active_count(count=len(future_jobs), refresh=False)
            dashboard.refresh(force=bool(completed))

    elapsed_seconds = perf_counter() - started
    if report_phases:
        _print_stage_timings(stage_name, elapsed_seconds, job_timings)
    failed_count: int = len(failed_jobs)
    return StageExecutionSummary(
        total=len(jobs),
        succeeded=len(jobs) - failed_count,
        failed=failed_count,
        failed_jobs=failed_jobs,
        job_timings=job_timings,
        elapsed_seconds=elapsed_seconds,
        execution_plan=plan,
    )


def plan_raster_operation_stage(
    *,
    jobs: Sequence[RasterOperationJob],
    workers: int | None,
    cache_mb: int = 128,
    chunk_mb: int = 256,
    memory_budget_mb: int | None = None,
    gdal_threads: int | None = None,
    vsi_cache_mb: int = 0,
) -> RasterExecutionPlan:
    """Validate all resource settings before a workflow creates any outputs."""
    return plan_raster_execution(
        job_count=len(jobs),
        max_input_count=max(
            (
                len(job.input_files) * (2 if job.write_source and job.original_input_groups is not None else 1)
                for job in jobs
            ),
            default=1,
        ),
        requested_workers=workers,
        cache_mb=cache_mb,
        chunk_mb=chunk_mb,
        memory_budget_mb=memory_budget_mb,
        threads=gdal_threads,
        vsi_cache_mb=vsi_cache_mb,
    )


def _format_job_timings(label: str, timings: RasterOperationTimings) -> str:
    phases = ", ".join(f"{name} {seconds:.2f}s" for name, seconds in timings.phase_seconds().items())
    block = "unknown" if timings.block_shape is None else f"{timings.block_shape[1]}x{timings.block_shape[0]}"
    return (
        f"TIMING {label}: total {timings.total_seconds:.2f}s; {phases}; "
        f"{timings.width}x{timings.height}, inputs {timings.input_count}, "
        f"input block {block}, windows {timings.windows}, GDAL cache {timings.gdal_cache_bytes} bytes, "
        f"GDAL threads {timings.gdal_threads}."
    )


def _print_stage_timings(
    stage_name: str, elapsed_seconds: float, job_timings: Sequence[tuple[str, RasterOperationTimings]]
) -> None:
    phase_totals: dict[str, float] = {}
    for _, timing in job_timings:
        for name, seconds in timing.phase_seconds().items():
            phase_totals[name] = phase_totals.get(name, 0.0) + seconds
    phases = ", ".join(f"{name} {seconds:.2f}s" for name, seconds in phase_totals.items())
    print(f"TIMING {stage_name}: elapsed {elapsed_seconds:.2f}s; successful-job phase totals (worker time): {phases}.")
