"""Coordinate bounded parallel execution of native ASC_to_ASC-style jobs."""

from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
import os
from pathlib import Path

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.tuflow.asc_to_asc_runner import RasterOperationJob, run_python_raster_job


@dataclass(slots=True, frozen=True)
class DashboardOptions:
    """Live-dashboard configuration shared by native raster workflows."""

    enabled: bool = True
    refresh_per_second: float = 2.0
    max_rows: int = 25
    use_alternate_screen: bool = False


@dataclass(slots=True, frozen=True)
class StageExecutionSummary:
    """Completion counts and failures for one parallel processing stage."""

    total: int
    succeeded: int
    failed: int
    failed_jobs: list[tuple[str, str]] = field(default_factory=lambda: list[tuple[str, str]]())

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
) -> StageExecutionSummary:
    """Run native raster jobs in a bounded process pool and report progress."""
    if not jobs:
        return StageExecutionSummary(total=0, succeeded=0, failed=0)
    requested_workers: int = workers if workers is not None else 6
    if requested_workers < 1:
        raise ValueError("Worker count must be at least 1")
    worker_count: int = min(requested_workers, len(jobs))

    dashboard = LiveWorkflowDashboard(
        title=dashboard_title,
        subtitle=f"{stage_name} | PID {os.getpid()} | {dashboard_subtitle}",
        enabled=dashboard_options.enabled,
        refresh_per_second=dashboard_options.refresh_per_second,
        max_rows=dashboard_options.max_rows,
        screen=dashboard_options.use_alternate_screen,
    )
    dashboard.set_tasks(labels=[job.label for job in jobs])
    dashboard.set_extra_metrics(metrics={"stage": stage_name, "workers": worker_count, "PID": os.getpid()})

    failed_jobs: list[tuple[str, str]] = []
    indexed_jobs: enumerate[RasterOperationJob] = iter(enumerate(jobs, start=1))
    with dashboard, ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_jobs: dict[Future[Path], tuple[int, RasterOperationJob]] = {}

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
            future_jobs[executor.submit(run_python_raster_job, job=job)] = (index, job)
            return True

        for _ in range(worker_count):
            submit_next()
        dashboard.set_active_count(count=len(future_jobs), refresh=False)
        dashboard.refresh(force=True)

        while future_jobs:
            completed, _ = wait(future_jobs, return_when=FIRST_COMPLETED)
            for future in completed:
                index, job = future_jobs.pop(future)
                try:
                    output_file: Path = future.result()
                    dashboard.mark_finished(index=index, status="OK", detail=output_file.name, refresh=False)
                except Exception as error:
                    error_message = str(error)
                    failed_jobs.append((job.label, error_message))
                    dashboard.mark_finished(index=index, status="FAIL", detail=error_message, refresh=False)
                    dashboard.print(f"ERROR: {job.label} failed: {error_message}")
                submit_next()
            dashboard.set_active_count(count=len(future_jobs), refresh=False)
            dashboard.refresh(force=True)

    failed_count: int = len(failed_jobs)
    return StageExecutionSummary(
        total=len(jobs),
        succeeded=len(jobs) - failed_count,
        failed=failed_count,
        failed_jobs=failed_jobs,
    )
