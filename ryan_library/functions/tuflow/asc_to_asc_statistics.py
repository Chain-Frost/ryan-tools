# ryan_library/functions/tuflow/asc_to_asc_statistics.py
"""Shared helpers for parallel ASC_to_ASC raster-statistics workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import os
from pathlib import Path
import re
import subprocess

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard


@dataclass(slots=True, frozen=True)
class DashboardOptions:
    """Live-dashboard configuration shared by ASC_to_ASC workflows."""

    enabled: bool = True
    refresh_per_second: float = 2.0
    max_rows: int = 25


@dataclass(slots=True, frozen=True)
class StatisticJob:
    """One independent ASC_to_ASC operation."""

    label: str
    operation: str
    input_files: tuple[Path, ...]
    output_file: Path


@dataclass(slots=True, frozen=True)
class StageExecutionSummary:
    """Completion counts for one parallel processing stage."""

    total: int
    succeeded: int
    failed: int

    @property
    def ok(self) -> bool:
        return self.failed == 0


def result_type_from_parser(*, parser: TuflowStringParser, result_types: Sequence[str]) -> str | None:
    """Return a requested GeoTIFF result type identified by the parser."""
    if parser.data_type is None:
        return None
    canonical_types: dict[str, str] = {value.casefold(): value for value in result_types}
    return canonical_types.get(parser.data_type.casefold())


def require_component_text(*, value: str | None, component: str, filename: str) -> str:
    """Return non-empty parsed component text or raise a useful error."""
    if value:
        return value
    raise ValueError(f"Parsed {component} text was empty in {filename}")


def replace_filename_component(*, filename: str, old_component: str, new_component: str | None) -> str:
    """Replace or remove one complete ``_``- or ``+``-delimited component.

    Delimiters are retained exactly as supplied so the output keeps the input
    filename's separator style. When removing a component, its following
    delimiter is removed as well (or its preceding delimiter at the end).
    """
    parts: list[str] = re.split(r"([_+])", filename)
    indexes: list[int] = [
        index for index in range(0, len(parts), 2) if parts[index].casefold() == old_component.casefold()
    ]
    if len(indexes) != 1:
        raise ValueError(f"Expected one {old_component!r} component in {filename!r}; found {len(indexes)}")
    component_index: int = indexes[0]
    if new_component is None:
        if component_index + 1 < len(parts):
            del parts[component_index : component_index + 2]
        elif component_index > 0:
            del parts[component_index - 1 : component_index + 1]
        else:
            del parts[component_index]
    else:
        parts[component_index] = new_component
    return "".join(parts)


def format_user_template(*, template: str, values: Mapping[str, object], description: str) -> str:
    """Format a user-editable template and report unknown placeholders clearly."""
    try:
        return template.format_map(values)
    except KeyError as exc:
        available: str = ", ".join(sorted(values))
        raise ValueError(
            f"Unknown placeholder {exc.args[0]!r} in {description}; available placeholders: {available}"
        ) from exc


def validate_output_filename(filename: str) -> str:
    """Reject wildcards, paths, and Windows-invalid characters in an output name."""
    invalid_characters: set[str] = set('<>:"/\\|?*')
    found_invalid: list[str] = sorted(invalid_characters.intersection(filename))
    if found_invalid:
        raise ValueError(
            f"Output filename {filename!r} contains invalid characters: {''.join(found_invalid)}. "
            "Use wildcards only in the input glob and named placeholders in the output template."
        )
    if Path(filename).name != filename or filename in {"", ".", ".."}:
        raise ValueError(f"Output filename template must produce a filename, not a path: {filename!r}")
    return filename


def run_statistic_job(*, executable: Path, job: StatisticJob) -> Path:
    """Execute one ASC_to_ASC job while capturing console output for diagnostics."""
    job.output_file.parent.mkdir(parents=True, exist_ok=True)
    completed_process: subprocess.CompletedProcess[str] = subprocess.run(
        [
            str(executable),
            "-b",
            "-src",
            "-out",
            str(job.output_file),
            job.operation,
            *(str(input_file) for input_file in job.input_files),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )
    if completed_process.returncode != 0:
        output_tail: str = " | ".join(completed_process.stdout.strip().splitlines()[-3:])
        raise RuntimeError(
            f"ASC_to_ASC exited with code {completed_process.returncode}" + (f": {output_tail}" if output_tail else "")
        )
    if not job.output_file.is_file():
        raise FileNotFoundError(f"Expected output was not created: {job.output_file}")
    return job.output_file


def run_statistic_stage(
    *,
    executable: Path,
    jobs: Sequence[StatisticJob],
    stage_name: str,
    dashboard_title: str,
    dashboard_subtitle: str,
    workers: int | None,
    dashboard_options: DashboardOptions,
) -> StageExecutionSummary:
    """Run a bounded thread queue and report each external process in a dashboard."""
    if not jobs:
        return StageExecutionSummary(total=0, succeeded=0, failed=0)
    requested_workers: int = workers if workers is not None else 24
    if requested_workers < 1:
        raise ValueError("Worker count must be at least 1")
    worker_count: int = min(requested_workers, len(jobs))

    dashboard = LiveWorkflowDashboard(
        title=dashboard_title,
        subtitle=f"{stage_name} | PID {os.getpid()} | {dashboard_subtitle}",
        enabled=dashboard_options.enabled,
        refresh_per_second=dashboard_options.refresh_per_second,
        max_rows=dashboard_options.max_rows,
    )
    dashboard.set_tasks(labels=[job.label for job in jobs])
    dashboard.set_extra_metrics(metrics={"stage": stage_name, "workers": worker_count, "PID": os.getpid()})

    failed_count: int = 0
    indexed_jobs: enumerate[StatisticJob] = iter(enumerate(jobs, start=1))
    with dashboard, ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_jobs: dict[Future[Path], tuple[int, StatisticJob]] = {}

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
            future_jobs[executor.submit(run_statistic_job, executable=executable, job=job)] = (index, job)
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
                    failed_count += 1
                    dashboard.mark_finished(index=index, status="FAIL", detail=str(error), refresh=False)
                    dashboard.print(f"ERROR: {job.label} failed: {error}")
                submit_next()
            dashboard.set_active_count(count=len(future_jobs), refresh=False)
            dashboard.refresh(force=True)

    return StageExecutionSummary(total=len(jobs), succeeded=len(jobs) - failed_count, failed=failed_count)
