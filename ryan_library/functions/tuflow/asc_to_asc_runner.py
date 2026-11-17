"""Run one native or external ASC_to_ASC-compatible raster operation.

Native execution is the normal application path and never requires the TUFLOW
utility. External execution is deliberately separate so a caller can load a
specific ``asc_to_asc.exe`` build and create a comparison raster for parity
testing. Output comparison itself remains a caller concern because some Python
NoData policies intentionally differ from ASC_to_ASC.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from .asc_to_asc_raster_operations import (
    MeanValueMethod,
    NodataPolicy,
    compute_max,
    compute_stat,
    flatten_nested_source_provenance,
    source_output_paths,
)
from .raster_performance import RasterExecutionOptions, RasterOperationTimings


@dataclass(slots=True, frozen=True)
class RasterOperationJob:
    """One independent raster operation and its native calculation policy."""

    label: str
    operation: str
    input_files: tuple[Path, ...]
    output_file: Path
    nodata_policy: NodataPolicy = "require_all"
    mean_value_method: MeanValueMethod = "closest_source"
    write_source: bool = False
    original_input_groups: tuple[tuple[Path, ...], ...] | None = None


@dataclass(slots=True, frozen=True)
class RasterJobResult:
    """Small serializable result returned by a profiled process-pool job."""

    output_file: Path
    timings: RasterOperationTimings


def run_profiled_python_raster_job(
    *, job: RasterOperationJob, execution_options: RasterExecutionOptions | None = None
) -> RasterJobResult:
    """Run the same native implementation and return its measured phases."""
    timings = RasterOperationTimings()
    output_file = run_python_raster_job(job=job, execution_options=execution_options, timings=timings)
    return RasterJobResult(output_file=output_file, timings=timings)


def run_python_raster_job(
    *,
    job: RasterOperationJob,
    execution_options: RasterExecutionOptions | None = None,
    timings: RasterOperationTimings | None = None,
) -> Path:
    """Execute one supported native raster operation.

    Supported operations are ``-max`` and the ``-statMean``, ``-statMedian``,
    ``-statMin`` and ``-statMax`` subset. Statistical jobs honour the job's
    Python-specific NoData and mean-value policies and also write source rasters
    and per-result source legends.
    """
    started = perf_counter()
    options = execution_options or RasterExecutionOptions()
    operation: str = job.operation.casefold()
    if operation not in {"-max", "-statmean", "-statmedian", "-statmin", "-statmax"}:
        msg = f"Unsupported native ASC_to_ASC operation: {job.operation}"
        raise RuntimeError(msg)
    job.output_file.parent.mkdir(parents=True, exist_ok=True)
    input_files: list[str] = [str(path) for path in job.input_files]
    fused_provenance = operation == "-statmax" and job.write_source and job.original_input_groups is not None
    nested_source_files = (
        [str(source_output_paths(str(input_file))[0]) for input_file in job.input_files]
        if job.write_source and job.original_input_groups is not None
        else None
    )
    original_input_groups = (
        [[str(original_input) for original_input in group] for group in job.original_input_groups]
        if job.original_input_groups is not None
        else None
    )

    try:
        if operation == "-max":
            compute_max(
                input_files=input_files,
                output_file=str(job.output_file),
                execution_options=options,
                timings=timings,
            )
        elif operation in {"-statmean", "-statmedian", "-statmin", "-statmax"}:
            compute_stat(
                stat_type=job.operation,
                input_files=input_files,
                output_file=str(job.output_file),
                nodata_policy=job.nodata_policy,
                mean_value_method=job.mean_value_method,
                write_source=job.write_source,
                execution_options=options,
                timings=timings,
                nested_source_files=nested_source_files if fused_provenance else None,
                original_input_groups=original_input_groups if fused_provenance else None,
            )
            if not fused_provenance and nested_source_files is not None and original_input_groups is not None:
                provenance_started = perf_counter()
                flatten_nested_source_provenance(
                    output_file=str(job.output_file),
                    nested_source_files=nested_source_files,
                    original_input_groups=original_input_groups,
                    execution_options=options,
                )
                if timings is not None:
                    timings.provenance_seconds += perf_counter() - provenance_started
    except Exception as error:
        msg = f"Python {job.operation} computation failed: {error}"
        raise RuntimeError(msg) from error
    finally:
        if timings is not None:
            timings.total_seconds = perf_counter() - started

    return job.output_file


def run_asc_to_asc_job(*, executable: Path, job: RasterOperationJob, output_file: Path | None = None) -> Path:
    """Run one job with a specific ASC_to_ASC executable for comparison.

    ``-src`` means *suppress* the auxiliary source grid. It is used for
    ``-min`` and ``-max``. It is not added automatically to ``-stat*`` because
    acceptance has varied between ASC_to_ASC builds; callers comparing those
    builds should account for any source rasters and legends they create.
    """
    if not executable.is_file():
        msg = f"ASC_to_ASC executable does not exist: {executable}"
        raise FileNotFoundError(msg)

    requested_output: Path = output_file or job.output_file
    requested_output.parent.mkdir(parents=True, exist_ok=True)
    operation: str = job.operation.casefold()
    command: list[str] = [str(executable), "-b"]
    if operation in {"-min", "-max"}:
        command.append("-src")
    command.extend(
        [
            "-out",
            str(requested_output),
            job.operation,
            *(str(input_file) for input_file in job.input_files),
        ]
    )

    completed_process: subprocess.CompletedProcess[str] = subprocess.run(
        args=command,
        check=False,
        input="\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )
    if completed_process.returncode != 0:
        output_tail: str = " | ".join(completed_process.stdout.strip().splitlines()[-3:])
        detail: str = f": {output_tail}" if output_tail else ""
        msg = f"ASC_to_ASC exited with code {completed_process.returncode}{detail}"
        raise RuntimeError(msg)

    actual_output = _resolve_asc_to_asc_output(requested_output=requested_output, operation=operation)
    if actual_output != requested_output and actual_output.is_file():
        actual_output.replace(requested_output)
    if not requested_output.is_file():
        msg = f"Expected output was not created: {requested_output}"
        raise FileNotFoundError(msg)
    return requested_output


def _resolve_asc_to_asc_output(*, requested_output: Path, operation: str) -> Path:
    """Return the requested output or an ASC_to_ASC statistic-suffixed output."""
    if requested_output.is_file():
        return requested_output

    suffixes: dict[str, str] = {
        "-statmean": "_Mean_Val",
        "-statmax": "_Max_Val",
        "-statmin": "_Min_Val",
        "-statmedian": "_Median_Val",
    }
    suffix: str | None = suffixes.get(operation)
    if suffix is None:
        return requested_output
    return requested_output.with_name(f"{requested_output.stem}{suffix}{requested_output.suffix}")
