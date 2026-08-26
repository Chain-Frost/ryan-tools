"""Run one native or external ASC_to_ASC-compatible raster operation.

Native execution is the normal application path and never requires the TUFLOW
utility. External execution is deliberately separate so a caller can load a
specific ``asc_to_asc.exe`` build and create a comparison raster for parity
testing. Output comparison itself remains a caller concern because some Python
NoData policies intentionally differ from ASC_to_ASC.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import subprocess

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import (
    MeanValueMethod,
    NodataPolicy,
    compute_max,
    compute_stat,
    flatten_nested_source_provenance,
    source_output_paths,
)


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


def run_python_raster_job(*, job: RasterOperationJob) -> Path:
    """Execute one supported native raster operation.

    Supported operations are ``-max`` and the ``-statMean``, ``-statMedian``,
    ``-statMin`` and ``-statMax`` subset. Statistical jobs honour the job's
    Python-specific NoData and mean-value policies and also write source rasters
    and per-result source legends.
    """
    job.output_file.parent.mkdir(parents=True, exist_ok=True)
    operation: str = job.operation.casefold()
    input_files: list[str] = [str(path) for path in job.input_files]

    try:
        if operation == "-max":
            compute_max(input_files=input_files, output_file=str(job.output_file))
        elif operation in {"-statmean", "-statmedian", "-statmin", "-statmax"}:
            compute_stat(
                stat_type=job.operation,
                input_files=input_files,
                output_file=str(job.output_file),
                nodata_policy=job.nodata_policy,
                mean_value_method=job.mean_value_method,
                write_source=job.write_source,
            )
            if job.write_source and job.original_input_groups is not None:
                nested_source_files = [str(source_output_paths(str(input_file))[0]) for input_file in job.input_files]
                flatten_nested_source_provenance(
                    output_file=str(job.output_file),
                    nested_source_files=nested_source_files,
                    original_input_groups=[
                        [str(original_input) for original_input in group] for group in job.original_input_groups
                    ],
                )
        else:
            raise ValueError(f"Unsupported native ASC_to_ASC operation: {job.operation}")
    except Exception as error:
        raise RuntimeError(f"Python {job.operation} computation failed: {error}") from error

    return job.output_file


def run_asc_to_asc_job(*, executable: Path, job: RasterOperationJob, output_file: Path | None = None) -> Path:
    """Run one job with a specific ASC_to_ASC executable for comparison.

    ``-src`` means *suppress* the auxiliary source grid. It is used for
    ``-min`` and ``-max``. It is not added automatically to ``-stat*`` because
    acceptance has varied between ASC_to_ASC builds; callers comparing those
    builds should account for any source rasters and legends they create.
    """
    if not executable.is_file():
        raise FileNotFoundError(f"ASC_to_ASC executable does not exist: {executable}")

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
        raise RuntimeError(f"ASC_to_ASC exited with code {completed_process.returncode}{detail}")

    actual_output = _resolve_asc_to_asc_output(requested_output=requested_output, operation=operation)
    if actual_output != requested_output and actual_output.is_file():
        actual_output.replace(requested_output)
    if not requested_output.is_file():
        raise FileNotFoundError(f"Expected output was not created: {requested_output}")
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
