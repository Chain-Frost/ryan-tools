# ryan_library/orchestrators/tuflow/asc2asc_stat_then_max_by_search.py
"""Create a temporal-pattern statistic, then its maximum across durations.

The first stage calculates a configurable mean or ASC_to_ASC-compatible upper
median for each duration. Mean workflows default to the ASC_to_ASC rule: write
the arithmetic mean and select the lowest original value at or above it for
source provenance. With source output enabled, the final source legend is
flattened to identify the original temporal-pattern rasters directly.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from loguru import logger

from ...classes.tuflow_string_classes import TuflowStringParser
from ...functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod, NodataPolicy
from ...functions.tuflow.asc_to_asc_runner import RasterOperationJob
from ...functions.tuflow.tuflow_result_naming import (
    replace_filename_component,
    require_component_text,
    result_type_from_parser,
)
from .asc_to_asc_batch import (
    DashboardOptions,
    StageExecutionSummary,
    run_raster_operation_stage,
)

type FirstStageStatistic = Literal["mean", "median"]


@dataclass(slots=True, frozen=True)
class ParsedRaster:
    """Parsed fields needed to group an ensemble raster."""

    path: Path
    grid_directory: Path
    scenario: str
    aep: str
    duration: str
    duration_minutes: float
    tp_number: int
    result_type: str
    trim_run_code: str
    mean_name: str
    max_name: str

    @property
    def statistic_name(self) -> str:
        """Return the statistic-neutral first-stage output name.

        ``mean_name`` remains the stored field for constructor compatibility
        through 2026-12-31.
        """
        return self.mean_name

    @property
    def maximum_name(self) -> str:
        """Return the statistic-neutral maximum output name."""
        return self.max_name


@dataclass(slots=True, frozen=True)
class FirstStageJobDetails:
    """A first-stage statistic job plus fields needed for duration grouping."""

    job: RasterOperationJob
    grid_directory: Path
    scenario: str
    aep: str
    duration: str
    duration_minutes: float
    result_type: str
    trim_run_code: str
    max_name: str
    first_stage_statistic: FirstStageStatistic = "mean"

    @property
    def maximum_name(self) -> str:
        """Return the statistic-neutral maximum output name.

        ``max_name`` remains the stored field for constructor compatibility
        through 2026-12-31.
        """
        return self.max_name


def _statistic_settings(statistic: FirstStageStatistic) -> tuple[str, str, str, str]:
    """Return operation, filename token, statistic folder, and maximum folder."""
    if statistic == "mean":
        return "-statMean", "TPMean", "means", "max_of_means"
    if statistic == "median":
        return "-statMedian", "TPMedian", "medians", "max_of_medians"
    msg = f"Unsupported first-stage statistic: {statistic}"
    raise ValueError(msg)


def _nodata_policy_for_result_type(result_type: str) -> NodataPolicy:
    """Return the hydraulic interpretation of NoData for an ensemble result."""
    normalized_result_type: str = result_type.casefold()
    if normalized_result_type in {"d_hr_max", "v_max"}:
        return "zero"
    if normalized_result_type in {"h_max", "h_hr_max"}:
        return "exclude"
    return "require_all"


def _parse_raster(
    *,
    input_file: Path,
    grid_directory: Path,
    scenarios: Sequence[str],
    result_types: Sequence[str],
    first_stage_statistic: FirstStageStatistic,
) -> ParsedRaster | None:
    parser = TuflowStringParser(file_path=input_file)
    result_type: str | None = result_type_from_parser(parser=parser, result_types=result_types)
    if result_type is None:
        return None
    if parser.aep is None or parser.duration is None or parser.tp is None:
        logger.info("Could not parse AEP, duration, and TP from {}", input_file.name)
        return None

    aep: str = require_component_text(value=parser.aep.original_text, component="AEP", filename=input_file.name)
    duration: str = require_component_text(
        value=parser.duration.original_text, component="duration", filename=input_file.name
    )
    tp: str = require_component_text(value=parser.tp.original_text, component="TP", filename=input_file.name)
    parsed_parts: set[str] = {part.casefold() for part in parser.run_code_parts.values()}
    matched_scenarios: list[str] = [scenario for scenario in scenarios if scenario.casefold() in parsed_parts]
    if len(matched_scenarios) != 1:
        msg = f"Expected one scenario in {input_file.name}; found {matched_scenarios}"
        raise ValueError(msg)

    _, statistic_token, _, _ = _statistic_settings(first_stage_statistic)
    statistic_name: str = replace_filename_component(
        filename=input_file.name, old_component=tp, new_component=statistic_token
    )
    maximum_name: str = replace_filename_component(
        filename=replace_filename_component(
            filename=input_file.name, old_component=tp, new_component=f"{statistic_token}-DurMax"
        ),
        old_component=duration,
        new_component=None,
    )
    return ParsedRaster(
        path=input_file,
        grid_directory=grid_directory,
        scenario=matched_scenarios[0],
        aep=aep,
        duration=duration,
        duration_minutes=float(parser.duration.raw_value),
        tp_number=int(parser.tp.raw_value),
        result_type=result_type,
        trim_run_code=parser.trim_run_code,
        mean_name=statistic_name,
        max_name=maximum_name,
    )


def discover_rasters(
    *,
    search_root: Path,
    input_glob: str,
    scenarios: Sequence[str],
    result_types: Sequence[str],
    first_stage_statistic: FirstStageStatistic,
) -> list[ParsedRaster]:
    """Discover supported rasters beneath matching grid directories."""
    rasters: list[ParsedRaster] = []
    grid_directories: list[Path] = sorted(path for path in search_root.rglob("grids") if path.is_dir())
    if not grid_directories:
        msg = f"No grids directories were found below {search_root}"
        raise FileNotFoundError(msg)
    for grid_directory in grid_directories:
        for input_file in sorted(path for path in grid_directory.glob(input_glob) if path.is_file()):
            parsed: ParsedRaster | None = _parse_raster(
                input_file=input_file,
                grid_directory=grid_directory,
                scenarios=scenarios,
                result_types=result_types,
                first_stage_statistic=first_stage_statistic,
            )
            if parsed is not None:
                rasters.append(parsed)
    if not rasters:
        msg = "No supported ensemble result rasters were found"
        raise FileNotFoundError(msg)
    return rasters


def discover_stat_jobs(
    *,
    rasters: Sequence[ParsedRaster],
    output_root: Path,
    expected_tps: frozenset[int],
    first_stage_statistic: FirstStageStatistic,
    mean_value_method: MeanValueMethod = "asc_to_asc",
    write_source: bool = False,
    allow_incomplete_tp_bins: bool = False,
) -> tuple[list[FirstStageJobDetails], list[str]]:
    """Prepare TP statistics, optionally admitting nonempty incomplete bins.

    Zero-byte inputs count as missing contributors. Duplicate or unexpected TP
    IDs remain errors, and no missing raster is substituted with a zero value.
    The returned findings include admitted and wholly unusable bins.
    """
    if not expected_tps or any(tp < 1 for tp in expected_tps):
        msg = "Expected temporal patterns must be a nonempty set of positive TP numbers"
        raise ValueError(msg)
    groups: dict[tuple[Path, str, str, str, str], list[ParsedRaster]] = defaultdict(list)
    for raster in rasters:
        groups[
            (
                raster.grid_directory,
                raster.trim_run_code.casefold(),
                raster.aep.casefold(),
                raster.duration.casefold(),
                raster.result_type.casefold(),
            )
        ].append(raster)

    operation, _, statistic_directory, _ = _statistic_settings(first_stage_statistic)
    jobs: list[FirstStageJobDetails] = []
    incomplete_groups: list[str] = []
    for group in groups.values():
        group.sort(key=lambda raster: raster.tp_number)
        representative: ParsedRaster = group[0]
        found_tps: list[int] = [raster.tp_number for raster in group]
        if len(found_tps) != len(set(found_tps)):
            msg = (
                f"Duplicate temporal patterns in {representative.scenario} {representative.aep} "
                f"{representative.duration} {representative.result_type}: {found_tps}"
            )
            raise ValueError(msg)
        unexpected = sorted(set(found_tps).difference(expected_tps))
        if unexpected:
            msg = (
                f"Unexpected temporal patterns in {representative.scenario} {representative.aep} "
                f"{representative.duration} {representative.result_type}: {unexpected}; "
                "check EXPECTED_TPS and the input selection"
            )
            raise ValueError(msg)
        empty_inputs: list[Path] = []
        available: list[ParsedRaster] = []
        for raster in group:
            if raster.path.stat().st_size == 0:
                empty_inputs.append(raster.path)
            else:
                available.append(raster)
        found_tps = [raster.tp_number for raster in available]
        found_tp_set: frozenset[int] = frozenset(found_tps)
        if found_tp_set != expected_tps:
            missing: list[int] = sorted(expected_tps.difference(found_tp_set))
            found_text = ", ".join(f"TP{tp:02d}" for tp in found_tps) or "none"
            missing_text = ", ".join(f"TP{tp:02d}" for tp in missing)
            empty_text = (
                f"; ignored zero-byte inputs: {', '.join(str(path) for path in empty_inputs)}" if empty_inputs else ""
            )
            unavailable_text = "; no usable inputs, bin excluded" if not available else ""
            incomplete_groups.append(
                f"{representative.scenario} {representative.aep} {representative.duration} "
                f"{representative.result_type} [{representative.trim_run_code}; {representative.grid_directory}]: "
                f"found {found_text}; missing {missing_text}{empty_text}{unavailable_text}"
            )
            if not allow_incomplete_tp_bins or not available:
                continue

        jobs.append(
            FirstStageJobDetails(
                job=RasterOperationJob(
                    label=(
                        f"{first_stage_statistic} {representative.scenario} {representative.aep} "
                        f"{representative.duration} {representative.result_type}"
                    ),
                    operation=operation,
                    input_files=tuple(raster.path for raster in available),
                    output_file=(
                        output_root
                        / statistic_directory
                        / representative.scenario
                        / representative.aep
                        / representative.statistic_name
                    ),
                    nodata_policy=_nodata_policy_for_result_type(representative.result_type),
                    mean_value_method=mean_value_method,
                    write_source=write_source,
                ),
                grid_directory=representative.grid_directory,
                scenario=representative.scenario,
                aep=representative.aep,
                duration=representative.duration,
                duration_minutes=representative.duration_minutes,
                result_type=representative.result_type,
                trim_run_code=representative.trim_run_code,
                max_name=representative.maximum_name,
                first_stage_statistic=first_stage_statistic,
            )
        )
    jobs.sort(key=lambda item: (item.scenario, item.aep, item.result_type, item.duration_minutes))
    return jobs, sorted(incomplete_groups)


def _maximum_group_key(details: ParsedRaster | FirstStageJobDetails) -> tuple[Path, str, str, str]:
    """Identify independent maxima consistently before and after TP grouping."""
    return (
        details.grid_directory,
        details.trim_run_code.casefold(),
        details.aep.casefold(),
        details.result_type.casefold(),
    )


def discover_max_jobs(
    *, stat_jobs: Sequence[FirstStageJobDetails], output_root: Path, write_source: bool = False
) -> list[tuple[RasterOperationJob, list[str]]]:
    """Group duration statistics by model, scenario, AEP, and result type."""
    groups: dict[tuple[Path, str, str, str], list[FirstStageJobDetails]] = defaultdict(list)
    for details in stat_jobs:
        groups[_maximum_group_key(details)].append(details)

    jobs: list[tuple[RasterOperationJob, list[str]]] = []
    for group in groups.values():
        group.sort(key=lambda details: details.duration_minutes)
        representative: FirstStageJobDetails = group[0]
        statistics = {details.first_stage_statistic for details in group}
        if len(statistics) != 1:
            msg = f"Mixed first-stage statistics in one maximum group: {sorted(statistics)}"
            raise ValueError(msg)
        statistic = representative.first_stage_statistic
        _, _, _, maximum_directory = _statistic_settings(statistic)
        durations: list[str] = [details.duration for details in group]
        if len(durations) != len(set(durations)):
            msg = (
                f"Duplicate {statistic} durations for {representative.scenario} {representative.aep} "
                f"{representative.result_type}: {durations}"
            )
            raise ValueError(msg)
        jobs.append(
            (
                RasterOperationJob(
                    label=f"maximum {statistic} {representative.scenario} {representative.aep} {representative.result_type}",
                    operation="-statMax",
                    input_files=tuple(details.job.output_file for details in group),
                    output_file=(
                        output_root
                        / maximum_directory
                        / representative.scenario
                        / representative.aep
                        / representative.maximum_name
                    ),
                    nodata_policy=_nodata_policy_for_result_type(representative.result_type),
                    write_source=write_source,
                    original_input_groups=(
                        tuple(details.job.input_files for details in group) if write_source else None
                    ),
                ),
                durations,
            )
        )
    return sorted(jobs, key=lambda item: item[0].label)


def run_stat_then_max_workflow(
    *,
    first_stage_statistic: FirstStageStatistic,
    search_root: Path,
    output_root: Path,
    input_glob: str,
    expected_tps: frozenset[int],
    scenarios: Sequence[str],
    result_types: Sequence[str],
    mean_value_method: MeanValueMethod = "asc_to_asc",
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool = False,
    allow_incomplete_tp_bins: bool = False,
    write_source: bool = False,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    live_use_alternate_screen: bool = False,
) -> int:
    """Validate and run the statistic-then-maximum workflow.

    Mean workflows default to ASC_to_ASC value/source selection. The option is
    accepted for median workflows for a uniform API but does not affect median
    calculation. Without the incomplete-TP override, skipped bins withhold
    their model/AEP/result maximum and cause a nonzero exit. Strict mode stops
    before either stage and cannot be combined with the override.
    """
    try:
        _statistic_settings(first_stage_statistic)
    except ValueError as error:
        print(f"ERROR: {error}")
        return 1
    if mean_value_method not in {"closest_source", "arithmetic", "asc_to_asc"}:
        print(f"ERROR: unsupported mean value method: {mean_value_method}")
        return 1
    if workers is not None and workers < 1:
        print("ERROR: worker count must be at least 1")
        return 1
    if live_refresh_per_second <= 0 or live_max_rows < 1:
        print("ERROR: dashboard refresh and row values must be greater than zero")
        return 1
    if strict and allow_incomplete_tp_bins:
        print("ERROR: --strict cannot be combined with --allow-incomplete-tp-bins; disable one of these settings.")
        return 1
    logger.disable("ryan_library")
    try:
        rasters: list[ParsedRaster] = discover_rasters(
            search_root=search_root,
            input_glob=input_glob,
            scenarios=scenarios,
            result_types=result_types,
            first_stage_statistic=first_stage_statistic,
        )
        stat_jobs, incomplete_groups = discover_stat_jobs(
            rasters=rasters,
            output_root=output_root,
            expected_tps=expected_tps,
            first_stage_statistic=first_stage_statistic,
            mean_value_method=mean_value_method,
            write_source=write_source,
            allow_incomplete_tp_bins=allow_incomplete_tp_bins,
        )
        blocked_maximum_groups: set[tuple[Path, str, str, str]] = set()
        if incomplete_groups and not allow_incomplete_tp_bins:
            prepared_inputs = {path for details in stat_jobs for path in details.job.input_files}
            blocked_maximum_groups = {
                _maximum_group_key(raster) for raster in rasters if raster.path not in prepared_inputs
            }
        maximum_stat_jobs = [
            details for details in stat_jobs if _maximum_group_key(details) not in blocked_maximum_groups
        ]
        max_job_data: list[tuple[RasterOperationJob, list[str]]] = discover_max_jobs(
            stat_jobs=maximum_stat_jobs,
            output_root=output_root,
            write_source=write_source,
        )
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1

    incomplete_count = len(incomplete_groups)
    admitted_count = sum(len(details.job.input_files) < len(expected_tps) for details in stat_jobs)
    excluded_count = incomplete_count - admitted_count
    print(f"Found {len(rasters)} supported input rasters.")
    print(
        f"Prepared {len(stat_jobs)} TP {first_stage_statistic} groups: "
        f"{len(stat_jobs) - admitted_count} complete; {admitted_count} incomplete admitted; {excluded_count} excluded."
    )
    print(f"Prepared {len(max_job_data)} maximum-of-{first_stage_statistic}s groups.")
    if first_stage_statistic == "mean":
        print(f"Mean value method: {mean_value_method}.")
    print(f"Source rasters and original-input legends: {'enabled' if write_source else 'disabled'}.")
    print(f"Incomplete TP bin override: {'enabled' if allow_incomplete_tp_bins else 'disabled'}.")
    for job, durations in max_job_data:
        print(f"  {job.label}: {len(job.input_files)} duration {first_stage_statistic}s ({', '.join(durations)})")
    for incomplete in incomplete_groups:
        print(f"WARNING: incomplete TP bin: {incomplete}")
    if incomplete_groups and allow_incomplete_tp_bins:
        print(
            "WARNING: proceeding with incomplete TP bins by explicit override. Statistics use only available "
            "non-empty rasters; bins with no usable inputs are omitted. Outputs do not represent the full TP ensemble."
        )
    if blocked_maximum_groups:
        print(
            f"WARNING: withholding {len(blocked_maximum_groups)} affected maximum groups because TP bins are incomplete. "
            "Regenerate the missing rasters, or explicitly enable --allow-incomplete-tp-bins."
        )
        for grid_directory, run_code, aep, result_type in sorted(blocked_maximum_groups):
            print(f"  Withheld maximum: {run_code} {aep} {result_type} [{grid_directory}]")
        print("WARNING: any existing affected maximum files are left unchanged and are not results of this run.")
    if incomplete_groups and strict:
        print(f"ERROR: {incomplete_count} incomplete groups found in strict mode.")
        return 1
    if not stat_jobs:
        print("ERROR: no usable statistic jobs were prepared")
        return 1
    incomplete_failure = bool(incomplete_groups) and not allow_incomplete_tp_bins
    if dry_run:
        print("Dry run complete; no rasters were created.")
        return 1 if incomplete_failure else 0

    dashboard_options = DashboardOptions(
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
        use_alternate_screen=live_use_alternate_screen,
    )
    statistic_summary: StageExecutionSummary = run_raster_operation_stage(
        jobs=[details.job for details in stat_jobs],
        stage_name=f"temporal-pattern {first_stage_statistic}",
        dashboard_title="Native Ensemble Raster Statistics",
        dashboard_subtitle=str(search_root),
        workers=workers,
        dashboard_options=dashboard_options,
    )
    print(
        f"Finished {first_stage_statistic} stage: {statistic_summary.succeeded} succeeded; {statistic_summary.failed} failed."
    )
    if not statistic_summary.ok:
        print(f"ERROR: {first_stage_statistic} stage failed; maximum stage was not started.")
        for label, error in statistic_summary.failed_jobs:
            print(f"  - {label} failed: {error}")
        return 1

    if not max_job_data:
        print("ERROR: maximum stage was not started because no complete maximum groups were prepared.")
        return 1

    max_summary: StageExecutionSummary = run_raster_operation_stage(
        jobs=[job for job, _ in max_job_data],
        stage_name=f"maximum of {first_stage_statistic}s",
        dashboard_title="Native Ensemble Raster Statistics",
        dashboard_subtitle=str(search_root),
        workers=workers,
        dashboard_options=dashboard_options,
    )
    print(f"Finished maximum stage: {max_summary.succeeded} succeeded; {max_summary.failed} failed.")
    if not max_summary.ok:
        print("ERROR: maximum stage completed with failures.")
        for label, error in max_summary.failed_jobs:
            print(f"  - {label} failed: {error}")
        return 1

    if incomplete_failure:
        print(f"ERROR: incomplete TP bins withheld affected maxima. Available outputs are under {output_root}")
        return 1
    if incomplete_groups:
        print("WARNING: finished using incomplete TP bins; review the missing-TP findings before using these outputs.")
    print(f"Finished. Outputs are under {output_root}")
    return 0
