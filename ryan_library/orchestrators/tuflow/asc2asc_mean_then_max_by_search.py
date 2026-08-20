# ryan_library/orchestrators/tuflow/asc2asc_mean_then_max_by_search.py
"""Create temporal-pattern means and then maximum means across durations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.tuflow.asc_to_asc_statistics import (
    DashboardOptions,
    StageExecutionSummary,
    StatisticJob,
    replace_filename_component,
    require_component_text,
    result_type_from_parser,
    run_statistic_stage,
)


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


@dataclass(slots=True, frozen=True)
class MeanJobDetails:
    """A mean job plus the fields needed for duration grouping."""

    job: StatisticJob
    grid_directory: Path
    scenario: str
    aep: str
    duration: str
    duration_minutes: float
    result_type: str
    trim_run_code: str
    max_name: str


def _parse_raster(
    *, input_file: Path, grid_directory: Path, scenarios: Sequence[str], result_types: Sequence[str]
) -> ParsedRaster | None:
    parser = TuflowStringParser(file_path=input_file)
    result_type: str | None = result_type_from_parser(parser=parser, result_types=result_types)
    if result_type is None:
        return None
    if parser.aep is None or parser.duration is None or parser.tp is None:
        logger.info(f"Could not parse AEP, duration, and TP from {input_file.name}")
        return None

    aep: str = require_component_text(value=parser.aep.original_text, component="AEP", filename=input_file.name)
    duration: str = require_component_text(
        value=parser.duration.original_text, component="duration", filename=input_file.name
    )
    tp: str = require_component_text(value=parser.tp.original_text, component="TP", filename=input_file.name)
    parsed_parts: set[str] = {part.casefold() for part in parser.run_code_parts.values()}
    matched_scenarios: list[str] = [scenario for scenario in scenarios if scenario.casefold() in parsed_parts]
    if len(matched_scenarios) != 1:
        raise ValueError(f"Expected one scenario in {input_file.name}; found {matched_scenarios}")

    mean_name: str = replace_filename_component(filename=input_file.name, old_component=tp, new_component="TPMean")
    max_name: str = replace_filename_component(
        filename=replace_filename_component(filename=input_file.name, old_component=tp, new_component="TPMean-DurMax"),
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
        mean_name=mean_name,
        max_name=max_name,
    )


def discover_rasters(
    *, search_root: Path, input_glob: str, scenarios: Sequence[str], result_types: Sequence[str]
) -> list[ParsedRaster]:
    """Discover supported rasters beneath matching grid directories."""
    rasters: list[ParsedRaster] = []
    grid_directories: list[Path] = sorted(path for path in search_root.rglob("grids") if path.is_dir())
    if not grid_directories:
        raise FileNotFoundError(f"No grids directories were found below {search_root}")
    for grid_directory in grid_directories:
        for input_file in sorted(path for path in grid_directory.glob(input_glob) if path.is_file()):
            parsed: ParsedRaster | None = _parse_raster(
                input_file=input_file,
                grid_directory=grid_directory,
                scenarios=scenarios,
                result_types=result_types,
            )
            if parsed is not None:
                rasters.append(parsed)
    if not rasters:
        raise FileNotFoundError("No supported ensemble result rasters were found")
    return rasters


def discover_mean_jobs(
    *, rasters: Sequence[ParsedRaster], output_root: Path, expected_tps: frozenset[int]
) -> tuple[list[MeanJobDetails], list[str]]:
    """Group exactly the expected temporal patterns for every duration."""
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

    jobs: list[MeanJobDetails] = []
    incomplete_groups: list[str] = []
    for group in groups.values():
        group.sort(key=lambda raster: raster.tp_number)
        representative: ParsedRaster = group[0]
        found_tps: list[int] = [raster.tp_number for raster in group]
        if len(found_tps) != len(set(found_tps)):
            raise ValueError(
                f"Duplicate temporal patterns in {representative.scenario} {representative.aep} "
                f"{representative.duration} {representative.result_type}: {found_tps}"
            )
        found_tp_set: frozenset[int] = frozenset(found_tps)
        if found_tp_set != expected_tps:
            missing: list[int] = sorted(expected_tps.difference(found_tp_set))
            incomplete_groups.append(
                f"{representative.scenario} {representative.aep} {representative.duration} "
                f"{representative.result_type}: found TP{', TP'.join(f'{tp:02d}' for tp in found_tps)}; "
                f"missing TP{', TP'.join(f'{tp:02d}' for tp in missing)}"
            )
            continue

        jobs.append(
            MeanJobDetails(
                job=StatisticJob(
                    label=(
                        f"mean {representative.scenario} {representative.aep} "
                        f"{representative.duration} {representative.result_type}"
                    ),
                    operation="-statMean",
                    input_files=tuple(raster.path for raster in group),
                    output_file=(
                        output_root / "means" / representative.scenario / representative.aep / representative.mean_name
                    ),
                ),
                grid_directory=representative.grid_directory,
                scenario=representative.scenario,
                aep=representative.aep,
                duration=representative.duration,
                duration_minutes=representative.duration_minutes,
                result_type=representative.result_type,
                trim_run_code=representative.trim_run_code,
                max_name=representative.max_name,
            )
        )
    jobs.sort(key=lambda item: (item.scenario, item.aep, item.result_type, item.duration_minutes))
    return jobs, sorted(incomplete_groups)


def discover_max_jobs(*, mean_jobs: Sequence[MeanJobDetails], output_root: Path) -> list[tuple[StatisticJob, list[str]]]:
    """Group duration means by model, scenario, AEP, and result type."""
    groups: dict[tuple[Path, str, str, str], list[MeanJobDetails]] = defaultdict(list)
    for details in mean_jobs:
        groups[
            (
                details.grid_directory,
                details.trim_run_code.casefold(),
                details.aep.casefold(),
                details.result_type.casefold(),
            )
        ].append(details)

    jobs: list[tuple[StatisticJob, list[str]]] = []
    for group in groups.values():
        group.sort(key=lambda details: details.duration_minutes)
        representative: MeanJobDetails = group[0]
        durations: list[str] = [details.duration for details in group]
        if len(durations) != len(set(durations)):
            raise ValueError(
                f"Duplicate mean durations for {representative.scenario} {representative.aep} "
                f"{representative.result_type}: {durations}"
            )
        jobs.append(
            (
                StatisticJob(
                    label=f"maximum mean {representative.scenario} {representative.aep} {representative.result_type}",
                    operation="-statMax",
                    input_files=tuple(details.job.output_file for details in group),
                    output_file=(
                        output_root
                        / "max_of_means"
                        / representative.scenario
                        / representative.aep
                        / representative.max_name
                    ),
                ),
                durations,
            )
        )
    return sorted(jobs, key=lambda item: item[0].label)


def run_mean_then_max_workflow(
    *,
    executable: Path,
    search_root: Path,
    output_root: Path,
    input_glob: str,
    expected_tps: frozenset[int],
    scenarios: Sequence[str],
    result_types: Sequence[str],
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool = False,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    live_use_alternate_screen: bool = False,
) -> int:
    """Validate and run the ensemble mean-then-maximum workflow."""
    if workers is not None and workers < 1:
        print("ERROR: worker count must be at least 1")
        return 1
    if live_refresh_per_second <= 0 or live_max_rows < 1:
        print("ERROR: dashboard refresh and row values must be greater than zero")
        return 1
    if not executable.is_file():
        print(f"ERROR: ASC_to_ASC was not found at: {executable}")
        return 1

    logger.disable("ryan_library")
    try:
        rasters: list[ParsedRaster] = discover_rasters(
            search_root=search_root,
            input_glob=input_glob,
            scenarios=scenarios,
            result_types=result_types,
        )
        mean_jobs, incomplete_groups = discover_mean_jobs(
            rasters=rasters, output_root=output_root, expected_tps=expected_tps
        )
        max_job_data: list[tuple[StatisticJob, list[str]]] = discover_max_jobs(mean_jobs=mean_jobs, output_root=output_root)
    except (FileNotFoundError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1

    incomplete_count = len(incomplete_groups)
    incomplete_text = f" (excluded {incomplete_count} incomplete groups)" if incomplete_count else ""
    print(f"Found {len(rasters)} supported input rasters.")
    print(f"Validated {len(mean_jobs)} complete TP mean groups{incomplete_text}.")
    print(f"Prepared {len(max_job_data)} maximum-of-means groups.")
    for job, durations in max_job_data:
        print(f"  {job.label}: {len(job.input_files)} duration means ({', '.join(durations)})")
    for incomplete in incomplete_groups:
        print(f"WARNING: excluded incomplete group: {incomplete}")
    if incomplete_groups and strict:
        print(f"ERROR: {incomplete_count} incomplete groups found in strict mode.")
        return 1
    if dry_run:
        print("Dry run complete; no rasters were created.")
        return 0
    if not mean_jobs or not max_job_data:
        print("ERROR: no complete statistic jobs were prepared")
        return 1

    dashboard_options = DashboardOptions(
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
        use_alternate_screen=live_use_alternate_screen,
    )
    mean_summary: StageExecutionSummary = run_statistic_stage(
        executable=executable,
        jobs=[details.job for details in mean_jobs],
        stage_name="temporal-pattern mean",
        dashboard_title="ASC_to_ASC Ensemble Statistics",
        dashboard_subtitle=str(search_root),
        workers=workers,
        dashboard_options=dashboard_options,
    )
    print(f"Finished mean stage: {mean_summary.succeeded} succeeded; {mean_summary.failed} failed.")
    if not mean_summary.ok:
        print("ERROR: mean stage failed; maximum stage was not started.")
        for label, error in mean_summary.failed_jobs:
            print(f"  - {label} failed: {error}")
        return 1

    max_summary: StageExecutionSummary = run_statistic_stage(
        executable=executable,
        jobs=[job for job, _ in max_job_data],
        stage_name="maximum of means",
        dashboard_title="ASC_to_ASC Ensemble Statistics",
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
        
    print(f"Finished. Outputs are under {output_root}")
    return 0
