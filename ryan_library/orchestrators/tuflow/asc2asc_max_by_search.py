# ryan_library/orchestrators/tuflow/asc2asc_max_by_search.py
"""Create raster maximums from configurable file-search definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import product
from pathlib import Path

from ryan_library.functions.tuflow.asc_to_asc_statistics import (
    DashboardOptions,
    StatisticJob,
    format_user_template,
    run_statistic_stage,
    validate_output_filename,
)


@dataclass(slots=True, frozen=True)
class MaxSearch:
    """One input glob and its maximum-raster output filename."""

    label: str
    input_glob: str
    output_filename: str


def build_max_searches(
    *,
    input_glob_template: str,
    output_filename_template: str,
    template_axes: Mapping[str, Sequence[object]],
) -> tuple[MaxSearch, ...]:
    """Expand arbitrary template axes into a Cartesian product of searches."""
    axis_names: tuple[str, ...] = tuple(template_axes)
    if not axis_names:
        raise ValueError("At least one template axis must be configured")
    empty_axes: list[str] = [name for name in axis_names if not template_axes[name]]
    if empty_axes:
        raise ValueError(f"Template axes must not be empty: {', '.join(empty_axes)}")

    searches: list[MaxSearch] = []
    for combination in product(*(template_axes[name] for name in axis_names)):
        values: dict[str, object] = dict(zip(axis_names, combination, strict=True))
        label: str = ", ".join(f"{name}={value}" for name, value in values.items())
        searches.append(
            MaxSearch(
                label=label,
                input_glob=format_user_template(
                    template=input_glob_template,
                    values=values,
                    description="input glob template",
                ),
                output_filename=format_user_template(
                    template=output_filename_template,
                    values=values,
                    description="output filename template",
                ),
            )
        )
    return tuple(searches)


def discover_max_jobs(*, search_root: Path, searches: Sequence[MaxSearch]) -> list[StatisticJob]:
    """Resolve each configured glob into an independent maximum job."""
    if not search_root.is_dir():
        raise FileNotFoundError(f"Search root was not found: {search_root}")
    if not searches:
        raise ValueError("No maximum searches were configured")

    jobs: list[StatisticJob] = []
    seen_outputs: set[Path] = set()
    for search in searches:
        if Path(search.input_glob).is_absolute():
            raise ValueError(f"Input glob must be relative to the search root: {search.input_glob!r}")
        output_filename: str = validate_output_filename(search.output_filename)
        output_file: Path = search_root / output_filename
        normalized_output: Path = output_file.resolve()
        if normalized_output in seen_outputs:
            raise ValueError(f"Multiple searches produce the same output: {output_file}")
        seen_outputs.add(normalized_output)

        input_files: tuple[Path, ...] = tuple(
            sorted(
                path
                for path in search_root.glob(search.input_glob)
                if path.is_file() and path.resolve() != normalized_output
            )
        )
        if not input_files:
            raise FileNotFoundError(f"No rasters matched {search_root / search.input_glob}")
        jobs.append(
            StatisticJob(
                label=search.label,
                operation="-statMax",
                input_files=input_files,
                output_file=output_file,
            )
        )
    return jobs


def run_max_workflow(
    *,
    executable: Path,
    search_root: Path,
    searches: Sequence[MaxSearch],
    workers: int | None = None,
    dry_run: bool = False,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    live_use_alternate_screen: bool = False,
) -> int:
    """Validate, report, and optionally run configured raster-maximum jobs."""
    if workers is not None and workers < 1:
        print("ERROR: worker count must be at least 1")
        return 1
    if live_refresh_per_second <= 0 or live_max_rows < 1:
        print("ERROR: dashboard refresh and row values must be greater than zero")
        return 1
    if not executable.is_file():
        print(f"ERROR: ASC_to_ASC was not found at: {executable}")
        return 1

    try:
        jobs: list[StatisticJob] = discover_max_jobs(search_root=search_root, searches=searches)
    except (FileNotFoundError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1

    for job in jobs:
        print(f"{job.label}: {len(job.input_files)} rasters; output = {job.output_file.name}")
    if dry_run:
        print(f"Dry run complete: {len(jobs)} maximum jobs resolved.")
        return 0

    summary = run_statistic_stage(
        executable=executable,
        jobs=jobs,
        stage_name="raster maximum",
        dashboard_title="ASC_to_ASC Raster Maximums",
        dashboard_subtitle=str(search_root),
        workers=workers,
        dashboard_options=DashboardOptions(
            enabled=use_live_dashboard,
            refresh_per_second=live_refresh_per_second,
            max_rows=live_max_rows,
            use_alternate_screen=live_use_alternate_screen,
        ),
    )
    print(f"Finished maximum stage: {summary.succeeded} succeeded; {summary.failed} failed.")
    return 0 if summary.ok else 1
