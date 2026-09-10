"""Compatibility API for the mean-then-maximum raster workflow.

New code should use asc2asc_stat_then_max_by_search. These forwarding APIs may
be removed after 2026-12-31.
"""

from __future__ import annotations

# pyright: reportUnusedFunction=false, reportPrivateUsage=false
from collections.abc import Sequence
from pathlib import Path
from ryan_library.functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod
from ryan_library.functions.tuflow.asc_to_asc_runner import RasterOperationJob
from ryan_library.orchestrators.tuflow.asc2asc_stat_then_max_by_search import (
    FirstStageJobDetails,
    ParsedRaster,
    _nodata_policy_for_result_type,
    _parse_raster as _parse_stat_raster,
    discover_max_jobs as _discover_max_jobs,
    discover_rasters as _discover_rasters,
    discover_stat_jobs,
    run_stat_then_max_workflow,
)

MeanJobDetails = FirstStageJobDetails

__all__ = [
    "MeanJobDetails",
    "ParsedRaster",
    "_nodata_policy_for_result_type",
    "_parse_raster",
    "discover_rasters",
    "discover_mean_jobs",
    "discover_max_jobs",
    "run_mean_then_max_workflow",
]


def _parse_raster(
    *, input_file: Path, grid_directory: Path, scenarios: Sequence[str], result_types: Sequence[str]
) -> ParsedRaster | None:
    """Parse a mean-workflow raster; retained through 2026-12-31."""
    return _parse_stat_raster(
        input_file=input_file,
        grid_directory=grid_directory,
        scenarios=scenarios,
        result_types=result_types,
        first_stage_statistic="mean",
    )


def discover_rasters(
    *, search_root: Path, input_glob: str, scenarios: Sequence[str], result_types: Sequence[str]
) -> list[ParsedRaster]:
    """Discover mean-workflow rasters; retained through 2026-12-31."""
    return _discover_rasters(
        search_root=search_root,
        input_glob=input_glob,
        scenarios=scenarios,
        result_types=result_types,
        first_stage_statistic="mean",
    )


def discover_mean_jobs(
    *,
    rasters: Sequence[ParsedRaster],
    output_root: Path,
    expected_tps: frozenset[int],
    mean_value_method: MeanValueMethod = "asc_to_asc",
    write_source: bool = False,
) -> tuple[list[FirstStageJobDetails], list[str]]:
    """Prepare mean jobs; retained through 2026-12-31."""
    return discover_stat_jobs(
        rasters=rasters,
        output_root=output_root,
        expected_tps=expected_tps,
        first_stage_statistic="mean",
        mean_value_method=mean_value_method,
        write_source=write_source,
    )


def discover_max_jobs(
    *, mean_jobs: Sequence[FirstStageJobDetails], output_root: Path, write_source: bool = False
) -> list[tuple[RasterOperationJob, list[str]]]:
    """Prepare maximum-of-means jobs; retained through 2026-12-31."""
    return _discover_max_jobs(stat_jobs=mean_jobs, output_root=output_root, write_source=write_source)


def run_mean_then_max_workflow(
    *,
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
    write_source: bool = False,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    live_use_alternate_screen: bool = False,
) -> int:
    """Run mean then maximum; retained through 2026-12-31."""
    return run_stat_then_max_workflow(
        first_stage_statistic="mean",
        search_root=search_root,
        output_root=output_root,
        input_glob=input_glob,
        expected_tps=expected_tps,
        scenarios=scenarios,
        result_types=result_types,
        mean_value_method=mean_value_method,
        workers=workers,
        dry_run=dry_run,
        strict=strict,
        write_source=write_source,
        use_live_dashboard=use_live_dashboard,
        live_refresh_per_second=live_refresh_per_second,
        live_max_rows=live_max_rows,
        live_use_alternate_screen=live_use_alternate_screen,
    )
