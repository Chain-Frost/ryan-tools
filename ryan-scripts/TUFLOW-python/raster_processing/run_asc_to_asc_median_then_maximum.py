"""Build temporal-pattern medians and duration maxima with native raster operations.

Edit the wrapper constants for the working directory, input glob, expected
temporal patterns, scenarios, result types, output folder, and dashboard
settings. Start with
``python run_asc_to_asc_median_then_maximum.py --dry-run`` to inspect discovered
groups without creating rasters; use ``--help`` for worker and strictness options.

The workflow completes all per-duration upper medians before calculating maxima across
durations. Input globs may contain wildcards, but output naming templates must
render concrete filenames and must not rediscover generated outputs.

Source rasters and CSV legends with per-source ``cell_count`` are enabled
by default for both stages. Set ``WRITE_SOURCE_RASTERS = False`` or use
``--no-source`` to disable them. Final legend IDs link directly to the
original temporal-pattern rasters.

For ``d_HR_Max`` and ``V_Max``, NoData contributes zero where some
other input has a valid cell; all-NoData locations remain NoData in both
stages. For elevation results ``h_Max`` and ``h_HR_Max``, NoData is
excluded from the statistic.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-18.1"

WORKING_DIR: Path = Path(__file__).absolute().parent
OUTPUT_DIRECTORY_NAME = "ensemble_statistics"
INPUT_GLOB = "*.tif"
EXPECTED_TPS: list[int] = list(range(1, 11))
SCENARIOS: list[str] = ["EXG", "DEV"]
RESULT_TYPES: list[str] = ["d_HR_Max", "h_HR_Max", "V_Max"]
WORKERS: int | None = None
# Memory settings use MiB. None selects 75% of available RAM and shares CPU threads.
GDAL_CACHE_MB = 128
CHUNK_MEMORY_MB = 256
TOTAL_MEMORY_BUDGET_MB: int | None = None
GDAL_THREADS: int | None = None
VSI_CACHE_MB = 0
REPORT_PHASE_TIMINGS = True
STRICT_INCOMPLETE_GROUPS = False
WRITE_SOURCE_RASTERS = True
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 10
LIVE_USE_ALTERNATE_SCREEN = False

import argparse
from dataclasses import dataclass

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.asc2asc_stat_then_max_by_search import run_stat_then_max_workflow


@dataclass(slots=True, frozen=True)
class CliOptions:
    common: CommonWrapperOptions
    workers: int | None
    dry_run: bool
    strict: bool | None
    write_source: bool | None
    cache_mb: int | None
    chunk_mb: int | None
    memory_budget_mb: int | None
    gdal_threads: int | None
    vsi_cache_mb: int | None
    report_phases: bool | None


def main(
    *,
    working_directory: Path | None = None,
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool | None = None,
    write_source: bool | None = None,
    cache_mb: int | None = None,
    chunk_mb: int | None = None,
    memory_budget_mb: int | None = None,
    gdal_threads: int | None = None,
    vsi_cache_mb: int | None = None,
    report_phases: bool | None = None,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
    live_use_alternate_screen: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the median-then-maximum orchestrator."""
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/raster_processing/run_asc_to_asc_median_then_maximum.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    search_root: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=search_root):
        return 1

    exit_code: int = run_stat_then_max_workflow(
        first_stage_statistic="median",
        search_root=search_root,
        output_root=search_root / OUTPUT_DIRECTORY_NAME,
        input_glob=INPUT_GLOB,
        expected_tps=frozenset(EXPECTED_TPS),
        scenarios=tuple(SCENARIOS),
        result_types=tuple(RESULT_TYPES),
        workers=workers if workers is not None else WORKERS,
        dry_run=dry_run,
        strict=STRICT_INCOMPLETE_GROUPS if strict is None else strict,
        write_source=WRITE_SOURCE_RASTERS if write_source is None else write_source,
        cache_mb=GDAL_CACHE_MB if cache_mb is None else cache_mb,
        chunk_mb=CHUNK_MEMORY_MB if chunk_mb is None else chunk_mb,
        memory_budget_mb=TOTAL_MEMORY_BUDGET_MB if memory_budget_mb is None else memory_budget_mb,
        gdal_threads=GDAL_THREADS if gdal_threads is None else gdal_threads,
        vsi_cache_mb=VSI_CACHE_MB if vsi_cache_mb is None else vsi_cache_mb,
        report_phases=REPORT_PHASE_TIMINGS if report_phases is None else report_phases,
        use_live_dashboard=USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard,
        live_refresh_per_second=(
            LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
        ),
        live_max_rows=LIVE_MAX_ROWS if live_max_rows is None else live_max_rows,
        live_use_alternate_screen=(
            LIVE_USE_ALTERNATE_SCREEN if live_use_alternate_screen is None else live_use_alternate_screen
        ),
    )
    return exit_code


def _parse_cli_arguments() -> CliOptions:
    parser = argparse.ArgumentParser(
        description="Create TP medians and maximums across durations. CLI options override wrapper defaults."
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument("--workers", type=int, help="Parallel native raster processes.")
    parser.add_argument("--cache-mb", type=int, help="Decoded GDAL cache per worker, in MiB (default 128).")
    parser.add_argument(
        "--chunk-mb", type=int, help="NumPy chunk working-memory target per worker, in MiB (default 256)."
    )
    parser.add_argument(
        "--memory-budget-mb",
        type=int,
        help="Total raster-worker memory budget in MiB; automatic default is 75%% of available RAM.",
    )
    parser.add_argument(
        "--gdal-threads", type=int, help="GDAL threads per worker; default shares logical CPUs across active workers."
    )
    parser.add_argument("--vsi-cache-mb", type=int, help="Additional raw-file cache per input, in MiB (default 0).")
    phase_group = parser.add_mutually_exclusive_group()
    phase_group.add_argument(
        "--report-phases",
        dest="report_phases",
        action="store_true",
        default=None,
        help="Report per-job and stage timings, including final close/flush (default).",
    )
    phase_group.add_argument("--no-report-phases", dest="report_phases", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="Validate groups without creating rasters.")
    strict_group = parser.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", default=None)
    strict_group.add_argument("--no-strict", dest="strict", action="store_false")
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--source",
        dest="write_source",
        action="store_true",
        default=None,
        help="Write source rasters and CSV legends with per-source cell counts (default).",
    )
    source_group.add_argument(
        "--no-source",
        dest="write_source",
        action="store_false",
        default=None,
        help="Disable source rasters and CSV legends.",
    )
    args: argparse.Namespace = parser.parse_args()
    return CliOptions(
        common=parse_common_cli_arguments(args=args),
        workers=args.workers,
        dry_run=args.dry_run,
        strict=args.strict,
        write_source=args.write_source,
        cache_mb=args.cache_mb,
        chunk_mb=args.chunk_mb,
        memory_budget_mb=args.memory_budget_mb,
        gdal_threads=args.gdal_threads,
        vsi_cache_mb=args.vsi_cache_mb,
        report_phases=args.report_phases,
    )


if __name__ == "__main__":
    cli_options: CliOptions = _parse_cli_arguments()
    result: int = main(
        working_directory=cli_options.common.working_directory,
        workers=cli_options.workers,
        dry_run=cli_options.dry_run,
        strict=cli_options.strict,
        write_source=cli_options.write_source,
        cache_mb=cli_options.cache_mb,
        chunk_mb=cli_options.chunk_mb,
        memory_budget_mb=cli_options.memory_budget_mb,
        gdal_threads=cli_options.gdal_threads,
        vsi_cache_mb=cli_options.vsi_cache_mb,
        report_phases=cli_options.report_phases,
        use_live_dashboard=cli_options.common.use_live_dashboard,
        live_refresh_per_second=cli_options.common.live_refresh_per_second,
        live_max_rows=cli_options.common.live_max_rows,
    )
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/raster_processing/run_asc_to_asc_median_then_maximum.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not cli_options.common.no_pause:
        pause_console()
    raise SystemExit(result)
