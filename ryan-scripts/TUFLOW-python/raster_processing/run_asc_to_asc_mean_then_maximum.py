"""Build temporal-pattern means and duration maxima with native raster operations.

Edit the wrapper constants for the working directory, input glob, expected
temporal patterns, scenarios, result types, output folder, and dashboard
settings. Start with
``python run_asc_to_asc_mean_then_maximum.py --dry-run`` to inspect discovered
groups without creating rasters; use ``--help`` for worker and strictness options.

The workflow completes all per-duration means before calculating maxima across
durations. By default, the value raster contains the arithmetic mean and its
source identifies the lowest contributing value at or above that mean, matching
ASC_to_ASC. Choose another ``MEAN_VALUE_METHOD`` or use
``--mean-value-method`` when different value/source selection is required.
Input globs may contain wildcards, but output naming templates must render
concrete filenames and must not rediscover generated outputs.

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
from typing import Literal

WRAPPER_VERSION = "2026-11-09.1"

WORKING_DIR: Path = Path(__file__).absolute().parent
OUTPUT_DIRECTORY_NAME = "ensemble_statistics"
INPUT_GLOB = "*.tif"
EXPECTED_TPS: list[int] = list(range(1, 11))
SCENARIOS: list[str] = ["EXG", "DEV"]
RESULT_TYPES: list[str] = ["d_HR_Max", "h_HR_Max", "V_Max"]
MEAN_VALUE_METHOD: Literal["asc_to_asc", "closest_source", "arithmetic"] = "asc_to_asc"
WORKERS: int | None = None
STRICT_INCOMPLETE_GROUPS = False
WRITE_SOURCE_RASTERS = True
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 10
LIVE_USE_ALTERNATE_SCREEN = False

import argparse
from dataclasses import dataclass

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod
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
    mean_value_method: MeanValueMethod | None
    workers: int | None
    dry_run: bool
    strict: bool | None
    write_source: bool | None


def main(
    *,
    working_directory: Path | None = None,
    mean_value_method: MeanValueMethod | None = None,
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool | None = None,
    write_source: bool | None = None,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
    live_use_alternate_screen: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the mean-then-maximum orchestrator."""
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/raster_processing/run_asc_to_asc_mean_then_maximum.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    search_root: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=search_root):
        return 1

    exit_code: int = run_stat_then_max_workflow(
        first_stage_statistic="mean",
        search_root=search_root,
        output_root=search_root / OUTPUT_DIRECTORY_NAME,
        input_glob=INPUT_GLOB,
        expected_tps=frozenset(EXPECTED_TPS),
        scenarios=tuple(SCENARIOS),
        result_types=tuple(RESULT_TYPES),
        mean_value_method=mean_value_method or MEAN_VALUE_METHOD,
        workers=workers if workers is not None else WORKERS,
        dry_run=dry_run,
        strict=STRICT_INCOMPLETE_GROUPS if strict is None else strict,
        write_source=WRITE_SOURCE_RASTERS if write_source is None else write_source,
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
        description="Create TP means and maximums across durations. CLI options override wrapper defaults."
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument(
        "--mean-value-method",
        choices=("asc_to_asc", "closest_source", "arithmetic"),
        help=(
            "Mean value/source rule. asc_to_asc writes the arithmetic mean and selects the lowest source at or above "
            "it; closest_source writes the nearest contributing value; arithmetic writes the mean and selects the "
            "nearest source."
        ),
    )
    parser.add_argument("--workers", type=int, help="Parallel native raster processes.")
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
        mean_value_method=args.mean_value_method,
        workers=args.workers,
        dry_run=args.dry_run,
        strict=args.strict,
        write_source=args.write_source,
    )


if __name__ == "__main__":
    cli_options: CliOptions = _parse_cli_arguments()
    result: int = main(
        working_directory=cli_options.common.working_directory,
        mean_value_method=cli_options.mean_value_method,
        workers=cli_options.workers,
        dry_run=cli_options.dry_run,
        strict=cli_options.strict,
        write_source=cli_options.write_source,
        use_live_dashboard=cli_options.common.use_live_dashboard,
        live_refresh_per_second=cli_options.common.live_refresh_per_second,
        live_max_rows=cli_options.common.live_max_rows,
    )
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/raster_processing/run_asc_to_asc_mean_then_maximum.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not cli_options.common.no_pause:
        pause_console()
    raise SystemExit(result)
