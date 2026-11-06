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

Source rasters and legends are enabled by default, with final source IDs
linked directly to the original temporal-pattern rasters. Set
``WRITE_SOURCE_RASTERS = False`` or use ``--no-source`` to disable them.

For ``d_HR_Max`` and ``V_Max``, NoData contributes zero to both statistics.
For elevation results ``h_Max`` and ``h_HR_Max``, NoData is excluded and an
output cell remains NoData only when every contributing cell is NoData.

Incomplete TP bins normally withhold their affected final maximum. Set
``ALLOW_INCOMPLETE_TP_BINS = True`` or use ``--allow-incomplete-tp-bins`` to
proceed using available non-empty rasters, with missing-TP warnings. Missing
files are not zero-valued contributors. This override cannot be combined with
``STRICT_INCOMPLETE_GROUPS`` / ``--strict``.
"""

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-06.1"

WORKING_DIR: Path = Path(__file__).absolute().parent
OUTPUT_DIRECTORY_NAME = "ensemble_statistics"
INPUT_GLOB = "*.tif"
EXPECTED_TPS: frozenset[int] = frozenset(range(1, 11))
SCENARIOS = ("EXG", "DEV")
RESULT_TYPES = ("d_HR_Max", "h_HR_Max", "V_Max")
MEAN_VALUE_METHOD: Literal["asc_to_asc", "closest_source", "arithmetic"] = "asc_to_asc"
WORKERS: int | None = None
STRICT_INCOMPLETE_GROUPS = False
# Opt in to statistics from fewer TPs; outputs will not represent the full ensemble.
ALLOW_INCOMPLETE_TP_BINS = False
WRITE_SOURCE_RASTERS = True
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25
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
    allow_incomplete_tp_bins: bool | None
    write_source: bool | None


def main(
    *,
    working_directory: Path | None = None,
    mean_value_method: MeanValueMethod | None = None,
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool | None = None,
    allow_incomplete_tp_bins: bool | None = None,
    write_source: bool | None = None,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
    live_use_alternate_screen: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the mean-then-maximum orchestrator."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    search_root: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=search_root):
        return 1

    exit_code: int = run_stat_then_max_workflow(
        first_stage_statistic="mean",
        search_root=search_root,
        output_root=search_root / OUTPUT_DIRECTORY_NAME,
        input_glob=INPUT_GLOB,
        expected_tps=EXPECTED_TPS,
        scenarios=SCENARIOS,
        result_types=RESULT_TYPES,
        mean_value_method=mean_value_method or MEAN_VALUE_METHOD,
        workers=workers if workers is not None else WORKERS,
        dry_run=dry_run,
        strict=STRICT_INCOMPLETE_GROUPS if strict is None else strict,
        allow_incomplete_tp_bins=(
            ALLOW_INCOMPLETE_TP_BINS if allow_incomplete_tp_bins is None else allow_incomplete_tp_bins
        ),
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
    parser.add_argument(
        "--allow-incomplete-tp-bins",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Use available non-empty TPs in incomplete bins and create duration maxima with warnings "
            "(default off; incompatible with --strict)."
        ),
    )
    parser.add_argument(
        "--source",
        dest="write_source",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Write source rasters and CSV legends linked to original inputs (enabled by default).",
    )
    args: argparse.Namespace = parser.parse_args()
    return CliOptions(
        common=parse_common_cli_arguments(args=args),
        mean_value_method=args.mean_value_method,
        workers=args.workers,
        dry_run=args.dry_run,
        strict=args.strict,
        allow_incomplete_tp_bins=args.allow_incomplete_tp_bins,
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
        allow_incomplete_tp_bins=cli_options.allow_incomplete_tp_bins,
        write_source=cli_options.write_source,
        use_live_dashboard=cli_options.common.use_live_dashboard,
        live_refresh_per_second=cli_options.common.live_refresh_per_second,
        live_max_rows=cli_options.common.live_max_rows,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not cli_options.common.no_pause:
        pause_console()
    raise SystemExit(result)
