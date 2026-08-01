"""Build temporal-pattern means and duration maxima with ASC_to_ASC.

Edit the wrapper constants for the executable, working directory, input glob,
expected temporal patterns, scenarios, result types, output folder, and dashboard
settings. Start with
``python asc2asc_mean_then_max_by_search.py --dry-run`` to inspect discovered
groups without creating rasters; use ``--help`` for worker and strictness options.

The workflow completes all per-duration means before calculating maxima across
durations. Input globs may contain wildcards, but output naming templates must
render concrete filenames and must not rediscover generated outputs.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.1"

ASC_TO_ASC_EXE = Path(r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe")
WORKING_DIR: Path = Path(__file__).absolute().parent
OUTPUT_DIRECTORY_NAME = "ensemble_statistics"
INPUT_GLOB = "*.tif"
EXPECTED_TPS: frozenset[int] = frozenset(range(1, 11))
SCENARIOS = ("EXG", "DEV")
RESULT_TYPES = ("d_HR_Max", "h_HR_Max", "V_Max")
WORKERS: int | None = None
STRICT_INCOMPLETE_GROUPS = False
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25

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
from ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search import run_mean_then_max_workflow


@dataclass(slots=True, frozen=True)
class CliOptions:
    common: CommonWrapperOptions
    workers: int | None
    dry_run: bool
    strict: bool | None


def main(
    *,
    working_directory: Path | None = None,
    workers: int | None = None,
    dry_run: bool = False,
    strict: bool | None = None,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
) -> int:
    """Resolve wrapper settings and run the mean-then-maximum orchestrator."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    search_root: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=search_root):
        return 1

    exit_code: int = run_mean_then_max_workflow(
        executable=ASC_TO_ASC_EXE,
        search_root=search_root,
        output_root=search_root / OUTPUT_DIRECTORY_NAME,
        input_glob=INPUT_GLOB,
        expected_tps=EXPECTED_TPS,
        scenarios=SCENARIOS,
        result_types=RESULT_TYPES,
        workers=workers if workers is not None else WORKERS,
        dry_run=dry_run,
        strict=STRICT_INCOMPLETE_GROUPS if strict is None else strict,
        use_live_dashboard=USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard,
        live_refresh_per_second=(
            LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
        ),
        live_max_rows=LIVE_MAX_ROWS if live_max_rows is None else live_max_rows,
    )
    return exit_code


def _parse_cli_arguments() -> CliOptions:
    parser = argparse.ArgumentParser(
        description="Create TP means and maximums across durations. CLI options override wrapper defaults."
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument("--workers", type=int, help="Parallel ASC_to_ASC processes.")
    parser.add_argument("--dry-run", action="store_true", help="Validate groups without creating rasters.")
    strict_group = parser.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", default=None)
    strict_group.add_argument("--no-strict", dest="strict", action="store_false")
    args: argparse.Namespace = parser.parse_args()
    return CliOptions(
        common=parse_common_cli_arguments(args=args),
        workers=args.workers,
        dry_run=args.dry_run,
        strict=args.strict,
    )


if __name__ == "__main__":
    cli_options: CliOptions = _parse_cli_arguments()
    result: int = main(
        working_directory=cli_options.common.working_directory,
        workers=cli_options.workers,
        dry_run=cli_options.dry_run,
        strict=cli_options.strict,
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
