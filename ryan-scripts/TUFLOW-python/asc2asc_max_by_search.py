"""Mutable wrapper for configurable ASC_to_ASC raster maximum searches.

Edit the constants below or use command-line arguments to override runtime settings.
The input glob determines what each job aggregates; it need not vary by duration.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.1"

ASC_TO_ASC_EXE = Path(r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe")
WORKING_DIR: Path = Path(__file__).absolute().parent

# Add, remove, or rename axes freely. One maximum job is created for every combination.
TEMPLATE_AXES: dict[str, tuple[str, ...]] = {
    "scenario": ("EXG", "DEV"),
    "event": ("PMP",),
    "result_type": ("d_HR_Max", "h_HR_Max", "V_Max"),
}
INPUT_GLOB_TEMPLATE = "{scenario}/{event}/grids/*_{result_type}.tif"
OUTPUT_FILENAME_TEMPLATE = "site_05_mineModel_{scenario}_{event}_Max_08M_{result_type}.tif"
WORKERS: int | None = None
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
from ryan_library.orchestrators.tuflow.asc2asc_max_by_search import MaxSearch, build_max_searches, run_max_workflow


@dataclass(slots=True, frozen=True)
class CliOptions:
    """Wrapper-specific CLI values alongside shared wrapper options."""

    common: CommonWrapperOptions
    workers: int | None
    dry_run: bool


def main(
    *,
    working_directory: Path | None = None,
    workers: int | None = None,
    dry_run: bool = False,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
) -> int:
    """Resolve wrapper settings and run the maximum-search orchestrator."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    search_root: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=search_root):
        return 1

    searches: tuple[MaxSearch, ...] = build_max_searches(
        input_glob_template=INPUT_GLOB_TEMPLATE,
        output_filename_template=OUTPUT_FILENAME_TEMPLATE,
        template_axes=TEMPLATE_AXES,
    )
    exit_code: int = run_max_workflow(
        executable=ASC_TO_ASC_EXE,
        search_root=search_root,
        searches=searches,
        workers=workers if workers is not None else WORKERS,
        dry_run=dry_run,
        use_live_dashboard=USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard,
        live_refresh_per_second=(
            LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
        ),
        live_max_rows=LIVE_MAX_ROWS if live_max_rows is None else live_max_rows,
    )
    return exit_code


def _parse_cli_arguments() -> CliOptions:
    parser = argparse.ArgumentParser(
        description="Create maximum rasters from configurable searches. CLI options override wrapper defaults."
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument("--workers", type=int, help="Parallel ASC_to_ASC processes.")
    parser.add_argument("--dry-run", action="store_true", help="Report resolved jobs without creating rasters.")
    args: argparse.Namespace = parser.parse_args()
    return CliOptions(
        common=parse_common_cli_arguments(args=args),
        workers=args.workers,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    cli_options: CliOptions = _parse_cli_arguments()
    result: int = main(
        working_directory=cli_options.common.working_directory,
        workers=cli_options.workers,
        dry_run=cli_options.dry_run,
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
