# ryan-scripts\TUFLOW-python\po_and_timeseries\combine_po_results.py
"""Wrapper Script: Combine TUFLOW PO Results.

This script acts as a mutable wrapper for `ryan_library.orchestrators.tuflow.po_combine`.
It allows users to hard-code default configurations for merging TUFLOW "PO" (Plot Output) CSV files,
while still providing command-line overrides for automation/batch processing.

Key features:
- Merges PO files from found subdirectories.
- Configurable defaults (constants) for manual editing.
- CLI arguments for runtime flexibility.
"""

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-01.1"

# None chooses SUCCESS with a dashboard, INFO without; an explicit level overrides it.
CONSOLE_LOG_LEVEL: str | None = None
INCLUDE_DATA_TYPES: tuple[str, ...] = ("PO",)
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()
WORKING_DIR: Path = Path(__file__).absolute().parent
# Relative input/output paths are interpreted relative to WORKING_DIR.
PATHS_TO_PROCESS: tuple[Path, ...] = ()
EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25
PAUSE_ON_FINISH = True
LOG_FILE: Path | None = None  # Optional detailed logfile, relative to WORKING_DIR.
FILE_LOG_LEVEL = "DEBUG"

import argparse
import sys

from ryan_library.functions.live_dashboard import validate_dashboard_settings
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    add_export_mode_cli_argument,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.po_combine import main_processing

def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    working_directory: Path | None = None,
    live_max_rows: int | None = None,
    live_refresh_per_second: float | None = None,
    use_live_dashboard: bool | None = None,
    log_file: Path | None = None,
    file_log_level: str | None = None,
    export_mode: Literal["excel", "parquet", "both"] | None = None,
) -> int:
    """Run the installed library workflow with effective editable and CLI options."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    script_directory = (working_directory if working_directory is not None else WORKING_DIR).expanduser().absolute()
    selected_roots = paths_to_process or PATHS_TO_PROCESS or (script_directory,)
    effective_paths = [
        (item if item.is_absolute() else script_directory / item).expanduser().absolute()
        for item in selected_roots
    ]
    selected_locations = locations_to_include if locations_to_include is not None else LOCATIONS_TO_INCLUDE
    effective_locations = selected_locations or None
    effective_data_types = include_data_types or INCLUDE_DATA_TYPES
    effective_dashboard = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard
    effective_refresh = LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
    effective_rows = LIVE_MAX_ROWS if live_max_rows is None else live_max_rows
    effective_console_level = CONSOLE_LOG_LEVEL if console_log_level is None else console_log_level
    effective_file_level = FILE_LOG_LEVEL if file_log_level is None else file_log_level
    configured_log_file = LOG_FILE if log_file is None else log_file
    effective_log_file = None if configured_log_file is None else (
        (configured_log_file if configured_log_file.is_absolute() else script_directory / configured_log_file)
        .expanduser().absolute()
    )
    validate_dashboard_settings(refresh_per_second=effective_refresh, max_rows=effective_rows)
    if not change_working_directory(target_dir=script_directory):
        return 1

    try:
        succeeded = main_processing(
            paths_to_process=effective_paths,
            include_data_types=list(effective_data_types),
            console_log_level=effective_console_level,
            locations_to_include=effective_locations,
            export_mode=EXPORT_MODE if export_mode is None else export_mode,
            use_live_dashboard=effective_dashboard,
            live_refresh_per_second=effective_refresh,
            live_max_rows=effective_rows,
            log_file=str(effective_log_file) if effective_log_file is not None else None,
            file_log_level=effective_file_level,
        )
    except KeyboardInterrupt:
        print("Combine TUFLOW PO results interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - wrapper converts workflow failures to an exit code
        print(f"Combine TUFLOW PO results failed: {exc}", file=sys.stderr)
        return 1
    return 1 if succeeded is False else 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine TUFLOW PO results. Command-line arguments override editable wrapper settings."
    )
    add_common_cli_arguments(parser=parser)
    add_export_mode_cli_argument(parser=parser)
    parser.add_argument("--paths-to-process", nargs="+", type=Path, metavar="PATH",
                        help="Input directory roots relative to working directory.")
    parser.add_argument("--log-file", type=Path, help="Optional detailed log file.")
    parser.add_argument("--file-log-level", help="File sink severity, e.g. DEBUG.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    options: CommonWrapperOptions = parse_common_cli_arguments(args=args)
    result = main(
        console_log_level=options.console_log_level,
        include_data_types=options.data_types,
        locations_to_include=options.locations_to_include,
        paths_to_process=tuple(args.paths_to_process) if args.paths_to_process is not None else None,
        working_directory=options.working_directory,
        live_max_rows=options.live_max_rows,
        live_refresh_per_second=options.live_refresh_per_second,
        use_live_dashboard=options.use_live_dashboard,
        log_file=options.log_file,
        file_log_level=options.file_log_level,
        export_mode=args.export_mode,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True,
    )
    should_pause = PAUSE_ON_FINISH if options.pause_on_finish is None else options.pause_on_finish
    if should_pause:
        pause_console()
    raise SystemExit(result)
