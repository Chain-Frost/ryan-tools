# ryan-scripts\TUFLOW-python\log_processing\create_log_summary_report.py
"""Wrapper Script: TUFLOW Log Summary.

This script acts as a mutable wrapper for `tuflow_logsummary.main_processing`.
It finds and parses TUFLOW log files in the directory tree to generate a simulation summary report.
Users can edit certain constants (like CONSOLE_LOG_LEVEL) to control the default behavior.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-01.1"

# None chooses SUCCESS with the dashboard and INFO without it; an explicit level always wins.
CONSOLE_LOG_LEVEL: str | None = None
LOG_FILE: Path | None = None  # Optional detailed log; relative paths use WORKING_DIR.
FILE_LOG_LEVEL = "DEBUG"
PAUSE_ON_FINISH = True
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25

import argparse
import sys

from ryan_library.functions.live_dashboard import validate_dashboard_settings
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.tuflow_logsummary import main_processing


def main(
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
    live_max_rows: int | None = None,
    live_refresh_per_second: float | None = None,
    use_live_dashboard: bool | None = None,
    log_file: Path | None = None,
    file_log_level: str | None = None,
) -> int:
    """Main entry point for log summary analysis.

    This function sets up the environment and initiates the log processing logic.
    By default, it processes files in the script's directory recursively.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        working_directory: Overrides the default WORKING_DIR.
        live_max_rows: Overrides the LIVE_MAX_ROWS constant.
        live_refresh_per_second: Overrides the LIVE_REFRESH_PER_SECOND constant.
        use_live_dashboard: Overrides the USE_LIVE_DASHBOARD constant.
        log_file: Optional detailed log path; None uses LOG_FILE.
        file_log_level: DEBUG file sink level override.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    script_directory: Path = (working_directory if working_directory is not None else WORKING_DIR).absolute()
    effective_console_log_level = console_log_level if console_log_level is not None else CONSOLE_LOG_LEVEL
    effective_live_max_rows = live_max_rows if live_max_rows is not None else LIVE_MAX_ROWS
    effective_live_refresh = live_refresh_per_second if live_refresh_per_second is not None else LIVE_REFRESH_PER_SECOND
    effective_dashboard = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard
    configured_log_file = log_file if log_file is not None else LOG_FILE
    configured_file_level = file_log_level if file_log_level is not None else FILE_LOG_LEVEL
    try:
        validate_dashboard_settings(refresh_per_second=effective_live_refresh, max_rows=effective_live_max_rows)
    except ValueError as exc:
        print(f"Invalid dashboard configuration: {exc}", file=sys.stderr)
        return 1

    # Capture relative bases before changing directory; do not dereference
    # mapped-drive or UNC paths merely to display them.
    log_path: Path | None = None
    if configured_log_file is not None:
        log_path = (
            configured_log_file if configured_log_file.is_absolute() else script_directory / configured_log_file
        ).absolute()

    if not change_working_directory(target_dir=script_directory):
        return 1

    try:
        succeeded = main_processing(
            console_log_level=effective_console_log_level,
            use_live_dashboard=effective_dashboard,
            live_refresh_per_second=effective_live_refresh,
            live_max_rows=effective_live_max_rows,
            log_file=str(log_path) if log_path is not None else None,
            file_log_level=configured_file_level,
        )
    except KeyboardInterrupt:
        print("Log summary interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - process boundary converts unexpected workflow failures to exit 1
        print(f"Log summary failed: {exc}", file=sys.stderr)
        return 1
    return 1 if succeeded is False else 0


def _parse_cli_arguments() -> CommonWrapperOptions:
    """Parse command-line arguments to override script defaults.

    Returns:
        CommonWrapperOptions: Parsed and processed common arguments.
    """
    parser = argparse.ArgumentParser(
        description="Summarise TUFLOW log files. Command-line options override the script defaults."
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument("--log-file", type=Path, help="Optional detailed log file (relative to working directory).")
    parser.add_argument("--file-log-level", help="Minimum file log level, e.g. DEBUG.")
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    result: int = main(
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
        live_max_rows=common_options.live_max_rows,
        live_refresh_per_second=common_options.live_refresh_per_second,
        use_live_dashboard=common_options.use_live_dashboard,
        log_file=common_options.log_file,
        file_log_level=common_options.file_log_level,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    pause_at_end = PAUSE_ON_FINISH if common_options.pause_on_finish is None else common_options.pause_on_finish
    if pause_at_end:
        pause_console()
    raise SystemExit(result)
