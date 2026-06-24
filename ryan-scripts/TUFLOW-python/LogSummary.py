# ryan-scripts\TUFLOW-python\LogSummary.py
"""
Wrapper Script: TUFLOW Log Summary.

This script acts as a mutable wrapper for `tuflow_logsummary.main_processing`.
It finds and parses TUFLOW log files in the directory tree to generate a simulation summary report.
Users can edit certain constants (like CONSOLE_LOG_LEVEL) to control the default behavior.
"""

import argparse
import gc
from pathlib import Path
import os
import subprocess

from ryan_library.scripts.tuflow.tuflow_logsummary import main_processing
from ryan_library.scripts.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25


def main(
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
    live_max_rows: int | None = None,
    live_refresh_per_second: float | None = None,
    use_live_dashboard: bool | None = None,
) -> None:
    """
    Main entry point for log summary analysis.

    This function sets up the environment and initiates the log processing logic.
    By default, it processes files in the script's directory recursively.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        working_directory: Overrides the default WORKING_DIR.
        live_max_rows: Overrides the LIVE_MAX_ROWS constant.
        live_refresh_per_second: Overrides the LIVE_REFRESH_PER_SECOND constant.
        use_live_dashboard: Overrides the USE_LIVE_DASHBOARD constant.
    """
    print_library_version()
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_live_max_rows: int = live_max_rows or LIVE_MAX_ROWS
    effective_live_refresh_per_second: float = live_refresh_per_second or LIVE_REFRESH_PER_SECOND
    effective_use_live_dashboard: bool = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard

    # Execute the main processing
    main_processing(
        console_log_level=effective_console_log_level,
        use_live_dashboard=effective_use_live_dashboard,
        live_refresh_per_second=effective_live_refresh_per_second,
        live_max_rows=effective_live_max_rows,
    )
    print()
    print_library_version()


def _parse_cli_arguments() -> CommonWrapperOptions:
    """
    Parse command-line arguments to override script defaults.

    Returns:
        CommonWrapperOptions: Parsed and processed common arguments.
    """
    parser = argparse.ArgumentParser(
        description="Summarise TUFLOW log files. Command-line options override the script defaults."
    )
    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


def _pause_on_windows() -> None:
    if os.name == "nt":
        subprocess.run(args=["cmd", "/C", "pause"], check=False)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    main(
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
        live_max_rows=common_options.live_max_rows,
        live_refresh_per_second=common_options.live_refresh_per_second,
        use_live_dashboard=common_options.use_live_dashboard,
    )
    gc.collect()
    _pause_on_windows()
