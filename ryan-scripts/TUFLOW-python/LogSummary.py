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

from ryan_library.orchestrators.tuflow.tuflow_logsummary import main_processing
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")


def main(
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
) -> None:
    """
    Main entry point for log summary analysis.

    This function sets up the environment and initiates the log processing logic.
    By default, it processes files in the script's directory recursively.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        working_directory: Overrides the default WORKING_DIR.
    """
    print_library_version()
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL

    # Execute the main processing
    main_processing(console_log_level=effective_console_log_level)
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


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    main(
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
