# ryan-scripts\TUFLOW-python\LogSummary.py

import argparse
import gc
from pathlib import Path
import os

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


def main(
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
) -> None:
    """Wrapper script to analyse and summarise TUFLOW log files.
    By default, it processes files in the script's directory recursively."""
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
