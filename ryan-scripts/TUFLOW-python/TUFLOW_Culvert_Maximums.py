# ryan-scripts\TUFLOW-python\TUFLOW_Culvert_Maximums.py
"""
Wrapper Script: Merge TUFLOW Culvert Maximums.

This script acts as a mutable wrapper for `tuflow_culverts_merge.main_processing`.
It combines culvert maximums (and raw data) from multiple CSV files.
Users can edit the hard-coded constants in this file to control include data types and export format,
or use command-line arguments to override these settings.
"""

from pathlib import Path
from typing import Literal

CONSOLE_LOG_LEVEL = "INFO"  # or "DEBUG"
INCLUDE_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")
EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")

import argparse
import gc
import os

from ryan_library.scripts.tuflow.tuflow_culverts_merge import main_processing
from ryan_library.scripts.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    working_directory: Path | None = None,
) -> None:
    """
    Main entry point to merge culvert maximums; double-clickable.

    This function sets up the environment and calls 'main_processing'.
    It allows users to default settings via constants while retaining CLI capabilities.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        include_data_types: Overrides the INCLUDE_DATA_TYPES constant.
        locations_to_include: Overrides the locations filter.
        working_directory: Overrides the default WORKING_DIR.
    """
    print_library_version()

    script_dir: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=script_dir):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_data_types: list[str] = list(include_data_types or INCLUDE_DATA_TYPES)
    effective_export_mode: Literal["excel", "parquet", "both"] = EXPORT_MODE

    main_processing(
        paths_to_process=[script_dir],
        include_data_types=effective_data_types,
        console_log_level=effective_console_log_level,
        locations_to_include=locations_to_include,
        export_mode=effective_export_mode,
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
        description="Merge TUFLOW culvert maximums. Command-line options override the script defaults."
    )
    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    main(
        console_log_level=common_options.console_log_level,
        include_data_types=common_options.data_types,
        locations_to_include=common_options.locations_to_include,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
