# ryan-scripts\TUFLOW-python\POMM_combine.py
"""
Wrapper Script: Combine TUFLOW POMM Results.

This script acts as a mutable wrapper for `ryan_library.scripts.tuflow.pomm_combine`.
It manages the combination of "POMM" (Plot Output Maximums/Minimums) data, primarily used for culvert peak analysis.
Users can edit hard-coded defaults in this file or use CLI arguments to control the execution.

Key features:
- Merges POMM and RLL_Qmx files (and others as specified).
- Configurable defaults (constants) for manual editing.
- CLI arguments for runtime flexibility.
"""

from pathlib import Path
from typing import Literal

CONSOLE_LOG_LEVEL = "INFO"  # or "DEBUG"
# Update this tuple to restrict processing to specific PO/Location values.
# Leave empty to include every location found in the POMM files.
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()
# Choose which data types to include; defaults combine POMM peaks and RLL_Qmx.
INCLUDE_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
# Choose output format: "excel", "parquet", or "both".
EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"
# Change the working directory
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03")

import argparse
import gc
import os

from ryan_library.scripts.tuflow.pomm_combine import main_processing
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
    export_mode: Literal["excel", "parquet", "both"] | None = None,
    working_directory: Path | None = None,
) -> None:
    """
    Main entry point for combining POMM results.

    This function initializes the environment and calls `main_processing` to merge data.
    It determines the effective configuration by checking for CLI argument overrides first,
    then falling back to the hard-coded constants defined in this script.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        include_data_types: Overrides the INCLUDE_DATA_TYPES constant.
        locations_to_include: Overrides the LOCATIONS_TO_INCLUDE constant.
        export_mode: Overrides the EXPORT_MODE constant.
        working_directory: Overrides the default WORKING_DIR.
    """
    print_library_version()

    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_data_types: list[str] = list(include_data_types or INCLUDE_DATA_TYPES)
    effective_locations: tuple[str, ...] | None = (
        locations_to_include if locations_to_include else (LOCATIONS_TO_INCLUDE or None)
    )
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE

    main_processing(
        paths_to_process=[script_directory],
        include_data_types=effective_data_types,
        console_log_level=effective_console_log_level,
        locations_to_include=effective_locations,
        export_mode=effective_export_mode,
    )
    print()
    print_library_version()


def _parse_cli_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments to override script defaults.

    Returns:
        argparse.Namespace: Filtered command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Combine TUFLOW POMM outputs. "
            "Command-line options override the hard-coded values near the top of this file."
        )
    )
    add_common_cli_arguments(parser=parser)
    parser.add_argument(
        "--export-mode",
        choices=("excel", "parquet", "both"),
        help="Select export format. Defaults to the script value.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    common_options: CommonWrapperOptions = parse_common_cli_arguments(args=args)
    main(
        console_log_level=common_options.console_log_level,
        include_data_types=common_options.data_types,
        locations_to_include=common_options.locations_to_include,
        export_mode=args.export_mode,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
