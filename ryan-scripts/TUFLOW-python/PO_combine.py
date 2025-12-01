# ryan-scripts\TUFLOW-python\PO_combine.py
import argparse
import gc
import os
from pathlib import Path
from typing import Literal

from ryan_library.scripts.tuflow.po_combine import main_processing
from ryan_library.scripts.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

CONSOLE_LOG_LEVEL = "INFO"  # or "DEBUG"

# Update this tuple to restrict processing to specific PO/Location values.
# Leave empty to include every location found in the PO files.
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()
# Choose output format: "excel", "parquet", or "both".
EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"

# Change the working directory
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03")


def main(
    *,
    console_log_level: str | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    export_mode: Literal["excel", "parquet", "both"] | None = None,
    working_directory: Path | None = None,
) -> None:
    """Wrapper script to merge PO results.
    By default, it processes files in the script's directory."""
    print_library_version()

    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_locations: tuple[str, ...] | None = (
        locations_to_include if locations_to_include else (LOCATIONS_TO_INCLUDE or None)
    )
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE

    main_processing(
        paths_to_process=[script_directory],
        include_data_types=["PO"],
        console_log_level=effective_console_log_level,
        locations_to_include=effective_locations,
        export_mode=effective_export_mode,
    )
    print()
    print_library_version()


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Combine TUFLOW PO outputs. "
            "Command-line options override the hard-coded values near the top of this file."
        )
    )
    add_common_cli_arguments(parser)
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
        locations_to_include=common_options.locations_to_include,
        export_mode=args.export_mode,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
