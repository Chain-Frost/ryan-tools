# ryan-scripts\TUFLOW-python\POMM-med-max-aep-dur.py
from pathlib import Path

CONSOLE_LOG_LEVEL = "INFO"  # or "DEBUG"
# Toggle to include the combined POMM sheet in the Excel export.
INCLUDE_POMM: bool = False
# Update this tuple to restrict processing to specific PO/Location values.
# Leave empty to include every location found in the POMM files.
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()
# Toggle which data types are included; defaults cover POMM peaks and RLL_Qmx.
INCLUDE_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
# Change the working directory
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")


import argparse
import gc
import os

from ryan_library.scripts.pomm_max_items import export_median_peak_report
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
    """Wrapper script for peak reporting."""

    print_library_version()
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_data_types: tuple[str, ...] | None = include_data_types or INCLUDE_DATA_TYPES or None
    effective_locations: tuple[str, ...] | None = (
        locations_to_include if locations_to_include else (LOCATIONS_TO_INCLUDE or None)
    )

    export_median_peak_report(
        script_directory=script_directory,
        log_level=effective_console_log_level,
        include_pomm=INCLUDE_POMM,
        locations_to_include=effective_locations,
        include_data_types=list(effective_data_types) if effective_data_types else None,
    )
    print()
    print_library_version()


def _parse_cli_arguments() -> CommonWrapperOptions:
    parser = argparse.ArgumentParser(
        description="Combine median POMM peak statistics. Command-line options override the script defaults."
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
