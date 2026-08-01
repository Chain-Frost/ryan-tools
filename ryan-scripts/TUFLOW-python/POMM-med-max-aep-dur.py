# ryan-scripts\TUFLOW-python\POMM-med-max-aep-dur.py
r"""
Wrapper Script: POMM Median Peak Reports.

This script locates TUFLOW POMM/RLL_Qmx CSV outputs in the working directory and produces a
timestamped Excel workbook summarizing median peak values across AEPs and durations.
Users can edit the hard-coded constants in this file to control data inclusion and filtering logic,
or use command-line arguments to override these settings.

Outputs:
    - ``<timestamp>_med_peaks.xlsx`` saved in the working directory.

Examples:
    python POMM-med-max-aep-dur.py --no-pause
    python POMM-med-max-aep-dur.py --working-directory E:\TUFLOW\Results --locations PO_01 PO_02
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.2"

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
# Optional explicit folder roots to scan. If left empty, the wrapper scans WORKING_DIR recursively.
PATHS_TO_PROCESS: tuple[Path, ...] = ()
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")


import argparse

from loguru import logger

from ryan_library.orchestrators.tuflow.pomm_max_items import export_median_peak_report
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    working_directory: Path | None = None,
) -> int:
    """
    Main entry point for median peak reporting.

    This function resolves overrides and calls `export_median_peak_report`.
    It prioritizes CLI arguments over the hard-coded defaults in this file.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        include_data_types: Overrides the INCLUDE_DATA_TYPES constant.
        locations_to_include: Overrides the LOCATIONS_TO_INCLUDE constant.
        paths_to_process: Explicit folder roots to scan for result files.
        working_directory: Overrides the default WORKING_DIR.
    """

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_data_types: tuple[str, ...] = INCLUDE_DATA_TYPES if include_data_types is None else include_data_types
    effective_locations: tuple[str, ...] = (
        LOCATIONS_TO_INCLUDE if locations_to_include is None else locations_to_include
    )
    effective_paths: tuple[Path, ...] = PATHS_TO_PROCESS if paths_to_process is None else paths_to_process

    try:
        export_median_peak_report(
            script_directory=target_directory,
            paths_to_process=effective_paths or None,
            log_level=console_log_level or CONSOLE_LOG_LEVEL,
            include_pomm=INCLUDE_POMM,
            locations_to_include=effective_locations or None,
            include_data_types=effective_data_types or None,
        )
    except Exception:
        logger.exception("POMM median peak report failed.")
        return 1
    return 0


def _parse_cli_arguments() -> CommonWrapperOptions:
    """
    Parse command-line arguments to override script defaults.

    Returns:
        CommonWrapperOptions: Parsed and processed common arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Combine median POMM peak statistics into a timestamped Excel report "
            "(e.g., 20240131-1530_med_peaks.xlsx). "
            "Command-line options override the script defaults."
        ),
        epilog=r"""Example:
  python POMM-med-max-aep-dur.py --working-directory E:\TUFLOW\Results --locations PO_01 PO_02 --no-pause""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    result: int = main(
        console_log_level=common_options.console_log_level,
        include_data_types=common_options.data_types,
        locations_to_include=common_options.locations_to_include,
        working_directory=common_options.working_directory,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
