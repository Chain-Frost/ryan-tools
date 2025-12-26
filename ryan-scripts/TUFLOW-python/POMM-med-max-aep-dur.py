# ryan-scripts\TUFLOW-python\POMM-med-max-aep-dur.py
"""
Wrapper Script: POMM Median Peak Reports.

This script locates TUFLOW POMM/RLL_Qmx CSV outputs in the working directory and produces a
timestamped Excel workbook summarizing median peak values across AEPs and durations.
Users can edit the hard-coded constants in this file to control data inclusion and filtering logic,
or use command-line arguments to override these settings.

Outputs:
    - ``<timestamp>_med_peaks.xlsx`` saved in the working directory.
"""

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

from ryan_library.orchestrators.tuflow.pomm_max_items import export_median_peak_report
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    PommPeakWrapperDefaults,
    add_common_cli_arguments,
    parse_common_cli_arguments,
    run_pomm_peak_report_wrapper,
)


DEFAULTS: PommPeakWrapperDefaults = PommPeakWrapperDefaults(
    console_log_level=CONSOLE_LOG_LEVEL,
    include_pomm=INCLUDE_POMM,
    include_data_types=INCLUDE_DATA_TYPES,
    locations_to_include=LOCATIONS_TO_INCLUDE,
    working_directory=WORKING_DIR,
)


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    working_directory: Path | None = None,
) -> None:
    """
    Main entry point for median peak reporting.

    This function resolves overrides and calls `export_median_peak_report`.
    It prioritizes CLI arguments over the hard-coded defaults in this file.

    Args:
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        include_data_types: Overrides the INCLUDE_DATA_TYPES constant.
        locations_to_include: Overrides the LOCATIONS_TO_INCLUDE constant.
        working_directory: Overrides the default WORKING_DIR.
    """

    overrides = CommonWrapperOptions(
        console_log_level=console_log_level,
        data_types=include_data_types,
        locations_to_include=locations_to_include,
        working_directory=working_directory,
    )
    run_pomm_peak_report_wrapper(
        exporter=export_median_peak_report,
        defaults=DEFAULTS,
        overrides=overrides,
    )


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
        )
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
    if os.name == "nt":
        os.system("PAUSE")
