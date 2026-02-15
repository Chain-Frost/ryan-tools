# ryan-scripts\TUFLOW-python\TUFLOW_Timeseries_Peaks_Check.py
"""
Wrapper Script: Peak checks for TUFLOW PO CSV files.

This wrapper exposes hard-coded defaults for quick edits while delegating the heavy
lifting to ``ryan_library.orchestrators.tuflow.peak_check_po_csvs``.
"""

from pathlib import Path
from typing import Literal
import argparse
import gc
import os

from ryan_library.orchestrators.tuflow.peak_check_po_csvs import main_processing
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

# TODO: make it more like the other wrappers, more CLI options.

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent
CSV_GLOB: str = "**/*_PO.csv"

# Datatype filter (include-list). Default: only 'Flow'.
DATATYPE_INCLUDE: tuple[str, ...] = ("Flow",)
DATATYPE_CASE_SENSITIVE: bool = False

# Location filter (exact include-list). Empty => no include filter.
LOCATION_INCLUDE: tuple[str, ...] = ()  # e.g. ("PO-009", "PO-010", "PO-011")
LOCATION_EXCLUDE: tuple[str, ...] = ()
LOCATION_CASE_SENSITIVE: bool = False

# Warning thresholds (hours from end)
WARN_2HOURS: float = 2.0
WARN_1HOUR: float = 1.0

# Treat deviations <= this as "flat"
FLAT_TOL: float = 1e-6

# Multiprocessing
n: int | None = os.cpu_count()
MAX_WORKERS: int | None = (max(n - 1, 1)) if n is not None else None
CHUNKSIZE: int = 1

EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    export_mode: Literal["excel", "parquet", "both"] | None = None,
    working_directory: Path | None = None,
) -> None:
    """
    Run peak checks on PO CSVs using wrapper defaults and optional CLI overrides.

    Args:
        console_log_level: Overrides CONSOLE_LOG_LEVEL.
        include_data_types: Overrides DATATYPE_INCLUDE.
        locations_to_include: Overrides LOCATION_INCLUDE.
        export_mode: Overrides EXPORT_MODE.
        working_directory: Overrides WORKING_DIR.
    """
    print_library_version()

    script_directory: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_data_types: tuple[str, ...] = include_data_types or DATATYPE_INCLUDE
    effective_locations: tuple[str, ...] | tuple[()] = (
        locations_to_include if locations_to_include else (LOCATION_INCLUDE or ())
    )
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE

    main_processing(
        paths_to_process=[script_directory],
        csv_glob=CSV_GLOB,
        datatype_include=effective_data_types,
        datatype_case_sensitive=DATATYPE_CASE_SENSITIVE,
        location_include=effective_locations,
        location_exclude=LOCATION_EXCLUDE,
        location_case_sensitive=LOCATION_CASE_SENSITIVE,
        warn_2hours=WARN_2HOURS,
        warn_1hour=WARN_1HOUR,
        flat_tol=FLAT_TOL,
        max_workers=MAX_WORKERS,
        chunksize=CHUNKSIZE,
        console_log_level=effective_console_log_level,
        export_mode=effective_export_mode,
    )

    print()
    print_library_version()


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check PO timeseries peak timing. "
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
