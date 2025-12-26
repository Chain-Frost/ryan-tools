# ryan-scripts/TUFLOW-python/TUFLOW_Timeseries_Stability.py
"""
Wrapper Script: Timeseries stability checks for TUFLOW PO CSVs.

Currently supports PO CSVs; Q support can be added in the library script later.
"""

from pathlib import Path
from typing import Literal
import argparse
import gc
import os

from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import main_processing
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent
CSV_GLOB: str = "**/*_PO.csv"

DATATYPE_INCLUDE: tuple[str, ...] = ("Flow",)
DATATYPE_CASE_SENSITIVE: bool = False

LOCATION_INCLUDE: tuple[str, ...] = ()
LOCATION_EXCLUDE: tuple[str, ...] = ()
LOCATION_CASE_SENSITIVE: bool = False

FLAT_TOL: float = 1e-6
DIFF_REL_TOL: float = 0.01
DIFF_ABS_TOL: float = 1e-6
MAX_SIGN_CHANGES: int = 2
MIN_POINTS: int = 5

MAX_WORKERS: int | None = (max(int(os.cpu_count()) - 1, 1)) if os.cpu_count() is not None else None
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
    Run stability checks on PO CSVs using wrapper defaults and optional CLI overrides.

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
        flat_tol=FLAT_TOL,
        diff_rel_tol=DIFF_REL_TOL,
        diff_abs_tol=DIFF_ABS_TOL,
        max_sign_changes=MAX_SIGN_CHANGES,
        min_points=MIN_POINTS,
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
            "Check PO timeseries stability. "
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
