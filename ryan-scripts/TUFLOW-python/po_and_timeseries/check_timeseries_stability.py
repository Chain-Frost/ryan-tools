"""Run configurable stability checks on TUFLOW PO and 1D Q time-series CSVs.

Review the editable configuration for search roots/glob, result and datatype
filters, locations, flatness/difference tolerances, rolling windows, output
paths, and dashboard settings. Run
``python check_timeseries_stability.py --help`` to select export mode or limit
result types at the command line.

The wrapper delegates processing to the shared TUFLOW stability orchestrator and
exports the configured reports. Validate tolerances against the model timestep
and expected signal scale before treating flagged periods as instability.
"""

from pathlib import Path
from typing import Literal
import os

WRAPPER_VERSION = "2026-08-02.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent
# Optional explicit folder roots to scan. If left empty, the wrapper scans WORKING_DIR recursively.
PATHS_TO_PROCESS: tuple[Path, ...] = ()
CSV_GLOB: str = "**/*_PO.csv"
RESULT_TYPES: tuple[str, ...] = ("PO", "Q")

DATATYPE_INCLUDE: tuple[str, ...] = ("Flow", "Q")
DATATYPE_CASE_SENSITIVE: bool = False

LOCATION_INCLUDE: tuple[str, ...] = ()
LOCATION_EXCLUDE: tuple[str, ...] = ()
LOCATION_CASE_SENSITIVE: bool = False

FLAT_TOL: float = 1e-6
DIFF_REL_TOL: float = 0.01
DIFF_ABS_TOL: float = 1e-6
MAX_SIGN_CHANGES: int = 2
MIN_POINTS: int = 5

CPU_COUNT: int | None = os.cpu_count()
MAX_WORKERS: int | None = max(CPU_COUNT - 1, 1) if CPU_COUNT is not None else None
CHUNKSIZE: int = 1

EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"

import argparse

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    add_export_mode_cli_argument,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import main_processing


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    result_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    export_mode: Literal["excel", "parquet", "both"] | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    working_directory: Path | None = None,
) -> int:
    """
    Run stability checks on PO CSVs using wrapper defaults and optional CLI overrides.

    Args:
        console_log_level: Overrides CONSOLE_LOG_LEVEL.
        include_data_types: Overrides DATATYPE_INCLUDE for series types such as "Flow" or "Q".
        result_types: Overrides RESULT_TYPES for file families: "PO", "Q", or "all".
        locations_to_include: Overrides LOCATION_INCLUDE.
        export_mode: Overrides EXPORT_MODE.
        paths_to_process: Explicit folder roots to scan for result files.
        working_directory: Overrides WORKING_DIR.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    script_directory: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_data_types: tuple[str, ...] = include_data_types or DATATYPE_INCLUDE
    effective_result_types: tuple[str, ...] = result_types or RESULT_TYPES
    effective_locations: tuple[str, ...] | tuple[()] = (
        locations_to_include if locations_to_include else (LOCATION_INCLUDE or ())
    )
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE
    effective_paths_to_process: list[Path] = list(paths_to_process or PATHS_TO_PROCESS or (script_directory,))

    main_processing(
        paths_to_process=effective_paths_to_process,
        csv_glob=CSV_GLOB,
        result_types=effective_result_types,
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

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check PO and 1D Q timeseries stability. "
            "Command-line options override the hard-coded values near the top of this file."
        )
    )
    add_common_cli_arguments(parser=parser)
    add_export_mode_cli_argument(parser=parser)
    parser.add_argument(
        "--result-types",
        nargs="+",
        metavar="TYPE",
        help="Result file families to process: PO, Q, or all. Defaults to the script value.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    common_options: CommonWrapperOptions = parse_common_cli_arguments(args=args)
    result: int = main(
        console_log_level=common_options.console_log_level,
        include_data_types=common_options.data_types,
        result_types=tuple(args.result_types) if args.result_types else None,
        locations_to_include=common_options.locations_to_include,
        export_mode=args.export_mode,
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
