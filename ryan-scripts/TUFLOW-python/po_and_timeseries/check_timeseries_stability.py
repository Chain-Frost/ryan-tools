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

import os
from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-08.1"
PAUSE_ON_FINISH = True

CONSOLE_LOG_LEVEL = "INFO"
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 10
WORKING_DIR: Path = Path(__file__).absolute().parent
# Optional explicit folder roots to scan. If left empty, the wrapper scans WORKING_DIR recursively.
PATHS_TO_PROCESS: list[Path] = []
RESULT_TYPES: list[str] = ["PO", "Q"]

DATATYPE_INCLUDE: list[str] = ["Flow", "Q"]
DATATYPE_CASE_SENSITIVE: bool = False

LOCATION_INCLUDE: list[str] = []
LOCATION_EXCLUDE: list[str] = []
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

from loguru import logger

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
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
) -> int:
    """Run stability checks on PO CSVs using wrapper defaults and optional CLI overrides.

    Args:
        console_log_level: Overrides CONSOLE_LOG_LEVEL.
        include_data_types: Overrides DATATYPE_INCLUDE for series types such as "Flow" or "Q".
        result_types: Overrides RESULT_TYPES for file families: "PO", "Q", or "all".
        locations_to_include: Overrides LOCATION_INCLUDE.
        export_mode: Overrides EXPORT_MODE.
        paths_to_process: Explicit folder roots to scan for result files.
        working_directory: Overrides WORKING_DIR.
    """
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/po_and_timeseries/check_timeseries_stability.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )

    script_directory: Path = (
        Path(working_directory if working_directory is not None else WORKING_DIR).expanduser().absolute()
    )
    configured_paths = PATHS_TO_PROCESS if paths_to_process is None else paths_to_process
    effective_paths_to_process = [
        path.absolute() if path.is_absolute() else (script_directory / path).absolute()
        for configured_path in (configured_paths or (script_directory,))
        for path in (Path(configured_path).expanduser(),)
    ]
    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = CONSOLE_LOG_LEVEL if console_log_level is None else console_log_level
    effective_use_live_dashboard: bool = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard
    effective_live_refresh_per_second: float = (
        LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
    )
    effective_live_max_rows: int = LIVE_MAX_ROWS if live_max_rows is None else live_max_rows
    effective_data_types: tuple[str, ...] = tuple(DATATYPE_INCLUDE if include_data_types is None else include_data_types)
    effective_result_types: tuple[str, ...] = tuple(RESULT_TYPES if result_types is None else result_types)
    effective_locations: tuple[str, ...] = tuple(LOCATION_INCLUDE if locations_to_include is None else locations_to_include)
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE

    try:
        outcome = main_processing(
            paths_to_process=effective_paths_to_process,
            result_types=effective_result_types,
            datatype_include=effective_data_types,
            datatype_case_sensitive=DATATYPE_CASE_SENSITIVE,
            location_include=effective_locations,
            location_exclude=tuple(LOCATION_EXCLUDE),
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
            use_live_dashboard=effective_use_live_dashboard,
            live_refresh_per_second=effective_live_refresh_per_second,
            live_max_rows=effective_live_max_rows,
        )
    except Exception:
        logger.exception("TUFLOW timeseries stability wrapper failed.")
        return 1

    return 1 if outcome is False else 0


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
        use_live_dashboard=common_options.use_live_dashboard,
        live_refresh_per_second=common_options.live_refresh_per_second,
        live_max_rows=common_options.live_max_rows,
    )
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/po_and_timeseries/check_timeseries_stability.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    should_pause = PAUSE_ON_FINISH if common_options.pause_on_finish is None else common_options.pause_on_finish
    if should_pause:
        pause_console()
    raise SystemExit(result)
