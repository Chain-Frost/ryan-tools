# ryan-scripts\\TUFLOW-python\\po_and_timeseries\\calculate_po_closure_durations.py
"""Wrapper for computing closure durations from TUFLOW PO files.

Update ``paths`` or ``thresholds`` below to customise processing.
``data_type`` defaults to ``"Flow"`` and ``allowed_locations`` can
restrict which locations are loaded from the CSV files.
``EXPORT_MODE`` selects formatted reports; the legacy raw CSV and gzip-Parquet
tables are always included in the same protected output batch.
"""

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-08.1"
PAUSE_ON_FINISH = True

CONSOLE_LOG_LEVEL = "INFO"
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 10
LOCATIONS_TO_INCLUDE: list[str] = []
EXPORT_MODE: Literal["excel", "parquet", "both"] = "excel"
WORKING_DIR: Path = Path(__file__).absolute().parent
# Optional explicit folder roots to scan. If left empty, the wrapper scans WORKING_DIR recursively.
PATHS_TO_PROCESS: list[Path] = []
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")

# TODO. expand this to work on a variety of file formats and data types.
# Ultimately, we are just looking at exceedance duration of some form so should not be that hard.

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
from ryan_library.orchestrators.tuflow.closure_durations import run_closure_durations


def main(
    *,
    console_log_level: str | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    export_mode: Literal["excel", "parquet", "both"] | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    working_directory: Path | None = None,
    use_live_dashboard: bool | None = None,
    live_refresh_per_second: float | None = None,
    live_max_rows: int | None = None,
) -> int:
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py",
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
    effective_locations: tuple[str, ...] | None = tuple(
        LOCATIONS_TO_INCLUDE if locations_to_include is None else locations_to_include
    ) or None
    effective_export_mode: Literal["excel", "parquet", "both"] = export_mode or EXPORT_MODE
    try:
        outcome = run_closure_durations(
            paths=effective_paths_to_process,
            thresholds=None,
            data_type="Flow",
            allowed_locations=effective_locations,
            log_level=effective_console_log_level,
            export_mode=effective_export_mode,
            use_live_dashboard=USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard,
            live_refresh_per_second=(
                LIVE_REFRESH_PER_SECOND if live_refresh_per_second is None else live_refresh_per_second
            ),
            live_max_rows=LIVE_MAX_ROWS if live_max_rows is None else live_max_rows,
        )
    except KeyboardInterrupt:
        logger.warning("Closure-duration workflow interrupted.")
        return 130
    except Exception:  # noqa: BLE001 - CLI boundary must return nonzero for unexpected workflow failure
        logger.exception("Closure-duration workflow failed.")
        return 1
    return 1 if outcome is False else 0


def _parse_cli_arguments() -> tuple[CommonWrapperOptions, Literal["excel", "parquet", "both"] | None]:
    parser = argparse.ArgumentParser(
        description=(
            "Compute closure durations from PO files. Raw CSV and gzip-Parquet tables are always included; "
            "--export-mode selects formatted reports. Command-line options override the script defaults."
        )
    )
    add_common_cli_arguments(parser=parser)
    add_export_mode_cli_argument(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    common: CommonWrapperOptions = parse_common_cli_arguments(args=args)
    export_mode: Literal["excel", "parquet", "both"] | None = args.export_mode  # type: ignore[attr-defined]
    return common, export_mode


if __name__ == "__main__":
    common_options, cli_export_mode = _parse_cli_arguments()
    result: int = main(
        console_log_level=common_options.console_log_level,
        locations_to_include=common_options.locations_to_include,
        export_mode=cli_export_mode,
        working_directory=common_options.working_directory,
        use_live_dashboard=common_options.use_live_dashboard,
        live_refresh_per_second=common_options.live_refresh_per_second,
        live_max_rows=common_options.live_max_rows,
    )
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    should_pause = PAUSE_ON_FINISH if common_options.pause_on_finish is None else common_options.pause_on_finish
    if should_pause:
        pause_console()
    raise SystemExit(result)
