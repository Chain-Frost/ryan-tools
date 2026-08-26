# ryan-scripts\TUFLOW-python\log_processing\append_log_summary_to_master_workbook.py
"""
Wrapper Script: Append TUFLOW Log Summary rows to an existing master workbook.

This wrapper searches the configured results folder for TUFLOW log files, skips
rows already present in the master workbook table, and appends only new rows.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(
    r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO" r"\TUFLOW_MLGD\results\v05"
)
MASTER_WORKBOOK: Path = WORKING_DIR / "Master_modelling_log.xlsx"
SHEET_NAME = "Log Summary"
TABLE_NAME = "Table1"
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25

import argparse

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.tuflow_logsummary_append import append_to_master_log_summary


def main(
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
    master_workbook: Path | None = None,
    sheet_name: str | None = None,
    table_name: str | None = None,
    live_max_rows: int | None = None,
    live_refresh_per_second: float | None = None,
    use_live_dashboard: bool | None = None,
) -> int:
    """Append new TUFLOW log-summary rows into the configured master workbook."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_master_workbook: Path = master_workbook or MASTER_WORKBOOK
    effective_sheet_name: str = sheet_name or SHEET_NAME
    effective_table_name: str | None = table_name or TABLE_NAME
    effective_live_max_rows: int = live_max_rows or LIVE_MAX_ROWS
    effective_live_refresh_per_second: float = live_refresh_per_second or LIVE_REFRESH_PER_SECOND
    effective_use_live_dashboard: bool = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard

    append_to_master_log_summary(
        master_workbook_path=effective_master_workbook,
        console_log_level=effective_console_log_level,
        sheet_name=effective_sheet_name,
        table_name=effective_table_name,
        use_live_dashboard=effective_use_live_dashboard,
        live_refresh_per_second=effective_live_refresh_per_second,
        live_max_rows=effective_live_max_rows,
    )
    return 0


def _parse_cli_arguments() -> tuple[CommonWrapperOptions, Path | None, str | None, str | None]:
    """Parse command-line arguments to override script defaults."""
    parser = argparse.ArgumentParser(description="Append new TUFLOW log-summary rows to an existing master workbook.")
    add_common_cli_arguments(parser=parser)
    parser.add_argument(
        "--master-workbook",
        type=Path,
        help="Existing .xlsx workbook to append to. Defaults to MASTER_WORKBOOK in this wrapper.",
    )
    parser.add_argument(
        "--sheet-name",
        help="Worksheet containing the raw log-summary table. Defaults to SHEET_NAME in this wrapper.",
    )
    parser.add_argument(
        "--table-name",
        help="Excel table to extend. Defaults to TABLE_NAME in this wrapper.",
    )
    args: argparse.Namespace = parser.parse_args()
    return (
        parse_common_cli_arguments(args=args),
        args.master_workbook,
        args.sheet_name,
        args.table_name,
    )


if __name__ == "__main__":
    common_options, cli_master_workbook, cli_sheet_name, cli_table_name = _parse_cli_arguments()
    result: int = main(
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
        master_workbook=cli_master_workbook,
        sheet_name=cli_sheet_name,
        table_name=cli_table_name,
        live_max_rows=common_options.live_max_rows,
        live_refresh_per_second=common_options.live_refresh_per_second,
        use_live_dashboard=common_options.use_live_dashboard,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
