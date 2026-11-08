# ryan-scripts\\TUFLOW-python\\log_processing\\append_log_summary_to_master_workbook.py
"""Append completed TUFLOW log-summary rows to an explicitly selected master workbook.

Edit MASTER_WORKBOOK or pass --master-workbook. This operation intentionally
modifies an existing .xlsx file; use a backup for important workbooks.
"""

import sys
from pathlib import Path

WRAPPER_VERSION = "2026-11-08.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path | None = None  # None: use the current directory.
MASTER_WORKBOOK: Path | None = None  # Required: choose an existing .xlsx workbook.
SHEET_NAME = "Log Summary"
TABLE_NAME: str | None = None  # None: use the first table, or the used sheet range.
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 10
PAUSE_ON_FINISH = True

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
    print_wrapper_banner(
        wrapper_source="TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    effective_master_workbook = master_workbook if master_workbook is not None else MASTER_WORKBOOK
    if effective_master_workbook is None:
        print("ERROR: Select MASTER_WORKBOOK or provide --master-workbook <existing.xlsx>.", file=sys.stderr)
        return 1

    script_directory = working_directory if working_directory is not None else WORKING_DIR
    if script_directory is not None and not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level = console_log_level if console_log_level is not None else CONSOLE_LOG_LEVEL
    effective_sheet_name = sheet_name if sheet_name is not None else SHEET_NAME
    effective_table_name = table_name if table_name is not None else TABLE_NAME
    effective_live_max_rows = live_max_rows if live_max_rows is not None else LIVE_MAX_ROWS
    effective_live_refresh_per_second = (
        live_refresh_per_second if live_refresh_per_second is not None else LIVE_REFRESH_PER_SECOND
    )
    effective_use_live_dashboard = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard

    try:
        completed = append_to_master_log_summary(
            master_workbook_path=effective_master_workbook,
            console_log_level=effective_console_log_level,
            sheet_name=effective_sheet_name,
            table_name=effective_table_name,
            use_live_dashboard=effective_use_live_dashboard,
            live_refresh_per_second=effective_live_refresh_per_second,
            live_max_rows=effective_live_max_rows,
        )
    except KeyboardInterrupt:
        print("ERROR: Master log append interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - process boundary must produce a nonzero exit on any failure
        print(f"ERROR: Master log append failed: {exc}", file=sys.stderr)
        return 1
    return 0 if completed else 1


def _parse_cli_arguments() -> tuple[CommonWrapperOptions, Path | None, str | None, str | None]:
    """Parse command-line arguments to override script defaults."""
    parser = argparse.ArgumentParser(description="Append new TUFLOW log-summary rows to an existing master workbook.")
    add_common_cli_arguments(parser=parser)
    parser.add_argument(
        "--master-workbook",
        type=Path,
        help="Existing .xlsx workbook to modify. Required unless MASTER_WORKBOOK is configured.",
    )
    parser.add_argument(
        "--sheet-name",
        help="Worksheet containing the raw log-summary table. Defaults to SHEET_NAME in this wrapper.",
    )
    parser.add_argument(
        "--table-name",
        help="Excel table to extend. None selects the first table or used worksheet range.",
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
        wrapper_source="TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if PAUSE_ON_FINISH and not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
