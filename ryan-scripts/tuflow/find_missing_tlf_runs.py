"""
Wrapper Script: Find Missing TUFLOW Runs.

This script acts as a mutable wrapper for `orchestrate_missing_runs_check`.
It analyzes a TUFLOW run tracking table to find missing AEP/Duration/TP sets.


I don't think this has been tested to see if it even works.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-09.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent

import argparse
from loguru import logger

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.orchestrators.tuflow.tlf_missing_runs import orchestrate_missing_runs_check


def main(
    input_table: Path,
    sheet: str | int = 0,
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
) -> int:
    """
    Main entry point for missing TUFLOW runs analysis.

    Args:
        input_table: Path to the tracking table (CSV or Excel).
        sheet: Sheet name or index if input is an Excel file.
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        working_directory: Overrides the default WORKING_DIR.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL

    configure_serial_logging(
        console_log_level=effective_console_log_level,
        log_file=str(input_table.parent / f"{input_table.stem}_missing_runs.log"),
    )

    try:
        orchestrate_missing_runs_check(input_table, sheet_name=sheet)
    except Exception:
        logger.exception("Missing runs analysis failed.")
        return 1

    return 0


def _parse_cli_arguments() -> tuple[argparse.Namespace, CommonWrapperOptions]:
    """
    Parse command-line arguments to override script defaults.

    Returns:
        tuple containing specific args and parsed common arguments.
    """
    parser = argparse.ArgumentParser(
        description="Analyze a TUFLOW run tracking table to find missing AEP/Duration/TP sets."
    )

    parser.add_argument(
        "-i",
        "--input-table",
        type=Path,
        required=True,
        help="Path to the tracking table (CSV or Excel).",
    )
    parser.add_argument(
        "-s",
        "--sheet",
        type=str,
        default="0",
        help="Sheet name or index if input is an Excel file (default: 0).",
    )

    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()

    # Convert sheet to int if it's a digit string
    sheet_val: str | int = args.sheet
    if isinstance(sheet_val, str) and sheet_val.isdigit():
        args.sheet = int(sheet_val)

    return args, parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    args, common_options = _parse_cli_arguments()
    result: int = main(
        input_table=args.input_table,
        sheet=args.sheet,
        console_log_level=common_options.console_log_level,
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
