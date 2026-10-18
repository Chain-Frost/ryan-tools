"""Inventory files with document numbers and revisions; export CSV and Excel.

Editable defaults can be overridden with CLI arguments. SHA-256 is optional.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-19.1"
DEFAULT_INPUT_DIRECTORIES: list[Path] = [Path(".")]
DEFAULT_PATTERNS: list[str] = ["*"]
DEFAULT_OUTPUT_DIRECTORY = Path(".")
DEFAULT_OUTPUT_BASENAME = "file_inventory"
DEFAULT_RECURSIVE = True
DEFAULT_INCLUDE_SHA256 = False
DEFAULT_CREATE_EXCEL = True
DEFAULT_PAUSE = True

import argparse
import sys
from uuid import uuid4

import pandas as pd
from loguru import logger

from ryan_library.functions.excel_export import ExcelExporter
from ryan_library.functions.file_inventory import INVENTORY_COLUMNS, collect_file_inventory
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner


def main(args: argparse.Namespace) -> int:
    """Export reports and return a nonzero code for metadata errors."""
    roots: list[Path] = [Path(item).absolute() for item in (args.input_directories or DEFAULT_INPUT_DIRECTORIES)]
    patterns: list[str] = args.patterns if args.patterns is not None else DEFAULT_PATTERNS
    output_dir = Path(args.output_directory or DEFAULT_OUTPUT_DIRECTORY).absolute()
    basename: str = args.basename or DEFAULT_OUTPUT_BASENAME
    recursive: bool = DEFAULT_RECURSIVE if args.recursive is None else args.recursive
    sha256: bool = DEFAULT_INCLUDE_SHA256 if args.sha256 is None else args.sha256
    excel: bool = DEFAULT_CREATE_EXCEL if args.excel is None else args.excel
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    if not basename or Path(basename).name != basename or basename in {".", ".."}:
        logger.error("Output basename must be a simple filename stem.")
        return 2
    try:
        rows = collect_file_inventory(roots, patterns=patterns, recursive=recursive, include_sha256=sha256)
        output_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame.from_records(rows, columns=list(INVENTORY_COLUMNS))
        csv_path = output_dir / f"{basename}.csv"
        temp_path = output_dir / f".{basename}.{uuid4().hex}.tmp"
        try:
            frame.to_csv(temp_path, index=False)
            temp_path.replace(csv_path)
        finally:
            temp_path.unlink(missing_ok=True)
        if excel:
            ExcelExporter().save_to_excel(
                data_frame=frame,
                sheet_name="Inventory",
                output_directory=output_dir,
                file_name=f"{basename}.xlsx",
                auto_adjust_width=False,
            )
        errors = sum(bool(row["error"]) for row in rows)
        logger.info("Inventory complete: {} file(s), {} error(s), reports in {}", len(rows), errors, output_dir)
        return 1 if errors else 0
    except (OSError, ValueError, RuntimeError):
        logger.exception("File inventory failed")
        return 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="File inventory with document/revision parsing")
    parser.add_argument("input_directories", nargs="*", type=Path)
    parser.add_argument("--pattern", dest="patterns", action="append")
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--basename")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--sha256", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--excel", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    arguments = _parse_cli_arguments()
    status = main(arguments)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if DEFAULT_PAUSE and not arguments.no_pause:
        pause_console()
    sys.exit(status)
