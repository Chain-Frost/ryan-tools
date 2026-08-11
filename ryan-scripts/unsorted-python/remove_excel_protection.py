"""
Removes worksheet and workbook protection from Excel files (.xlsx, .xlsm) by
parsing and modifying the internal XML within the ZIP archive. Does not require
the password and is safer than rewriting through pandas/openpyxl, preserving
macros and styling.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR: PathLike = Path(".")
DEFAULT_OUTPUT_DIR: PathLike = Path(r".\unprotected")
# ==============================================================================

import argparse
import os
import shutil
import zipfile

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list, PathLike
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def del_xml_element(file_path: Path, del_string: str, separator: str) -> None:
    """Removes XML elements containing a specific string by parsing text."""
    with open(file_path, "r", encoding="utf-8") as xf:
        rl = xf.readlines()

    splitxf = [a.split(separator) for a in rl]
    for components in splitxf:
        for i in range(len(components) - 1, -1, -1):
            if del_string in components[i]:
                components.pop(i)

    fixedxf = [separator.join(a) for a in splitxf]

    with open(file_path, "w", encoding="utf-8") as xf:
        for lines in fixedxf:
            xf.write(lines)


def process_excel_file(file_path: Path, output_dir: Path) -> bool:
    """Unzips the file, removes protection, and re-zips it."""
    temp_folder = output_dir / f"temp_{file_path.stem}"

    if temp_folder.exists():
        shutil.rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            zf.extractall(path=temp_folder)
            wslist = [f for f in zf.namelist() if "xl/worksheets/" in f and "_rels" not in f]

        wb_loc = temp_folder / "xl" / "workbook.xml"
        if wb_loc.exists():
            del_xml_element(wb_loc, "workbookProtection", "<")

        for ws in wslist:
            ws_path = temp_folder / ws
            if ws_path.exists():
                del_xml_element(ws_path, "sheetProtection", "<")

        output_file = output_dir / f"{file_path.stem}_unprotected{file_path.suffix}"

        with zipfile.ZipFile(output_file, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=3) as archive:
            for r, _d, ff in os.walk(temp_folder):
                for file in ff:
                    file_full_path = Path(r) / file
                    arcname = file_full_path.relative_to(temp_folder)
                    archive.write(file_full_path, arcname=arcname)

        logger.debug("Successfully processed: {}", output_file.name)
        return True
    except Exception:
        logger.exception("Failed to process Excel file {}", file_path.name)
        return False
    finally:
        if temp_folder.exists():
            shutil.rmtree(temp_folder)


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_directories)]

    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    total_success = 0
    total_files = 0

    for target_directory in targets:
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        if not output_dir.is_absolute():
            output_dir = target_directory / output_dir

        output_dir.mkdir(parents=True, exist_ok=True)

        excel_files: list[Path] = []
        excel_files.extend(target_directory.glob("*.xlsx"))
        excel_files.extend(target_directory.glob("*.xlsm"))

        if not excel_files:
            logger.warning("No .xlsx or .xlsm files found in {}", target_directory)
            continue

        total_files += len(excel_files)

        logger.info("Found {} Excel files to unprotect in {}.", len(excel_files), target_directory.name)

        for f in excel_files:
            if f.parent == output_dir:
                continue
            if process_excel_file(f, output_dir):
                total_success += 1

    logger.success("Unprotected {}/{} Excel files total.", total_success, total_files)
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Removes protection from Excel files (.xlsx, .xlsm).")
    parser.add_argument(
        "-i",
        "--input_directories",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories containing Excel files.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="remove_excel_protection.log", file_log_level="DEBUG"):
        result = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
