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
import shutil
import tempfile
import zipfile

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathLike, PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def del_xml_element(file_path: Path, del_string: str, separator: str) -> None:
    """Removes XML elements containing a specific string by parsing text."""
    with open(file=file_path, mode="r", encoding="utf-8") as xf:
        rl: list[str] = xf.readlines()

    splitxf: list[list[str]] = [a.split(separator) for a in rl]
    for components in splitxf:
        for i in range(len(components) - 1, -1, -1):
            if del_string in components[i]:
                components.pop(i)

    fixedxf: list[str] = [separator.join(a) for a in splitxf]

    with open(file=file_path, mode="w", encoding="utf-8") as xf:
        for lines in fixedxf:
            xf.write(lines)


def process_excel_file(file_path: Path, output_dir: Path) -> bool:
    """Unzips the file, removes protection, and re-zips it."""
    temp_folder = Path(tempfile.mkdtemp(prefix=f"{file_path.stem}_", dir=output_dir))
    output_file: Path = output_dir / f"{file_path.stem}_unprotected{file_path.suffix}"
    temporary_output: Path = output_file.with_suffix(suffix=f"{output_file.suffix}.tmp")

    try:
        with zipfile.ZipFile(file=file_path, mode="r") as zf:
            zf.extractall(path=temp_folder)
            wslist: list[str] = [f for f in zf.namelist() if "xl/worksheets/" in f and "_rels" not in f]

        wb_loc: Path = temp_folder / "xl" / "workbook.xml"
        if wb_loc.exists():
            del_xml_element(file_path=wb_loc, del_string="workbookProtection", separator="<")

        for ws in wslist:
            ws_path: Path = temp_folder / ws
            if ws_path.exists():
                del_xml_element(file_path=ws_path, del_string="sheetProtection", separator="<")

        temporary_output.unlink(missing_ok=True)
        with zipfile.ZipFile(
            file=temporary_output, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=3
        ) as archive:
            for file_full_path in temp_folder.rglob("*"):
                if file_full_path.is_file():
                    archive.write(filename=file_full_path, arcname=file_full_path.relative_to(other=temp_folder))

        with zipfile.ZipFile(file=temporary_output, mode="r") as archive:
            if archive.testzip() is not None:
                raise zipfile.BadZipFile("Generated workbook failed its ZIP integrity check")

        temporary_output.replace(target=output_file)

        logger.debug("Successfully processed: {}", output_file.name)
        return True
    except Exception:
        logger.exception("Failed to process Excel file {}", file_path.name)
        return False
    finally:
        temporary_output.unlink(missing_ok=True)
        shutil.rmtree(temp_folder, ignore_errors=True)


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets: list[Path] = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(paths=input_directories)]

    targets = list(dict.fromkeys(targets))
    if any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    total_success = 0
    total_files = 0

    for target_directory in targets:
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        if not output_dir.is_absolute():
            output_dir: Path = target_directory / output_dir

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
    return 0 if total_success == total_files else 1


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
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="remove_excel_protection.log", file_log_level="DEBUG"):
        result: int = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
