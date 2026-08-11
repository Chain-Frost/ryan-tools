"""
Lists all files in a directory recursively and saves their paths to a text file.
Provides the same functionality as `dir /s /b /o:en > filenames.txt`, but allows
running from a different permitted location (a common requirement in restricted
corporate environments).
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path(".")
DEFAULT_OUTPUT_FILE = Path("filenames.txt")
# ==============================================================================

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_directories)]

    # We just change to the first directory to satisfy the standard, but we'll scan all of them.
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    output_file = Path(DEFAULT_OUTPUT_FILE).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    all_files: list[Path] = []
    
    for target_dir in targets:
        logger.info("Scanning {} recursively...", target_dir.name)
        # rglob("*") gets all files and directories, we filter for is_file()
        all_files.extend(f for f in target_dir.rglob("*") if f.is_file())

    if not all_files:
        logger.warning("No files found across {} input directories.", len(targets))
        return 0

    # Sort files by extension then by name to mimic `dir /o:en`
    all_files.sort(key=lambda f: (f.suffix.lower(), f.stem.lower()))

    try:
        with open(output_file, "w", encoding="utf-8") as f_out:
            for file_path in all_files:
                f_out.write(f"{file_path}\n")
        logger.success("Wrote {} file paths to {}", len(all_files), output_file.name)
    except OSError:
        logger.exception("Failed to write to {}", output_file.name)
        return 1

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recursively lists files and saves their paths to a text file.")
    parser.add_argument(
        "-i", "--input_directories", type=Path, nargs="+", default=None, help="Root directories to scan."
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="list_files_recursive.log", file_log_level="DEBUG"):
        result = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
