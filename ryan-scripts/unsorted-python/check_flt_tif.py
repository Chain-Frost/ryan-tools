"""
Finds primary raster files (e.g., .flt) that are missing their secondary counterpart
(e.g., .tif) or where the primary file is newer than the secondary file.

Outputs a text file listing all missing/older secondary files. Useful for QA
during raster format migrations.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path(".")
DEFAULT_OUTPUT_FILE = Path("missing_tifs.txt")
DEFAULT_PRIMARY_EXT = ".flt"
DEFAULT_SECONDARY_EXT = ".tif"
# ==============================================================================

import argparse
import os
from multiprocessing import Pool, cpu_count

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def parallel_walk(directory: Path) -> list[Path]:
    """Returns a list of all directories under the given root directory."""
    all_paths: list[Path] = []
    for root, _dirs, _files in os.walk(directory):
        all_paths.append(Path(root))
    logger.debug("Completed walking in {}", directory)
    return all_paths


def check_files_in_directory(args: tuple[Path, str, str]) -> list[str]:
    """Checks a single directory for primary extensions missing secondary extensions."""
    directory, primary_ext, secondary_ext = args
    missing_or_older_files: list[str] = []

    primary_files: set[str] = {f for f in os.listdir(directory) if f.lower().endswith(primary_ext.lower())}
    secondary_files: dict[str, str] = {
        f.lower(): f for f in os.listdir(directory) if f.lower().endswith(secondary_ext.lower())
    }

    for primary_file in primary_files:
        secondary_file: str = os.path.splitext(primary_file)[0] + secondary_ext

        primary_path: Path = directory / primary_file
        matching_secondary: str | None = secondary_files.get(secondary_file.lower())

        if matching_secondary is None:
            missing_or_older_files.append(str(primary_path))
        elif primary_path.stat().st_mtime > (directory / matching_secondary).stat().st_mtime:
            # The primary file is newer than the secondary file (out of date)
            missing_or_older_files.append(str(primary_path))

    return missing_or_older_files


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets: list[Path] = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_directories)]

    # We just change to the first directory to satisfy the standard, but we'll scan all of them.
    targets = list(dict.fromkeys(targets))
    if any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    logger.info(
        "Scanning {} root directories for {} files missing a {} counterpart...",
        len(targets),
        DEFAULT_PRIMARY_EXT,
        DEFAULT_SECONDARY_EXT,
    )

    # Collect all top-level directories to distribute among processes
    top_level_directories: list[Path] = []
    for target_directory in targets:
        top_level_directories.extend(
            [target_directory / name for name in os.listdir(target_directory) if (target_directory / name).is_dir()]
        )

    logger.info("Collected {} top-level directories.", len(top_level_directories))

    # Walk directories
    if top_level_directories:
        with Pool(processes=min(cpu_count(), len(top_level_directories))) as pool:
            all_directories: list[list[Path]] = pool.map(parallel_walk, top_level_directories)
    else:
        all_directories = []

    # Flatten the list of directories, including the root directories themselves
    directories: list[Path] = list(dict.fromkeys(targets + [path for sublist in all_directories for path in sublist]))
    logger.info("Completed walking. {} total directories to process.", len(directories))

    with Pool(processes=min(cpu_count(), len(directories))) as pool:
        results: list[list[str]] = pool.map(
            func=check_files_in_directory,
            iterable=[(d, DEFAULT_PRIMARY_EXT, DEFAULT_SECONDARY_EXT) for d in directories],
        )

    all_missing_files: list[str] = [item for sublist in results for item in sublist]

    output_file: Path = Path(DEFAULT_OUTPUT_FILE).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file=output_file, mode="w") as file:
            for item in all_missing_files:
                file.write(f"{item}\n")
        logger.success("Output written to {} with {} entries.", output_file.name, len(all_missing_files))
    except OSError:
        logger.exception("Error writing to file {}", output_file.name)
        return 1

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finds primary raster files missing a secondary counterpart.")
    parser.add_argument(
        "-i", "--input_directories", type=Path, nargs="+", default=None, help="Root directories to scan."
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="check_flt_tif.log", file_log_level="DEBUG"):
        result: int = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
