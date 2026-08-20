"""
Compresses files matching a specific pattern into individual .7z archives using
the external 7-Zip executable. Uses multiprocessing to process multiple files
concurrently.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

import argparse
import multiprocessing
from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-20.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
DEFAULT_EXTENSION = "*"
DEFAULT_EXCLUDE_FOLDERS: list[str] = ["xf"]
DEFAULT_EXCLUDE_EXTENSIONS: list[str] = [".xf4", ".xmdf", ".2dm", ".dat", ".7z"]
# ==============================================================================

from loguru import logger

from ryan_library.functions.archive_utils import create_7zip_archive, find_7zip
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner

SEVEN_ZIP_EXE: Path | None = find_7zip()


def _compress_file(file_path: Path) -> tuple[Path, str, str]:
    """Worker function to compress a single file using 7z.exe. Returns (Path, status, msg)."""
    if not SEVEN_ZIP_EXE:
        return file_path, "ERROR", "7-Zip executable not found on system."

    output_archive: Path = file_path.with_suffix(f"{file_path.suffix}.7z")
    try:
        if output_archive.exists():
            return file_path, "SKIPPED", f"Output {output_archive.name} already exists"

        create_7zip_archive(
            executable=SEVEN_ZIP_EXE,
            output_archive=output_archive,
            inputs=[file_path],
        )
        return file_path, "SUCCESS", ""
    except Exception as e:
        return file_path, "ERROR", str(e)


def main(
    *,
    input_paths: PathOrList | None = None,
    exclude_folders: list[str] | None = None,
    exclude_extensions: list[str] | None = None,
) -> int:
    if input_paths is None:
        targets: list[Path] = [Path(DEFAULT_INPUT).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_paths)]

    if targets:
        first_target: Path = targets[0]
        working_dir: Path = first_target.parent if first_target.is_file() else first_target
        if not change_working_directory(target_dir=working_dir):
            return 1

    all_files_to_compress: list[Path] = []

    _ex_folders: list[str] = exclude_folders if exclude_folders is not None else DEFAULT_EXCLUDE_FOLDERS
    _ex_extensions: list[str] = exclude_extensions if exclude_extensions is not None else DEFAULT_EXCLUDE_EXTENSIONS

    active_exclude_folders: list[str] = [f.lower() for f in _ex_folders]
    active_exclude_extensions: list[str] = [e.lower() for e in _ex_extensions]

    for target in targets:
        if target.is_file():
            all_files_to_compress.append(target)
        elif target.is_dir():
            for f in target.rglob(pattern=DEFAULT_EXTENSION):
                if not f.is_file():
                    continue
                # Skip if any part of the path matches an excluded folder
                if any(part.lower() in active_exclude_folders for part in f.parts):
                    continue
                # Skip if the file extension matches an excluded extension
                if f.suffix.lower() in active_exclude_extensions:
                    continue
                all_files_to_compress.append(f)
        else:
            logger.warning("Target {} does not exist", target)

    # Deduplicate paths
    all_files_to_compress = list(dict.fromkeys(all_files_to_compress))

    if not all_files_to_compress:
        logger.warning("No files found matching '{}' across targets.", DEFAULT_EXTENSION)
        return 0

    if not SEVEN_ZIP_EXE:
        logger.error("7-Zip executable not found in PATH or Program Files. Cannot proceed.")
        return 1

    logger.info("Found {} files to compress. Starting multiprocessing pool...", len(all_files_to_compress))

    total_success = 0
    total_skipped = 0
    total_errors = 0

    workers = min(multiprocessing.cpu_count(), len(all_files_to_compress))
    with multiprocessing.Pool(processes=workers) as pool:
        for file_path, status, msg in pool.imap_unordered(_compress_file, all_files_to_compress):
            if status == "SUCCESS":
                logger.debug("Compressed {}", file_path.name)
                total_success += 1
            elif status == "SKIPPED":
                logger.debug("Skipped {}: {}", file_path.name, msg)
                total_skipped += 1
            else:
                logger.error("Failed to compress {}: {}", file_path.name, msg)
                total_errors += 1

    logger.success(
        "Finished: {} compressed, {} skipped, {} errors.",
        total_success,
        total_skipped,
        total_errors,
    )
    return 1 if total_errors > 0 else 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compresses files into individual .7z archives using 7-Zip.")
    parser.add_argument(
        "-i",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories or specific files.",
    )
    parser.add_argument(
        "--exclude_folders",
        type=str,
        nargs="+",
        default=None,
        help="List of folder names to exclude from the search (e.g. xf).",
    )
    parser.add_argument(
        "--exclude_extensions",
        type=str,
        nargs="+",
        default=None,
        help="List of file extensions to exclude (e.g. .xf4 .xmdf).",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="archive_compress.log", file_log_level="DEBUG"):
        result: int = main(
            input_paths=args.input_paths,
            exclude_folders=args.exclude_folders,
            exclude_extensions=args.exclude_extensions,
        )

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
