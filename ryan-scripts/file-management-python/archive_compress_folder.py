"""
Compresses entire directories into single .7z archives using the external 7-Zip
executable. Respects exclusion lists directly using 7-Zip's native -xr! flags.
Uses multiprocessing to process multiple folders concurrently.
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
DEFAULT_EXCLUDE_FOLDERS: list[str] = ["xf"]
DEFAULT_EXCLUDE_EXTENSIONS: list[str] = [".xf4", ".xmdf", ".2dm", ".dat", ".7z"]
# ==============================================================================

from loguru import logger

from ryan_library.functions.archive_utils import create_7zip_archive, find_7zip
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner

SEVEN_ZIP_EXE: Path | None = find_7zip()


def _compress_folder(args: tuple[Path, list[str], list[str]]) -> tuple[Path, str, str]:
    """Worker function to compress an entire folder using 7z.exe."""
    folder_path, ex_folders, ex_extensions = args

    if not SEVEN_ZIP_EXE:
        return folder_path, "ERROR", "7-Zip executable not found on system."

    output_archive: Path = folder_path.with_suffix(suffix=".7z")
    try:
        if output_archive.exists():
            return folder_path, "SKIPPED", f"Output {output_archive.name} already exists"

        exclusions: list[str] = []
        for folder in ex_folders:
            exclusions.append(f"-xr!{folder}")
        for ext in ex_extensions:
            ext_str: str = ext if ext.startswith(".") else f".{ext}"
            exclusions.append(f"-xr!*{ext_str}")
        create_7zip_archive(
            executable=SEVEN_ZIP_EXE,
            output_archive=output_archive,
            inputs=[f"{folder_path}\\*"],
            exclusions=exclusions,
        )
        return folder_path, "SUCCESS", ""
    except Exception as e:
        return folder_path, "ERROR", str(e)


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

    all_folders_to_compress: list[Path] = []

    _ex_folders: list[str] = exclude_folders if exclude_folders is not None else DEFAULT_EXCLUDE_FOLDERS
    _ex_extensions: list[str] = exclude_extensions if exclude_extensions is not None else DEFAULT_EXCLUDE_EXTENSIONS

    active_exclude_folders: list[str] = [f.lower() for f in _ex_folders]
    active_exclude_extensions: list[str] = [e.lower() for e in _ex_extensions]

    for target in targets:
        if target.is_dir():
            all_folders_to_compress.append(target)
        else:
            logger.warning("Target {} is not a directory or does not exist", target)

    # Deduplicate paths
    all_folders_to_compress = list(dict.fromkeys(all_folders_to_compress))

    if not all_folders_to_compress:
        logger.warning("No valid folders found to compress.")
        return 0

    if not SEVEN_ZIP_EXE:
        logger.error("7-Zip executable not found in PATH or Program Files. Cannot proceed.")
        return 1

    logger.info("Found {} folders to compress. Starting multiprocessing pool...", len(all_folders_to_compress))

    total_success = 0
    total_skipped = 0
    total_errors = 0

    # Pack the args for multiprocessing
    pool_args: list[tuple[Path, list[str], list[str]]] = [
        (f, active_exclude_folders, active_exclude_extensions) for f in all_folders_to_compress
    ]

    workers: int = min(multiprocessing.cpu_count(), len(pool_args))
    with multiprocessing.Pool(processes=workers) as pool:
        for folder_path, status, msg in pool.imap_unordered(_compress_folder, pool_args):
            if status == "SUCCESS":
                logger.debug("Compressed folder {}", folder_path.name)
                total_success += 1
            elif status == "SKIPPED":
                logger.debug("Skipped {}: {}", folder_path.name, msg)
                total_skipped += 1
            else:
                logger.error("Failed to compress {}: {}", folder_path.name, msg)
                total_errors += 1

    logger.success(
        "Finished: {} compressed, {} skipped, {} errors.",
        total_success,
        total_skipped,
        total_errors,
    )
    return 1 if total_errors > 0 else 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compresses entire folders into single .7z archives using 7-Zip.")
    parser.add_argument(
        "-i",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories to compress.",
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

    with setup_logger(console_log_level="SUCCESS", log_file="archive_compress_folder.log", file_log_level="DEBUG"):
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
