"""Compress matching files into one .7z archive per file.

The external 7-Zip executable is required. Multiple files are processed
concurrently. Archives can optionally be split into upload-sized volumes; this
is disabled by default. Existing archives are skipped and source files are not
modified. Use ``--dry-run`` to preview outputs without invoking 7-Zip.

Windows example:
    python archive_compress_individual.py -i "Q:\\Project\\Results" --max-part-size-gib 5 --no-pause
"""

from __future__ import annotations

import argparse
import multiprocessing
from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-11-01.2"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path()
DEFAULT_EXTENSION = "*"
DEFAULT_EXCLUDE_FOLDERS: list[str] = ["xf"]
DEFAULT_EXCLUDE_EXTENSIONS: list[str] = [".xf4", ".xmdf", ".2dm", ".dat", ".7z"]
DEFAULT_7ZIP_EXE: Path | None = None  # None = auto-discover 7z.exe
DEFAULT_MAX_WORKERS: int | None = None  # None = CPU count, limited to file count
DEFAULT_MAX_PART_SIZE_GIB: int | None = None  # None = one unsplit .7z; use 5 for upload-sized parts
DEFAULT_DRY_RUN = False
DEFAULT_CONSOLE_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE: Path | None = Path("archive_compress_individual.log")
DEFAULT_FILE_LOG_LEVEL = "DEBUG"
DEFAULT_PAUSE_ON_FINISH = True
# ==============================================================================

from loguru import logger

from ryan_library.functions.archive_utils import create_7zip_archive, find_7zip
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def _compress_file(job: tuple[Path, Path, int | None]) -> tuple[Path, str, str]:
    """Worker function to compress a single file using 7z.exe. Returns (Path, status, msg)."""
    file_path, seven_zip_exe, max_part_size_gib = job

    output_archive: Path = file_path.with_suffix(f"{file_path.suffix}.7z")
    try:
        outputs = create_7zip_archive(
            executable=seven_zip_exe,
            output_archive=output_archive,
            inputs=[file_path],
            max_part_size_gib=max_part_size_gib,
        )
        detail = f"Created {outputs[0].name}" if len(outputs) == 1 else f"Created {len(outputs)} parts"
        return file_path, "SUCCESS", detail
    except FileExistsError as exc:
        return file_path, "SKIPPED", str(exc)
    except Exception as e:
        return file_path, "ERROR", str(e)


def _is_7zip_volume(path: Path) -> bool:
    """Return whether a file is a numbered part of a split .7z archive."""
    name = path.name.casefold()
    marker = name.rfind(".7z.")
    return marker >= 0 and name[marker + 4 :].isdigit()


def main(
    *,
    input_paths: PathOrList | None = None,
    exclude_folders: list[str] | None = None,
    exclude_extensions: list[str] | None = None,
    seven_zip_exe: Path | None = None,
    force_auto_discovery: bool = False,
    max_workers: int | None = None,
    max_part_size_gib: int | None = None,
    dry_run: bool | None = None,
) -> int:
    if input_paths is None:
        targets: list[Path] = [Path(DEFAULT_INPUT).absolute()]
    else:
        targets = [p.absolute() for p in to_path_list(input_paths)]

    valid_targets: list[Path] = []
    invalid_count = 0
    for target in dict.fromkeys(targets):
        if target.exists():
            valid_targets.append(target)
        else:
            invalid_count += 1
            logger.error("Input path does not exist: {}", target)
    if not valid_targets:
        logger.error("No valid input files or directories were supplied.")
        return 1

    first_target = valid_targets[0]
    working_dir = first_target.parent if first_target.is_file() else first_target
    if not change_working_directory(target_dir=working_dir):
        return 1

    all_files_to_compress: list[Path] = []

    _ex_folders: list[str] = exclude_folders if exclude_folders is not None else DEFAULT_EXCLUDE_FOLDERS
    _ex_extensions: list[str] = exclude_extensions if exclude_extensions is not None else DEFAULT_EXCLUDE_EXTENSIONS

    active_exclude_folders: list[str] = [f.lower() for f in _ex_folders]
    active_exclude_extensions: list[str] = [e.lower() for e in _ex_extensions]
    configured_part_size = max_part_size_gib if max_part_size_gib is not None else DEFAULT_MAX_PART_SIZE_GIB
    if max_part_size_gib == 0:
        configured_part_size = None
    if configured_part_size is not None and configured_part_size < 1:
        logger.error("Maximum archive part size must be at least 1 GiB, got {}.", configured_part_size)
        return 1
    requested_workers = max_workers if max_workers is not None else DEFAULT_MAX_WORKERS
    if requested_workers is not None and requested_workers < 1:
        logger.error("Worker count must be at least 1, got {}.", requested_workers)
        return 1
    configured_dry_run = dry_run if dry_run is not None else DEFAULT_DRY_RUN

    for target in valid_targets:
        if target.is_file():
            all_files_to_compress.append(target)
        elif target.is_dir():
            for f in target.rglob(pattern=DEFAULT_EXTENSION):
                if not f.is_file():
                    continue
                if _is_7zip_volume(f):
                    continue
                # Skip if any part of the path matches an excluded folder
                if any(part.lower() in active_exclude_folders for part in f.parts):
                    continue
                # Skip if the file extension matches an excluded extension
                if f.suffix.lower() in active_exclude_extensions:
                    continue
                all_files_to_compress.append(f)
    # Deduplicate paths
    all_files_to_compress = list(dict.fromkeys(all_files_to_compress))

    if not all_files_to_compress:
        logger.warning("No files found matching '{}' across targets.", DEFAULT_EXTENSION)
        return 1 if invalid_count else 0

    configured_exe = None if force_auto_discovery else seven_zip_exe if seven_zip_exe is not None else DEFAULT_7ZIP_EXE
    executable = configured_exe.absolute() if configured_exe is not None else find_7zip()

    split_description = "off" if configured_part_size is None else f"{configured_part_size} GiB maximum per part"
    logger.info(
        "Found {} files to compress. Archive splitting: {}.",
        len(all_files_to_compress),
        split_description,
    )
    if configured_dry_run:
        for file_path in all_files_to_compress:
            logger.info("Would create {}", file_path.with_suffix(f"{file_path.suffix}.7z"))
        if executable is None or not executable.is_file():
            logger.warning("7-Zip is not currently available; it is required for a real run.")
        logger.success("Dry run complete: {} archive(s) planned.", len(all_files_to_compress))
        return 1 if invalid_count else 0
    if executable is None or not executable.is_file():
        logger.error("7-Zip executable not found: {}. Set DEFAULT_7ZIP_EXE or --seven-zip-exe.", executable)
        return 1

    total_success = 0
    total_skipped = 0
    total_errors = 0

    workers = min(requested_workers or multiprocessing.cpu_count(), len(all_files_to_compress))
    logger.info("Starting {} worker(s).", workers)
    try:
        with multiprocessing.Pool(processes=workers) as pool:
            jobs = [(file_path, executable, configured_part_size) for file_path in all_files_to_compress]
            for finished, (file_path, status, msg) in enumerate(
                pool.imap_unordered(_compress_file, jobs),
                start=1,
            ):
                if status == "SUCCESS":
                    logger.info("[{}/{}] Compressed {}: {}", finished, len(jobs), file_path.name, msg)
                    total_success += 1
                elif status == "SKIPPED":
                    logger.info("[{}/{}] Skipped {}: {}", finished, len(jobs), file_path.name, msg)
                    total_skipped += 1
                else:
                    logger.error("[{}/{}] Failed {}: {}", finished, len(jobs), file_path.name, msg)
                    total_errors += 1
    except (OSError, RuntimeError, ValueError):
        logger.exception("Archive worker pool failed.")
        return 1

    logger.success(
        "Finished: {} compressed, {} skipped, {} errors.",
        total_success,
        total_skipped,
        total_errors,
    )
    return 1 if total_errors > 0 or invalid_count else 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
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
        nargs="*",
        default=None,
        help="List of folder names to exclude from the search (e.g. xf).",
    )
    parser.add_argument(
        "--exclude_extensions",
        type=str,
        nargs="*",
        default=None,
        help="List of file extensions to exclude (e.g. .xf4 .xmdf).",
    )
    parser.add_argument(
        "--max-part-size-gib",
        type=int,
        default=None,
        help="Split each .7z into parts no larger than this many GiB; disabled by default. Use 0 to force no split.",
    )
    executable_group = parser.add_mutually_exclusive_group()
    executable_group.add_argument(
        "--seven-zip-exe",
        type=Path,
        default=None,
        help="Explicit 7z.exe path; otherwise automatically discovered.",
    )
    executable_group.add_argument(
        "--auto-seven-zip",
        action="store_true",
        help="Force automatic 7-Zip discovery, overriding an edited DEFAULT_7ZIP_EXE.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Maximum concurrent file compressions; defaults to CPU count.",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show planned archives without running 7-Zip.",
    )
    parser.add_argument("--console-log-level", default=None, help="Loguru console level, e.g. INFO or SUCCESS.")
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file location; defaults to DEFAULT_LOG_FILE.",
    )
    log_group.add_argument("--no-log-file", action="store_true", help="Disable file logging.")
    parser.add_argument(
        "--pause",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Pause after execution; defaults to DEFAULT_PAUSE_ON_FINISH.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    configured_log = None if args.no_log_file else args.log_file if args.log_file is not None else DEFAULT_LOG_FILE
    log_file = str(configured_log.absolute()) if configured_log is not None else None
    with setup_logger(
        console_log_level=args.console_log_level or DEFAULT_CONSOLE_LOG_LEVEL,
        log_file=log_file,
        file_log_level=DEFAULT_FILE_LOG_LEVEL,
    ):
        try:
            result: int = main(
                input_paths=args.input_paths,
                exclude_folders=args.exclude_folders,
                exclude_extensions=args.exclude_extensions,
                seven_zip_exe=args.seven_zip_exe,
                force_auto_discovery=args.auto_seven_zip,
                max_workers=args.workers,
                max_part_size_gib=args.max_part_size_gib,
                dry_run=args.dry_run,
            )
        except Exception:
            logger.exception("Individual archive compression wrapper failed.")
            result = 1

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    pause_on_finish = args.pause if args.pause is not None else DEFAULT_PAUSE_ON_FINISH
    if pause_on_finish:
        pause_console()
    raise SystemExit(result)
