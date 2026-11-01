"""Compress specified directories into one sibling .7z archive per directory.

7-Zip is required. Each input directory is archived as `folder.7z` beside
the source directory, excluding configured folder names and file extensions
with native 7-Zip `-xr!` arguments. Existing archives are skipped (never
overwritten). Input folders are not deleted.
Use ``--max-part-size-gib 5`` for upload-sized volumes or ``--dry-run`` to
preview the outputs without invoking 7-Zip.

The default target is the current working directory. Edit the settings below
or override them on the command line. Progress reports completed *folders*,
not bytes within a running 7-Zip subprocess; a spinner and elapsed timer show
that a large individual archive is still being processed.

Windows example:
    python archive_compress_folder.py -i "Q:\\BGER\\PER\\Project Results" --workers 2 --no-pause

This script uses an installed `ryan-tools` package; a copied wrapper
requires a package version containing the shared archive compression helper.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Wrapper identity and editable project settings (CLI options override these)
# ---------------------------------------------------------------------------
WRAPPER_VERSION = "2026-11-01.2"

DEFAULT_INPUT_PATHS: tuple[Path, ...] = (Path.cwd(),)
DEFAULT_WORKING_DIRECTORY: Path | None = None  # None = first input directory
DEFAULT_EXCLUDE_FOLDERS: tuple[str, ...] = ("xf",)
DEFAULT_EXCLUDE_EXTENSIONS: tuple[str, ...] = (".xf4", ".xmdf", ".2dm", ".dat", ".7z")

DEFAULT_7ZIP_EXE: Path | None = None  # None = auto-discover 7z.exe
DEFAULT_MAX_WORKERS: int | None = None  # None = CPU count, limited to folder count
DEFAULT_MAX_PART_SIZE_GIB: int | None = None  # None = one unsplit .7z; use 5 for upload-sized parts
DEFAULT_DRY_RUN = False
DEFAULT_SHOW_PROGRESS = True  # Rich progress in interactive terminals; plain text otherwise
DEFAULT_CONSOLE_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE: Path | None = Path("archive_compress_folder.log")
DEFAULT_FILE_LOG_LEVEL = "DEBUG"
DEFAULT_PAUSE_ON_FINISH = True
# ---------------------------------------------------------------------------

import argparse
import multiprocessing
import sys
from collections.abc import Sequence

from loguru import logger
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn

from ryan_library.functions.archive_utils import FolderCompressionJob, compress_folder_7zip, find_7zip
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def main(
    *,
    input_paths: PathOrList | None = None,
    exclude_folders: Sequence[str] | None = None,
    exclude_extensions: Sequence[str] | None = None,
    working_directory: Path | None = None,
    seven_zip_exe: Path | None = None,
    force_auto_discovery: bool = False,
    max_workers: int | None = None,
    max_part_size_gib: int | None = None,
    dry_run: bool | None = None,
    show_progress: bool | None = None,
) -> int:
    """Compress the selected folders and report per-folder and batch outcomes."""
    # Resolve all caller-provided paths before any working-directory change.
    targets = [
        path.absolute() for path in to_path_list(input_paths if input_paths is not None else DEFAULT_INPUT_PATHS)
    ]
    if not targets:
        logger.error("No input directories specified.")
        return 1

    configured_working_directory = working_directory if working_directory is not None else DEFAULT_WORKING_DIRECTORY
    workdir = (
        configured_working_directory.absolute()
        if configured_working_directory is not None
        else targets[0].parent
        if targets[0].is_file()
        else targets[0]
    )
    requested_workers = max_workers if max_workers is not None else DEFAULT_MAX_WORKERS
    if requested_workers is not None and requested_workers < 1:
        logger.error("Worker count must be at least 1, got {}.", requested_workers)
        return 1
    configured_part_size = max_part_size_gib if max_part_size_gib is not None else DEFAULT_MAX_PART_SIZE_GIB
    if max_part_size_gib == 0:
        configured_part_size = None
    if configured_part_size is not None and configured_part_size < 1:
        logger.error("Maximum archive part size must be at least 1 GiB, got {}.", configured_part_size)
        return 1
    configured_dry_run = dry_run if dry_run is not None else DEFAULT_DRY_RUN
    configured_exe = None if force_auto_discovery else seven_zip_exe if seven_zip_exe is not None else DEFAULT_7ZIP_EXE
    explicit_exe = configured_exe.absolute() if configured_exe is not None else None
    if not change_working_directory(target_dir=workdir):
        return 1

    folders: list[Path] = []
    invalid_count = 0
    for target in dict.fromkeys(targets):
        if target.is_dir():
            folders.append(target)
        else:
            invalid_count += 1
            logger.error("Input directory does not exist or is not a directory: {}", target)
    if not folders:
        logger.error("No valid input folders to compress.")
        return 1

    worker_count = min(requested_workers or multiprocessing.cpu_count(), len(folders))
    exclusions_folders = tuple(exclude_folders if exclude_folders is not None else DEFAULT_EXCLUDE_FOLDERS)
    exclusions_extensions = tuple(exclude_extensions if exclude_extensions is not None else DEFAULT_EXCLUDE_EXTENSIONS)
    exe = explicit_exe if explicit_exe is not None else find_7zip()
    console = Console()
    split_description = "off" if configured_part_size is None else f"{configured_part_size} GiB maximum per part"
    if configured_dry_run:
        console.print(f"Dry run | folders: {len(folders)} | workers: {worker_count}", markup=False)
        console.print(f"Archive splitting: {split_description}", markup=False)
        for folder in folders:
            console.print(f"Would create: {folder.with_suffix('.7z')} from {folder}", markup=False)
        if exe is None or not exe.is_file():
            logger.warning("7-Zip is not currently available; it is required for a real run.")
        return 1 if invalid_count else 0
    if exe is None or not exe.is_file():
        logger.error("7-Zip executable not found: {}. Set DEFAULT_7ZIP_EXE or --seven-zip-exe.", exe)
        return 1

    jobs = [
        FolderCompressionJob(
            folder=folder,
            executable=exe,
            excluded_folders=exclusions_folders,
            excluded_extensions=exclusions_extensions,
            max_part_size_gib=configured_part_size,
        )
        for folder in folders
    ]
    interactive = (show_progress if show_progress is not None else DEFAULT_SHOW_PROGRESS) and sys.stdout.isatty()
    console.print(f"Folders: {len(jobs)} | workers: {worker_count} | 7-Zip: {exe}", markup=False)
    console.print(
        f"Excluded folders: {exclusions_folders} | excluded extensions: {exclusions_extensions}", markup=False
    )
    console.print(f"Archive splitting: {split_description}", markup=False)
    if len(jobs) == 1:
        console.print(f"Processing: {jobs[0].folder}", markup=False)
        console.print(f"Output: {jobs[0].folder.with_suffix('.7z')}", markup=False)

    counts: dict[str, int] = {"SUCCESS": 0, "SKIPPED": 0, "ERROR": 0}
    with Progress(
        SpinnerColumn(),
        TextColumn("Folder progress"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        disable=not interactive,
    ) as progress:
        task = progress.add_task("Compressing folders", total=len(jobs))
        try:
            with multiprocessing.Pool(processes=worker_count) as pool:
                for finished, result in enumerate(pool.imap_unordered(compress_folder_7zip, jobs), start=1):
                    counts[result.status] += 1
                    progress.advance(task)
                    console.print(f"[{finished}/{len(jobs)}] {result.status}: {result.folder}", markup=False)
                    console.print(f"  Archive: {result.archive}", markup=False)
                    if result.message:
                        console.print(f"  Detail: {result.message}", markup=False)
                    logger.debug(
                        "Folder {} -> {}: {} {}",
                        result.folder,
                        result.archive,
                        result.status,
                        result.message,
                    )
        except (OSError, RuntimeError, ValueError):
            logger.exception("Archive worker pool failed.")
            return 1

    logger.success(
        "Archive compression complete: {} compressed, {} skipped, {} failed, {} invalid input(s).",
        counts["SUCCESS"],
        counts["SKIPPED"],
        counts["ERROR"],
        invalid_count,
    )
    return 1 if invalid_count or counts["ERROR"] else 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-i",
        "--input_paths",
        "--input-paths",
        type=Path,
        nargs="+",
        default=None,
        help="Folders to archive; overrides DEFAULT_INPUT_PATHS.",
    )
    parser.add_argument(
        "--exclude_folders",
        "--exclude-folders",
        nargs="*",
        default=None,
        help="Folder names to exclude; overrides DEFAULT_EXCLUDE_FOLDERS.",
    )
    parser.add_argument(
        "--exclude_extensions",
        "--exclude-extensions",
        nargs="*",
        default=None,
        help="Extensions to exclude; overrides DEFAULT_EXCLUDE_EXTENSIONS.",
    )
    parser.add_argument(
        "--working-directory", type=Path, default=None, help="Working directory; default is first source folder."
    )
    executable_group = parser.add_mutually_exclusive_group()
    executable_group.add_argument(
        "--seven-zip-exe", type=Path, default=None, help="Explicit 7z.exe path; otherwise automatically discovered."
    )
    executable_group.add_argument(
        "--auto-seven-zip",
        action="store_true",
        help="Force automatic 7-Zip discovery, overriding an edited DEFAULT_7ZIP_EXE.",
    )
    parser.add_argument(
        "--workers", type=int, default=None, help="Maximum concurrent folder compressions; defaults to CPU count."
    )
    parser.add_argument(
        "--max-part-size-gib",
        type=int,
        default=None,
        help="Split each .7z into parts no larger than this many GiB; disabled by default. Use 0 to force no split.",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show the archive plan without running 7-Zip.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable the interactive folder progress bar.",
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
    cli = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    configured_log = None if cli.no_log_file else cli.log_file if cli.log_file is not None else DEFAULT_LOG_FILE
    log_file = str(configured_log.absolute()) if configured_log is not None else None
    with setup_logger(
        console_log_level=cli.console_log_level or DEFAULT_CONSOLE_LOG_LEVEL,
        log_file=log_file,
        file_log_level=DEFAULT_FILE_LOG_LEVEL,
    ):
        try:
            exit_code = main(
                input_paths=cli.input_paths,
                exclude_folders=cli.exclude_folders,
                exclude_extensions=cli.exclude_extensions,
                working_directory=cli.working_directory,
                seven_zip_exe=cli.seven_zip_exe,
                force_auto_discovery=cli.auto_seven_zip,
                max_workers=cli.workers,
                max_part_size_gib=cli.max_part_size_gib,
                dry_run=cli.dry_run,
                show_progress=cli.progress,
            )
        except Exception:
            logger.exception("Archive compression wrapper failed.")
            exit_code = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    pause_on_finish = cli.pause if cli.pause is not None else DEFAULT_PAUSE_ON_FINISH
    if pause_on_finish:
        pause_console()
    raise SystemExit(exit_code)
