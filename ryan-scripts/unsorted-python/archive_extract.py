"""
Extracts standard archives (like .zip, .tar, .gz) found in target directories
into subfolders. Uses multiprocessing to unpack multiple archives concurrently.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import shutil
import subprocess
from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
# ==============================================================================

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def _find_7z() -> Path | None:
    if which_7z := shutil.which("7z"):
        return Path(which_7z)

    paths: list[Path] = [
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "7-Zip" / "7z.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "7-Zip" / "7z.exe",
    ]
    for p in paths:
        if p.exists():
            return p
    return None


SEVEN_ZIP_EXE = _find_7z()


def _extract_archive(archive_path: Path) -> tuple[Path, bool, str]:
    """Worker function to extract a single archive."""
    extract_dir: Path = archive_path.parent / archive_path.stem
    temporary_extract_dir = archive_path.parent / f".{archive_path.stem}.extracting"
    try:
        if extract_dir.exists():
            return archive_path, True, f"Output {extract_dir.name} already exists"

        if temporary_extract_dir.exists():
            shutil.rmtree(temporary_extract_dir)
        temporary_extract_dir.mkdir(parents=True)

        if SEVEN_ZIP_EXE:
            # -y = yes to all prompts, -o = output directory
            result: subprocess.CompletedProcess[str] = subprocess.run(
                args=[str(object=SEVEN_ZIP_EXE), "x", "-y", f"-o{temporary_extract_dir}", str(object=archive_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"7z exit code {result.returncode}: {result.stderr.strip() or result.stdout.strip()}"
                )
        else:
            # Fallback to shutil if 7-Zip isn't installed (won't work for .7z or .rar)
            shutil.unpack_archive(filename=str(archive_path), extract_dir=str(temporary_extract_dir))

        temporary_extract_dir.replace(extract_dir)
        return archive_path, True, ""
    except Exception as e:
        shutil.rmtree(temporary_extract_dir, ignore_errors=True)
        return archive_path, False, str(e)


def main(*, input_paths: PathOrList | None = None) -> int:
    if input_paths is None:
        targets: list[Path] = [Path(DEFAULT_INPUT).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_paths)]

    if targets:
        first_target: Path = targets[0]
        working_dir: Path = first_target.parent if first_target.is_file() else first_target
        if not change_working_directory(target_dir=working_dir):
            return 1

    # shutil formats don't have leading dots (e.g. 'zip', 'tar', 'gztar')
    # basic extensions:
    supported_extensions: set[str] = {".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar"}

    all_archives: list[Path] = []

    for target in targets:
        if target.is_file():
            if target.suffix.lower() in supported_extensions:
                all_archives.append(target)
            else:
                logger.warning("File {} is not a supported archive.", target.name)
        elif target.is_dir():
            for ext in supported_extensions:
                all_archives.extend(target.rglob(f"*{ext}"))
        else:
            logger.warning("Target {} does not exist", target)

    all_archives = list(dict.fromkeys(all_archives))

    if not all_archives:
        logger.warning("No supported archives found across targets.")
        return 0

    logger.info("Found {} archives to extract. Starting multiprocessing pool...", len(all_archives))

    total_success = 0
    total_files: int = len(all_archives)

    workers = min(multiprocessing.cpu_count(), len(all_archives))
    with multiprocessing.Pool(processes=workers) as pool:
        for file_path, success, error_msg in pool.imap_unordered(_extract_archive, all_archives):
            if success:
                logger.debug("Extracted {}", file_path.name)
                total_success += 1
            else:
                logger.error("Failed to extract {}: {}", file_path.name, error_msg)

    logger.success("Successfully extracted {}/{} archives.", total_success, total_files)
    return 0 if total_success == total_files else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extracts archives into subfolders concurrently.")
    parser.add_argument(
        "-i",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories or specific archives.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="archive_extract.log", file_log_level="DEBUG"):
        result = main(input_paths=args.input_paths)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
