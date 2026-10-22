"""Rebuilds and shrinks all GeoPackage (.gpkg) databases in the target directories.
Uses Python's native sqlite3 library to run the VACUUM command without needing
QGIS, GDAL, or OSGeo4W environments.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-10-16.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path()
# ==============================================================================

import argparse

from loguru import logger

from ryan_library.functions.file_verification import has_linked_component
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def _vacuum_backup(source: sqlite3.Connection, gpkg_path: Path) -> Path:
    """Create an exclusive, consistent SQLite backup without rewriting the original.

    The .bak extension ensures the next run's *.gpkg discovery does not
    accidentally vacuum the backup. Never replace an existing backup.
    """
    backup_path = gpkg_path.with_name(f"{gpkg_path.name}.before-vacuum.bak")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    # Exclusive creation avoids clobbering a user's existing backup.
    descriptor = os.open(backup_path, flags, 0o600)
    os.close(descriptor)
    try:
        with closing(sqlite3.connect(backup_path)) as destination:
            source.backup(destination)
        return backup_path
    except Exception:
        # Only the backup we just created can be cleaned up here.
        backup_path.unlink(missing_ok=True)
        raise


def _vacuum_file(gpkg_path: Path, *, dry_run: bool, backup: bool) -> bool:
    """Vacuum one existing regular database, with optional preview and backup."""
    if has_linked_component(gpkg_path):
        logger.error("Skipping linked GeoPackage or directory: {}", gpkg_path)
        return False
    try:
        original_size = gpkg_path.stat().st_size
        if not gpkg_path.is_file():
            logger.error("Not a regular GeoPackage: {}", gpkg_path)
            return False
        if dry_run:
            logger.info("DRY RUN: would vacuum {} ({} bytes; backup={})", gpkg_path, original_size, backup)
            return True

        # The rw-only SQLite URI prevents creating a new empty database if a
        # file disappears between discovery and connection (incl. UNC paths).
        with closing(sqlite3.connect(f"{gpkg_path.as_uri()}?mode=rw", uri=True, timeout=5)) as connection:
            if backup:
                saved_copy = _vacuum_backup(connection, gpkg_path)
                logger.info("Saved pre-VACUUM database backup: {}", saved_copy)
            connection.execute("VACUUM")

        new_size = gpkg_path.stat().st_size
        saved_bytes = original_size - new_size
        if saved_bytes > 0:
            logger.debug("Vacuumed {} (Saved {:.2f} MB)", gpkg_path.name, saved_bytes / (1024 * 1024))
        else:
            logger.debug("Vacuumed {} (No space saved)", gpkg_path.name)
        return True
    except (sqlite3.Error, OSError):
        logger.exception("Failed to vacuum {}; original may need inspection", gpkg_path)
        return False


def main(*, input_directories: PathOrList | None = None, dry_run: bool = False, backup: bool = False) -> int:
    """Process immediate GeoPackages only, preserving the original target defaults."""
    if input_directories is None:
        targets: list[Path] = [Path(DEFAULT_INPUT_DIR).absolute()]
    else:
        targets = [p.absolute() for p in to_path_list(input_directories)]

    targets = list(dict.fromkeys(targets))
    if any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    if any(has_linked_component(target) for target in targets):
        logger.error("Refusing linked input directory.")
        return 1
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    total_success = 0
    total_files = 0

    for target_dir in targets:
        gpkg_files = sorted(target_dir.glob("*.gpkg"))
        if not gpkg_files:
            logger.warning("No .gpkg files found in {}", target_dir.name)
            continue
        total_files += len(gpkg_files)
        logger.info("Found {} GeoPackages to {} in {}.", len(gpkg_files), "inspect" if dry_run else "vacuum", target_dir)
        for gpkg_path in gpkg_files:
            if _vacuum_file(gpkg_path, dry_run=dry_run, backup=backup):
                total_success += 1

    if total_files > 0:
        logger.success(
            "{} {}/{} GeoPackages total.",
            "Inspected" if dry_run else "Vacuumed",
            total_success,
            total_files,
        )
    return 0 if total_success == total_files else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shrinks GeoPackage databases using the VACUUM command.")
    parser.add_argument(
        "-i",
        "--input_directories",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories containing GeoPackages.",
    )
    parser.add_argument("--dry-run", action="store_true", help="List GeoPackages without modifying their contents.")
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create an exclusive .gpkg.before-vacuum.bak SQLite backup before each VACUUM (refuses existing backups).",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="vacuum_geopackages.log", file_log_level="DEBUG"):
        result: int = main(input_directories=args.input_directories, dry_run=args.dry_run, backup=args.backup)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
