"""Preview or compact GeoPackages with optional consistent SQLite backups.

A normal run is read-only. Use --execute to VACUUM each validated GeoPackage,
retaining a SQLite-consistent sibling .pre-vacuum.bak by default. Existing
backups are never overwritten; use --no-backup only when this risk is accepted.

Example:
    python vacuum_geopackages.py -i "Q:\\Project\\Model\\GIS" --no-pause
    python vacuum_geopackages.py -i "Q:\\Project\\Model\\GIS" --execute --no-pause

Requires only Python's sqlite3; no GDAL/QGIS installation is necessary.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-10-22.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path()
DEFAULT_EXECUTE = False
DEFAULT_BACKUP = True
# ==============================================================================

import argparse
import os
import sqlite3
from contextlib import closing

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner

GPKG_APPLICATION_ID = 0x47504B47
REQUIRED_TABLES = frozenset({"gpkg_contents", "gpkg_spatial_ref_sys"})


def _check_unlinked_path(path: Path) -> None:
    """Refuse linked inputs/parents before any write; TOCTOU races still need platform acceptance."""
    for candidate in (path, *path.parents):
        if candidate.is_symlink() or candidate.is_junction():
            msg = f"Linked GeoPackage path is not supported: {candidate}"
            raise ValueError(msg)


def _validate_geopackage(connection: sqlite3.Connection) -> None:
    """Check native GeoPackage identity and SQLite integrity before and after VACUUM."""
    application_id = connection.execute("PRAGMA application_id").fetchone()
    if application_id is None or application_id[0] != GPKG_APPLICATION_ID:
        msg = "Database does not have a GeoPackage application_id"
        raise ValueError(msg)
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?, ?)",
        tuple(REQUIRED_TABLES),
    )
    found = {str(row[0]) for row in rows}
    if found != REQUIRED_TABLES:
        msg = "Database is missing required GeoPackage metadata tables"
        raise ValueError(msg)
    integrity = connection.execute("PRAGMA quick_check(1)").fetchone()
    if integrity != ("ok",):
        msg = f"SQLite quick_check failed: {integrity}"
        raise ValueError(msg)


def _connect_existing(path: Path, *, read_only: bool) -> sqlite3.Connection:
    """Open an existing database without ever creating a missing input."""
    mode = "ro" if read_only else "rw"
    return sqlite3.connect(f"{path.as_uri()}?mode={mode}", uri=True, timeout=1.0)


def _backup_geopackage(connection: sqlite3.Connection, path: Path) -> Path:
    """Create an exclusive SQLite snapshot. Never replace an existing backup."""
    backup_path = path.with_name(f"{path.name}.pre-vacuum.bak")
    descriptor = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        with closing(sqlite3.connect(backup_path, timeout=1.0)) as target:
            connection.backup(target)
            _validate_geopackage(target)
    except Exception:
        # Only remove the newly reserved, incomplete backup. Leave successful
        # backups in place even if the later VACUUM fails.
        backup_path.unlink(missing_ok=True)
        raise
    return backup_path


def _process_geopackage(path: Path, *, execute: bool, backup: bool) -> tuple[int, int]:
    """Return input/output byte counts after read-only validation or VACUUM."""
    _check_unlinked_path(path)
    if not path.is_file():
        msg = f"GeoPackage does not exist: {path}"
        raise ValueError(msg)

    original_size = path.stat().st_size
    with closing(_connect_existing(path, read_only=not execute)) as connection:
        _validate_geopackage(connection)
        if not execute:
            return original_size, original_size

        if backup:
            saved = _backup_geopackage(connection, path)
            logger.info("Preserved SQLite backup: {}", saved)
        connection.execute("VACUUM")
        _validate_geopackage(connection)

    return original_size, path.stat().st_size


def main(
    *,
    input_directories: PathOrList | None = None,
    execute: bool | None = None,
    backup: bool | None = None,
) -> int:
    """Preview by default; modify validated inputs only with explicit execution."""
    targets = (
        [Path(DEFAULT_INPUT_DIR).absolute()]
        if input_directories is None
        else [p.absolute() for p in to_path_list(input_directories)]
    )
    targets = list(dict.fromkeys(targets))
    if any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    do_execute = DEFAULT_EXECUTE if execute is None else execute
    make_backup = DEFAULT_BACKUP if backup is None else backup
    if do_execute and not make_backup:
        logger.warning("Backup explicitly disabled; VACUUM will modify originals in place.")

    total_success = 0
    total_files = 0
    for target_dir in targets:
        gpkg_files = sorted(target_dir.glob("*.gpkg"))
        if not gpkg_files:
            logger.warning("No .gpkg files found in {}", target_dir.name)
            continue

        total_files += len(gpkg_files)
        logger.info("Found {} GeoPackages in {}.", len(gpkg_files), target_dir.name)
        for gpkg_path in gpkg_files:
            try:
                before, after = _process_geopackage(gpkg_path, execute=do_execute, backup=make_backup)
                if do_execute:
                    logger.success("Vacuumed {} (saved {:.2f} MB)", gpkg_path.name, (before - after) / 1_048_576)
                else:
                    logger.success("DRY RUN: would vacuum {} ({:.2f} MB)", gpkg_path.name, before / 1_048_576)
                total_success += 1
            except (sqlite3.Error, OSError, ValueError):
                logger.exception("Cannot safely vacuum {}", gpkg_path)

    status = "vacuumed" if do_execute else "validated for preview"
    logger.success("Successfully {} {}/{} GeoPackages.", status, total_success, total_files)
    return 0 if total_success == total_files else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview or safely VACUUM GeoPackage files (preview is default).")
    parser.add_argument(
        "-i",
        "--input_directories",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories containing GeoPackages.",
    )
    parser.add_argument("--execute", action="store_true", help="Run SQLite VACUUM after validation (modifies inputs).")
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="With --execute, skip the default SQLite-consistent .pre-vacuum.bak backup.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    with setup_logger(console_log_level="SUCCESS", log_file="vacuum_geopackages.log", file_log_level="DEBUG"):
        result = main(input_directories=args.input_directories, execute=args.execute, backup=not args.no_backup)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
