"""
Rebuilds and shrinks all GeoPackage (.gpkg) databases in the target directories.
Uses Python's native sqlite3 library to run the VACUUM command without needing
QGIS, GDAL, or OSGeo4W environments.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path(".")
# ==============================================================================

import argparse
import sqlite3

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets: list[Path] = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_directories)]

    targets = list(dict.fromkeys(targets))
    if any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    total_success = 0
    total_files = 0

    for target_dir in targets:
        gpkg_files: list[Path] = list(target_dir.glob("*.gpkg"))
        if not gpkg_files:
            logger.warning("No .gpkg files found in {}", target_dir.name)
            continue

        total_files += len(gpkg_files)
        logger.info("Found {} GeoPackages to vacuum in {}.", len(gpkg_files), target_dir.name)

        for gpkg_path in gpkg_files:
            try:
                original_size: int = gpkg_path.stat().st_size
                with sqlite3.connect(database=gpkg_path) as conn:
                    conn.execute("VACUUM")
                new_size: int = gpkg_path.stat().st_size

                saved_bytes: int = original_size - new_size
                saved_mb: float = saved_bytes / (1024 * 1024)

                if saved_bytes > 0:
                    logger.debug("Vacuumed {} (Saved {:.2f} MB)", gpkg_path.name, saved_mb)
                else:
                    logger.debug("Vacuumed {} (No space saved)", gpkg_path.name)

                total_success += 1
            except sqlite3.Error:
                logger.exception("SQLite error while vacuuming {}", gpkg_path.name)
            except OSError:
                logger.exception("OS error while accessing {}", gpkg_path.name)

    if total_files > 0:
        logger.success("Successfully vacuumed {}/{} GeoPackages total.", total_success, total_files)

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
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="vacuum_geopackages.log", file_log_level="DEBUG"):
        result: int = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
