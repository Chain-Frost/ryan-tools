"""
Converts Esri File Geodatabases (.gdb) to GeoPackage (.gpkg) natively using GDAL.
"""

from __future__ import annotations

import argparse
import multiprocessing
from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-11.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
# ==============================================================================

from loguru import logger
from osgeo import gdal  # type: ignore

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner

# Enable GDAL exceptions so errors bubble up nicely
gdal.UseExceptions()


def _convert_gdb(gdb_path: Path) -> tuple[Path, str, str]:
    """Worker function to convert a single GDB to GPKG."""
    output_gpkg: Path = gdb_path.with_suffix(".gpkg")

    try:
        if output_gpkg.exists():
            return gdb_path, "SKIPPED", f"Output {output_gpkg.name} already exists"

        # VectorTranslate is the Python equivalent of ogr2ogr
        # We don't need to specify source format as GDAL auto-detects .gdb (OpenFileGDB driver)
        gdal.VectorTranslate(  # type: ignore
            destNameOrDestDS=str(object=output_gpkg),
            srcDS=str(object=gdb_path),
            format="GPKG",
        )

        return gdb_path, "SUCCESS", ""
    except Exception as e:
        if output_gpkg.exists():
            try:
                output_gpkg.unlink()
            except OSError:
                pass
        return gdb_path, "ERROR", str(e)


def main(*, input_paths: PathOrList | None = None) -> int:
    if input_paths is None:
        targets: list[Path] = [Path(DEFAULT_INPUT).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_paths)]

    if targets:
        first_target: Path = targets[0]
        working_dir: Path = (
            first_target.parent if first_target.is_file() or first_target.suffix.lower() == ".gdb" else first_target
        )
        if not change_working_directory(target_dir=working_dir):
            return 1

    all_gdbs: list[Path] = []

    for target in targets:
        if target.suffix.lower() == ".gdb":
            all_gdbs.append(target)
        elif target.is_dir():
            # A .gdb is technically a directory, so we look for directories ending in .gdb
            for d in target.rglob(pattern="*.gdb"):
                if d.is_dir():
                    all_gdbs.append(d)
        else:
            logger.warning("Target {} does not exist or is not a .gdb", target)

    # Deduplicate paths
    all_gdbs = list(dict.fromkeys(all_gdbs))

    if not all_gdbs:
        logger.warning("No .gdb folders found across targets.")
        return 0

    logger.info("Found {} .gdb databases. Starting conversion...", len(all_gdbs))

    total_success = 0
    total_skipped = 0
    total_errors = 0

    # We use multiprocessing, but GDAL can sometimes be memory-heavy.
    # cpu_count() // 2 is a safe middle ground for database translation.
    workers: int = max(1, multiprocessing.cpu_count() // 2)

    with multiprocessing.Pool(processes=workers) as pool:
        for gdb_path, status, msg in pool.imap_unordered(_convert_gdb, all_gdbs):
            if status == "SUCCESS":
                logger.debug("Converted {}", gdb_path.name)
                total_success += 1
            elif status == "SKIPPED":
                logger.debug("Skipped {}: {}", gdb_path.name, msg)
                total_skipped += 1
            else:
                logger.error("Failed to convert {}: {}", gdb_path.name, msg)
                total_errors += 1

    logger.success(
        "Finished: {} converted, {} skipped, {} errors.",
        total_success,
        total_skipped,
        total_errors,
    )
    return 1 if total_errors > 0 else 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Converts Esri .gdb to .gpkg using native GDAL.")
    parser.add_argument(
        "-i",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories to search, or specific .gdb folders.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="convert_gdb.log", file_log_level="DEBUG"):
        result: int = main(input_paths=args.input_paths)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
