"""
Removes the NoData metadata flag from all TIF files in the target directories.
This prevents software like QGIS from treating those pixel values as transparent.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-20.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
# ==============================================================================

import argparse

from loguru import logger

from ryan_library.functions.gdal.raster_processing import clear_raster_nodata
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


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

    total_success = 0
    total_files = 0
    seen_tif_files: set[Path] = set()

    for target in targets:
        tif_files: list[Path] = []
        if target.is_file():
            if target.suffix.lower() == ".tif":
                tif_files.append(target)
            else:
                logger.warning("Input file {} is not a .tif file", target.name)
                continue
        elif target.is_dir():
            tif_files.extend(path for path in target.iterdir() if path.is_file() and path.suffix.lower() == ".tif")
        else:
            logger.warning("Target {} does not exist", target)
            continue

        if not tif_files:
            logger.warning("No .tif files found in {}", target.name)
            continue

        tif_files = [path for path in tif_files if path not in seen_tif_files]
        seen_tif_files.update(tif_files)
        if not tif_files:
            continue

        total_files += len(tif_files)
        logger.info("Found {} TIF files to unset NoData in {}.", len(tif_files), target.name)

        for tif_path in tif_files:
            try:
                clear_raster_nodata(tif_path)
                total_success += 1
            except Exception:
                logger.exception("Failed to process {}", tif_path.name)

    if total_files > 0:
        logger.success("Successfully processed {}/{} TIF files total.", total_success, total_files)

    return 0 if total_success == total_files else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Removes NoData metadata from TIF files.")
    parser.add_argument(
        "-i",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories or specific TIF files.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="unset_nodata.log", file_log_level="DEBUG"):
        result: int = main(input_paths=args.input_paths)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
