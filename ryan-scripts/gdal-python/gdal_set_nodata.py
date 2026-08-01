r"""Set raster NoData metadata in place for every matching file.

This changes only the NoData value stored in each selected raster band; it does
not rewrite pixel values. Edit the constants below for double-click/IDE use or
pass CLI options.

Examples::

    python gdal_set_nodata.py "D:\Terrain" --nodata -9999
    python gdal_set_nodata.py "D:\Terrain" --pattern "*.tif" --bands 1 --recursive

Common scenario::

    # Set -9999 on TIFF metadata in the current folder.
    python gdal_set_nodata.py "D:\Terrain" --pattern "*.tif" --nodata -9999
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.3"

WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
FILE_PATTERN = "*.tif"
NODATA_VALUE: float = -9999.0
BANDS: tuple[int, ...] | None = None
RECURSIVE = False
WORKERS: int | None = None

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_maintenance import set_nodata_in_directory


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    file_pattern: str | None = None,
    nodata_value: float | None = None,
    bands: tuple[int, ...] | None = None,
    recursive: bool | None = None,
    workers: int | None = None,
) -> int:
    """Resolve wrapper settings and update matching rasters through the shared library."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            outputs: list[Path] = set_nodata_in_directory(
                directory=target_directory,
                pattern=file_pattern or FILE_PATTERN,
                nodata=NODATA_VALUE if nodata_value is None else nodata_value,
                bands=bands if bands is not None else BANDS,
                recursive=RECURSIVE if recursive is None else recursive,
                workers=workers if workers is not None else WORKERS,
            )
            logger.info(f"NoData metadata updated for {len(outputs)} raster(s).")
        except Exception:
            logger.exception("NoData metadata update failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse optional command-line overrides for the editable constants."""
    parser = argparse.ArgumentParser(
        description="Set NoData metadata on matching rasters without changing pixels.",
        epilog=r"""Processing scenario:
  python gdal_set_nodata.py "D:\Terrain" --pattern "*.tif" --nodata -9999

This edits metadata in place. Existing pixel values are not replaced or recalculated.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory to process.")
    parser.add_argument("--console-log-level")
    parser.add_argument("--pattern", help="Input glob; default: *.tif.")
    parser.add_argument("--nodata", type=float, help="NoData value; default: -9999.")
    parser.add_argument("--bands", nargs="+", type=int, help="One-based bands; default: every band.")
    parser.add_argument("--recursive", action="store_true", default=None)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(
        working_directory=args.directory,
        console_log_level=args.console_log_level,
        file_pattern=args.pattern,
        nodata_value=args.nodata,
        bands=tuple(args.bands) if args.bands else None,
        recursive=args.recursive,
        workers=args.workers,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
