r"""Create valid-data vector footprints for matching rasters.

GDAL traces the valid-data region of each raster, so the result is more useful
than a simple rectangular file extent. GeoPackage is the default output; use
``--vector-format shp`` where Shapefile is required.

Examples::

    python gdal_raster_footprint.py "D:\Rasters"
    python gdal_raster_footprint.py "D:\Rasters" --pattern "*.vrt" --vector-format shp

Common scenario::

    # Recursively create valid-data footprints.
    python gdal_raster_footprint.py "D:\Rasters" --pattern "*.tif" --vector-format gpkg --recursive
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.3"

WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
FILE_PATTERN = "*.tif"
VECTOR_FORMAT: VectorFormat = "gpkg"
LAYER_NAME = "raster_footprint"
RECURSIVE = True
WORKERS: int | None = None
OVERWRITE = False

import argparse

from loguru import logger

from ryan_library.functions.gdal.raster_processing import VectorFormat
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_maintenance import create_footprints_in_directory


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    file_pattern: str | None = None,
    vector_format: VectorFormat | None = None,
    layer_name: str | None = None,
    recursive: bool | None = None,
    workers: int | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and create footprints through the shared library."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            outputs = create_footprints_in_directory(
                target_directory,
                pattern=file_pattern or FILE_PATTERN,
                vector_format=vector_format or VECTOR_FORMAT,
                layer_name=layer_name or LAYER_NAME,
                recursive=RECURSIVE if recursive is None else recursive,
                workers=workers if workers is not None else WORKERS,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Created or retained {len(outputs)} raster footprint(s).")
        except Exception:
            logger.exception("Raster footprint creation failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse optional command-line overrides for the editable constants."""
    parser = argparse.ArgumentParser(
        description="Create GPKG or SHP valid-data footprints for matching rasters.",
        epilog=r"""Processing scenario:
  python gdal_raster_footprint.py "D:\Rasters" --pattern "*.tif" --vector-format gpkg --recursive

Each output follows <raster-stem>_footprint.gpkg. Use --vector-format shp when
the receiving workflow requires Shapefile.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--console-log-level")
    parser.add_argument("--pattern", help="Input glob; default: *.tif.")
    parser.add_argument("--vector-format", choices=("gpkg", "shp"))
    parser.add_argument("--layer-name")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.directory,
        console_log_level=args.console_log_level,
        file_pattern=args.pattern,
        vector_format=args.vector_format,
        layer_name=args.layer_name,
        recursive=args.recursive,
        workers=args.workers,
        overwrite=args.overwrite,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
