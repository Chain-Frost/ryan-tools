r"""Select raster tiles intersecting a vector extent and merge them.

GDAL reads each raster's actual georeferenced extent and compares it with the
bounding box of the selected GPKG or SHP layer, without depending on tile
names. Inputs and the vector must use the same CRS; this workflow does not
reproject the extent before selection.

Examples::

    python gdal_merge_by_extent.py "D:\XYZ" "D:\Extent\site.gpkg"
    python gdal_merge_by_extent.py "D:\Tiles" site.shp --pattern "*.tif" --list-only

Common scenario::

    # Find intersecting XYZ tiles, crop the mosaic to a vector bounding box,
    # assign NoData, and create a VRT plus GeoTIFF.
    python gdal_merge_by_extent.py "D:\XYZ" "D:\Extent\site.gpkg" --pattern "*.xyz" --nodata -9999
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.3"

WORKING_DIR: Path = Path(__file__).resolve().parent
EXTENT_VECTOR: Path | None = None
CONSOLE_LOG_LEVEL = "INFO"
FILE_PATTERN = "*.xyz"
OUTPUT_TIF: Path | None = None
OUTPUT_VRT: Path | None = None
OUTPUT_SRS: str | None = None
NODATA_VALUE: float | None = -9999.0
PROFILE: RasterProfile = "tuflow"
BUILD_OVERVIEWS = False
LIST_ONLY = False
OVERWRITE = False

import argparse

from loguru import logger

from ryan_library.functions.gdal.raster_processing import RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_merge import merge_directory_by_vector_extent


def main(
    *,
    working_directory: Path | None = None,
    extent_vector: Path | None = None,
    console_log_level: str | None = None,
    file_pattern: str | None = None,
    output_tif: Path | None = None,
    output_vrt: Path | None = None,
    output_srs: str | None = None,
    nodata_value: float | None = None,
    profile: RasterProfile | None = None,
    build_overviews: bool | None = None,
    list_only: bool | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings, select intersecting tiles, and optionally merge them."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    configured_extent = extent_vector or EXTENT_VECTOR
    if configured_extent is None:
        print("No extent vector configured. Set EXTENT_VECTOR or pass it as the second positional argument.")
        return 1
    resolved_extent = configured_extent.resolve()
    resolved_tif = output_tif.resolve() if output_tif else (OUTPUT_TIF.resolve() if OUTPUT_TIF else None)
    resolved_vrt = output_vrt.resolve() if output_vrt else (OUTPUT_VRT.resolve() if OUTPUT_VRT else None)
    if not change_working_directory(target_dir=target_directory):
        return 1
    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            selected = merge_directory_by_vector_extent(
                target_directory,
                resolved_extent,
                file_pattern=file_pattern or FILE_PATTERN,
                output_tif=resolved_tif,
                output_vrt=resolved_vrt,
                output_srs=output_srs if output_srs is not None else OUTPUT_SRS,
                nodata=nodata_value if nodata_value is not None else NODATA_VALUE,
                profile=profile or PROFILE,
                build_overviews=BUILD_OVERVIEWS if build_overviews is None else build_overviews,
                list_only=LIST_ONLY if list_only is None else list_only,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Selected {len(selected)} raster tile(s).")
        except Exception:
            logger.exception("Extent-based raster merge failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse optional command-line overrides for the editable constants."""
    parser = argparse.ArgumentParser(
        description="Select and merge rasters intersecting a GPKG or SHP extent.",
        epilog=r"""Processing scenario:
  Select XYZ tiles intersecting a GeoPackage extent and create a cropped mosaic:
    python gdal_merge_by_extent.py "D:\XYZ" "D:\Extent\site.gpkg" --pattern "*.xyz" --nodata -9999

  Inspect the selection without creating a VRT or TIFF:
    python gdal_merge_by_extent.py "D:\XYZ" "D:\Extent\site.gpkg" --list-only

The input rasters and extent vector must already use the same CRS.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory containing raster or XYZ tiles.")
    parser.add_argument("extent", nargs="?", type=Path, help="GPKG or SHP whose bounding box selects tiles.")
    parser.add_argument("--console-log-level")
    parser.add_argument("--pattern", help="Input glob; default: *.xyz.")
    parser.add_argument("--output-tif", type=Path)
    parser.add_argument("--output-vrt", type=Path)
    parser.add_argument("--output-srs")
    parser.add_argument("--nodata", type=float)
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument("--overviews", action="store_true", default=None)
    parser.add_argument("--list-only", action="store_true", default=None)
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.directory,
        extent_vector=args.extent,
        console_log_level=args.console_log_level,
        file_pattern=args.pattern,
        output_tif=args.output_tif,
        output_vrt=args.output_vrt,
        output_srs=args.output_srs,
        nodata_value=args.nodata,
        profile=args.profile,
        build_overviews=args.overviews,
        list_only=args.list_only,
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
