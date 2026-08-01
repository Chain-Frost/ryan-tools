r"""Merge a directory of rasters into a persistent VRT and GeoTIFF.

This replaces the general ``gdal_merge_CLI.bat`` workflow. The GeoTIFF uses
the TUFLOW-compatible profile by default; ``--profile efficient`` opts into a
smaller, tiled ZSTD output. External overviews are optional.

Examples::

    python gdal_merge.py "D:\Tiles" --pattern "*.tif"
    python gdal_merge.py "D:\Tiles" --output-tif "D:\Mosaics\dem.tif" --overviews
"""

from __future__ import annotations

import argparse
import gc
from pathlib import Path

from loguru import logger

from ryan_library.functions.gdal.raster_processing import RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_library_version
from ryan_library.orchestrators.gdal.raster_merge import merge_directory

WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
FILE_PATTERN = "*.tif"
OUTPUT_BASENAME: str | None = None
OUTPUT_TIF: Path | None = None
OUTPUT_VRT: Path | None = None
OUTPUT_SRS: str | None = None
NODATA_VALUE: float | None = None
PROFILE: RasterProfile = "tuflow"
BUILD_OVERVIEWS = False
OVERWRITE = False


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    file_pattern: str | None = None,
    output_basename: str | None = None,
    output_tif: Path | None = None,
    output_vrt: Path | None = None,
    output_srs: str | None = None,
    nodata_value: float | None = None,
    profile: RasterProfile | None = None,
    build_overviews: bool | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and merge matching rasters through the shared library."""
    print_library_version()
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    configured_basename: str | None = output_basename or OUTPUT_BASENAME
    default_tif: Path | None = target_directory / f"{configured_basename}.tif" if configured_basename else None
    default_vrt: Path | None = target_directory / f"{configured_basename}.vrt" if configured_basename else None
    configured_tif: Path | None = output_tif or OUTPUT_TIF or default_tif
    configured_vrt: Path | None = output_vrt or OUTPUT_VRT or default_vrt
    resolved_tif: Path | None = configured_tif.resolve() if configured_tif else None
    resolved_vrt: Path | None = configured_vrt.resolve() if configured_vrt else None
    if not change_working_directory(target_dir=target_directory):
        return 1
    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            vrt, tif = merge_directory(
                input_directory=target_directory,
                file_pattern=file_pattern or FILE_PATTERN,
                output_tif=resolved_tif,
                output_vrt=resolved_vrt,
                output_srs=output_srs if output_srs is not None else OUTPUT_SRS,
                nodata=nodata_value if nodata_value is not None else NODATA_VALUE,
                profile=profile or PROFILE,
                build_overviews=BUILD_OVERVIEWS if build_overviews is None else build_overviews,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Mosaic complete: {vrt} and {tif}")
        except Exception:
            logger.exception("Raster merge failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse optional command-line overrides for the editable constants."""
    parser = argparse.ArgumentParser(description="Merge matching rasters to VRT and GeoTIFF.")
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("file_pattern", nargs="?", help="Positional input glob, matching the old BAT interface.")
    parser.add_argument("output_basename", nargs="?", help="Positional output stem, matching the old BAT interface.")
    parser.add_argument("--console-log-level")
    parser.add_argument("--pattern", help="Input glob; default: *.tif.")
    parser.add_argument("--output-tif", type=Path)
    parser.add_argument("--output-vrt", type=Path)
    parser.add_argument("--output-srs", help="Optional target CRS, for example EPSG:7855.")
    parser.add_argument("--nodata", type=float)
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument("--overviews", action="store_true", default=None, help="Create external .ovr pyramids.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(
        working_directory=args.directory,
        console_log_level=args.console_log_level,
        file_pattern=args.pattern or args.file_pattern,
        output_basename=args.output_basename,
        output_tif=args.output_tif,
        output_vrt=args.output_vrt,
        output_srs=args.output_srs,
        nodata_value=args.nodata,
        profile=args.profile,
        build_overviews=args.overviews,
        overwrite=args.overwrite,
    )
    gc.collect()
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
