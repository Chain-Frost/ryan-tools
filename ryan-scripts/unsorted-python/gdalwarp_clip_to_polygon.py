"""
Clips all .tif rasters in a directory to a specified polygon shapefile.
Uses osgeo.gdal.Warp() via the Python GDAL API to perform the clipping.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-10.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path(".")
DEFAULT_OUTPUT_DIR = Path(r".\clipped")
DEFAULT_SHAPEFILE = Path(r".\Result_Trim_Polygon.shp")
DEFAULT_SHAPEFILE_LAYER = "Result_Trim_Polygon"
DEFAULT_CRS = "EPSG:28350"
# ==============================================================================

import argparse

from loguru import logger
from osgeo import gdal  # type: ignore

# Enable GDAL exceptions so errors are raised as Python exceptions
gdal.UseExceptions()  # type: ignore

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def run_gdalwarp(input_file: Path, output_file: Path, shapefile: Path, shapefile_layer: str, crs: str) -> bool:
    """
    Run gdal.Warp to clip a raster based on a shapefile via the GDAL Python API.
    """
    logger.debug("Running gdalwarp on {}...", input_file.name)

    warp_options = gdal.WarpOptions(  # type: ignore
        format="GTiff",
        srcSRS=crs,
        dstSRS=crs,
        cutlineDSName=str(shapefile),
        cutlineLayer=shapefile_layer,
        cropToCutline=True,
        multithread=True,
        creationOptions=["COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"],
    )

    try:
        ds = gdal.Warp(str(output_file), str(input_file), options=warp_options)  # type: ignore
        ds.FlushCache()  # type: ignore
        ds = None
        logger.debug("Completed gdalwarp on {}. Output saved to {}.", input_file.name, output_file.name)
        return True
    except Exception:
        logger.exception("Error processing {}", input_file.name)
        return False


def main(*, input_directories: PathOrList | None = None) -> int:
    if input_directories is None:
        targets = [Path(DEFAULT_INPUT_DIR).resolve()]
    else:
        targets = [p.resolve() for p in to_path_list(input_directories)]

    if targets and not change_working_directory(target_dir=targets[0]):
        return 1

    # In a multi-directory batch, if the shapefile isn't absolute, we assume it's relative to the first target
    # or just use it as given. Here we resolve it against the first target to keep behaviour somewhat consistent.
    shapefile = Path(DEFAULT_SHAPEFILE)
    if not shapefile.is_absolute():
        shapefile = targets[0] / shapefile

    if not shapefile.exists():
        logger.error("Shapefile {} does not exist.", shapefile)
        return 1

    total_success = 0
    total_files = 0
    
    for target_directory in targets:
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        if not output_dir.is_absolute():
            output_dir = target_directory / output_dir

        output_dir.mkdir(parents=True, exist_ok=True)

        tif_files = list(target_directory.glob("*.tif"))
        if not tif_files:
            logger.warning("No .tif files found in {}.", target_directory)
            continue
            
        total_files += len(tif_files)

        logger.info("Starting gdalwarp batch process for {} files in {}...", len(tif_files), target_directory.name)
        
        for file_path in tif_files:
            if file_path.parent == output_dir:
                continue

            output_file = output_dir / f"{file_path.stem}_clipped{file_path.suffix}"
            if run_gdalwarp(file_path, output_file, shapefile, DEFAULT_SHAPEFILE_LAYER, DEFAULT_CRS):
                total_success += 1

    logger.success("Completed gdalwarp batch process. {}/{} total files clipped successfully.", total_success, total_files)
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clip rasters to a polygon using GDAL Python API.")
    parser.add_argument(
        "-i", "--input_directories", type=Path, nargs="+", default=None, help="Root directories containing .tif files."
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="gdalwarp_clip.log", file_log_level="DEBUG"):
        result = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
