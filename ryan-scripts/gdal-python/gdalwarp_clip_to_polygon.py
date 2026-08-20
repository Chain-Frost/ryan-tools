"""
Clips all .tif rasters in a directory to a specified polygon vector datasource.
Uses osgeo.gdal.Warp() via the Python GDAL API to perform the clipping.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-20.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path(".")
DEFAULT_OUTPUT_DIR = Path(r".\clipped")
DEFAULT_SHAPEFILE = Path(r".\Result_Trim_Polygon.shp")
DEFAULT_CRS = "EPSG:28350"
# ==============================================================================

import argparse

from loguru import logger
from osgeo import gdal  # type: ignore

# Enable GDAL exceptions so errors are raised as Python exceptions
gdal.UseExceptions()  # type: ignore

from ryan_library.functions.gdal.raster_processing import geotiff_creation_options
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def select_cutline_layer(cutline_path: Path) -> str | None:
    """Select an unambiguous layer from a vector cutline datasource."""
    try:
        dataset = gdal.OpenEx(str(cutline_path), gdal.OF_VECTOR | gdal.OF_READONLY)  # type: ignore
        if dataset is None:
            raise RuntimeError("GDAL could not open the cutline datasource")

        layer_names: list[str] = []
        for layer_index in range(dataset.GetLayerCount()):  # type: ignore
            layer = dataset.GetLayer(layer_index)  # type: ignore
            if layer is None:
                raise RuntimeError(f"GDAL could not read cutline layer at index {layer_index}")
            layer_names.append(layer.GetName())  # type: ignore
        dataset = None
    except Exception:
        logger.exception("Could not inspect cutline datasource {}", cutline_path)
        return None

    if not layer_names:
        logger.error("Cutline datasource {} contains no layers.", cutline_path)
        return None
    if len(layer_names) == 1:
        logger.debug("Using the only layer {!r} from {}.", layer_names[0], cutline_path.name)
        return layer_names[0]

    matching_names = [name for name in layer_names if name.casefold() == cutline_path.stem.casefold()]
    if len(matching_names) == 1:
        logger.debug("Using filename-matched layer {!r} from {}.", matching_names[0], cutline_path.name)
        return matching_names[0]

    logger.error(
        "Could not select a unique cutline layer from {}. Available layers: {}",
        cutline_path,
        ", ".join(layer_names),
    )
    return None


def run_gdalwarp(input_file: Path, output_file: Path, cutline_path: Path, cutline_layer: str, crs: str) -> bool:
    """
    Run gdal.Warp to clip a raster based on a vector cutline via the GDAL Python API.
    """
    logger.debug("Running gdalwarp on {}...", input_file.name)

    warp_options = gdal.WarpOptions(  # type: ignore
        format="GTiff",
        srcSRS=crs,
        dstSRS=crs,
        cutlineDSName=str(cutline_path),
        cutlineLayer=cutline_layer,
        cropToCutline=True,
        multithread=True,
        creationOptions=geotiff_creation_options("tuflow", "ALL_CPUS"),
    )

    temporary_output: Path = output_file.with_name(f".{output_file.stem}.tmp{output_file.suffix}")
    try:
        temporary_output.unlink(missing_ok=True)
        ds = gdal.Warp(str(temporary_output), str(input_file), options=warp_options)  # type: ignore
        if ds is None:
            raise RuntimeError("GDAL did not create an output dataset")
        ds.FlushCache()  # type: ignore
        ds = None
        temporary_output.replace(output_file)
        logger.debug("Completed gdalwarp on {}. Output saved to {}.", input_file.name, output_file.name)
        return True
    except Exception:
        logger.exception("Error processing {}", input_file.name)
        try:
            temporary_output.unlink(missing_ok=True)
        except OSError:
            logger.exception("Failed to remove partial output {}", temporary_output.name)
        return False


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

    # In a multi-directory batch, if the cutline isn't absolute, we assume it's relative to the first target
    # or just use it as given. Here we resolve it against the first target to keep behaviour somewhat consistent.
    shapefile = Path(DEFAULT_SHAPEFILE)
    if not shapefile.is_absolute():
        shapefile: Path = targets[0] / shapefile

    if not shapefile.exists():
        logger.error("Cutline datasource {} does not exist.", shapefile)
        return 1

    cutline_layer: str | None = select_cutline_layer(cutline_path=shapefile)
    if cutline_layer is None:
        return 1

    total_success = 0
    total_files = 0

    for target_directory in targets:
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        if not output_dir.is_absolute():
            output_dir: Path = target_directory / output_dir

        output_dir.mkdir(parents=True, exist_ok=True)

        tif_files: list[Path] = list(target_directory.glob("*.tif"))
        if not tif_files:
            logger.warning("No .tif files found in {}.", target_directory)
            continue

        total_files += len(tif_files)

        logger.info("Starting gdalwarp batch process for {} files in {}...", len(tif_files), target_directory.name)

        for file_path in tif_files:
            if file_path.parent == output_dir:
                continue

            output_file: Path = output_dir / f"{file_path.stem}_clipped{file_path.suffix}"
            if run_gdalwarp(
                input_file=file_path,
                output_file=output_file,
                cutline_path=shapefile,
                cutline_layer=cutline_layer,
                crs=DEFAULT_CRS,
            ):
                total_success += 1

    logger.success(
        "Completed gdalwarp batch process. {}/{} total files clipped successfully.", total_success, total_files
    )
    return 0 if total_success == total_files else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clip rasters to a polygon using GDAL Python API.")
    parser.add_argument(
        "-i", "--input_directories", type=Path, nargs="+", default=None, help="Root directories containing .tif files."
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="gdalwarp_clip.log", file_log_level="DEBUG"):
        result: int = main(input_directories=args.input_directories)

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
