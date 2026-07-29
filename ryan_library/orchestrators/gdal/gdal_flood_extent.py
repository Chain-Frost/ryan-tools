"""Discover TUFLOW maximum-depth rasters and generate flood extents.

The orchestrator processes ``*_d_HR_Max.tif`` inputs non-recursively, creates
one Byte mask and vector dataset per requested cutoff, and skips outputs that
are newer than their source data. Polygon output defaults to GeoPackage.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    GdalConcurrency,
    RasterProfile,
    VectorFormat,
    calculate_flood_extent,
    plan_gdal_concurrency,
    polygonize_flood_extent,
)
from ryan_library.functions.loguru_helpers import setup_logger


def main_processing(
    paths_to_process: list[Path],
    console_log_level: str = "INFO",
    qgis_path: Path | None = None,
    *,
    cutoff_values: tuple[float, ...] = (0.0,),
    profile: RasterProfile = "tuflow",
    vector_format: VectorFormat = "gpkg",
    workers: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Generate flood extents for non-recursive ``*_d_HR_Max.tif`` inputs.

    Args:
        paths_to_process: Directories searched for input rasters.
        console_log_level: Loguru console threshold used during the workflow.
        qgis_path: Deprecated compatibility argument; Python GDAL no longer
            requires a QGIS installation path.
        cutoff_values: Depth thresholds in source-raster units.
        profile: Storage profile used for flood-mask GeoTIFFs.
        vector_format: Polygon output format; ``gpkg`` (default) or ``shp``.
        workers: Optional upper limit for concurrent source rasters.
        overwrite: Force regeneration of current outputs.

    Returns:
        Alternating raster and vector paths for every source/cutoff pair.
    """
    with setup_logger(console_log_level=console_log_level):
        if qgis_path is not None:
            logger.warning(f"Ignoring obsolete QGIS path because Python GDAL is installed: {qgis_path}")

        matched_files: list[Path] = sorted(
            {
                path.resolve()
                for root in paths_to_process
                for path in root.resolve().glob("*_d_HR_Max.tif")
                if path.is_file() and "_FE_" not in path.stem
            }
        )
        if not matched_files:
            logger.warning("No *_d_HR_Max.tif files found to process.")
            return []

        concurrency: GdalConcurrency = plan_gdal_concurrency(len(matched_files), workers)
        logger.info(
            f"Processing {len(matched_files)} raster(s) with {concurrency.workers} worker(s) and "
            f"{concurrency.threads_per_dataset} GDAL thread(s) per raster."
        )

        def process(filepath: Path) -> list[Path]:
            return process_file(
                filepath,
                cutoff_values=cutoff_values,
                profile=profile,
                vector_format=vector_format,
                threads=concurrency.threads_per_dataset,
                overwrite=overwrite,
            )

        if concurrency.workers == 1:
            nested_outputs: list[list[Path]] = [process(filepath=path) for path in matched_files]
        else:
            with ThreadPoolExecutor(max_workers=concurrency.workers) as executor:
                nested_outputs = list(executor.map(process, matched_files))
        outputs: list[Path] = [output for group in nested_outputs for output in group]
        logger.info(f"Flood extent processing completed: {len(outputs)} output file(s).")
        return outputs


def process_file(
    filepath: Path,
    *,
    cutoff_values: tuple[float, ...] = (0.0,),
    profile: RasterProfile = "tuflow",
    vector_format: VectorFormat = "gpkg",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> list[Path]:
    """Create flood-mask rasters and vector datasets beside one depth raster.

    Output names follow ``<input-stem>_FE_<cutoff>m``. A pair is current only
    when the mask is newer than the source and the vector dataset is newer than
    the mask.
    """
    outputs: list[Path] = []
    logger.info(f"Processing flood extents: {filepath}")
    for cutoff in cutoff_values:
        suffix: str = format_cutoff_value(cutoff)
        output_raster: Path = filepath.with_name(f"{filepath.stem}_FE_{suffix}m.tif")
        vector_extension: Literal[".gpkg"] | Literal[".shp"] = ".gpkg" if vector_format == "gpkg" else ".shp"
        output_vector: Path = filepath.with_name(f"{filepath.stem}_FE_{suffix}m{vector_extension}")
        current: bool = (
            output_raster.exists()
            and output_vector.exists()
            and output_raster.stat().st_mtime >= filepath.stat().st_mtime
            and output_vector.stat().st_mtime >= output_raster.stat().st_mtime
        )
        # Treat the raster and vector dataset as one product so partial outputs are rebuilt.
        if current and not overwrite:
            logger.info(f"Flood extent outputs are current: {output_vector}")
        else:
            calculate_flood_extent(
                input_file=filepath,
                output_raster=output_raster,
                cutoff=cutoff,
                profile=profile,
                threads=threads,
                overwrite=output_raster.exists(),
            )
            polygonize_flood_extent(
                input_raster=output_raster,
                output_vector=output_vector,
                vector_format=vector_format,
                overwrite=output_vector.exists(),
            )
        outputs.extend((output_raster, output_vector))
    return outputs


def format_cutoff_value(value: float) -> str:
    """Format a cutoff for filenames, e.g. ``0.05`` -> ``005``."""
    formatted: str = f"{value:g}"
    return formatted.replace("-", "neg").replace(".", "")
