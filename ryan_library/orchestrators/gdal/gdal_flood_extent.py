# ryan_library/orchestrators/gdal/gdal_flood_extent.py
"""Discover rasters and generate flood extents from a selectable source band.

The default discovery pattern targets TUFLOW maximum-depth rasters. Callers may
instead provide any glob pattern, recurse through subdirectories, select a
different raster band, and remove small connected regions with GDAL's sieve
filter. Polygon output defaults to GeoPackage.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import tempfile
from typing import Literal

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    GdalConcurrency,
    RasterProfile,
    VectorFormat,
    calculate_flood_extent,
    plan_gdal_concurrency,
    polygonize_flood_extent,
    sieve_raster,
)
from ryan_library.functions.loguru_helpers import setup_logger


def main_processing(
    paths_to_process: list[Path],
    console_log_level: str = "INFO",
    qgis_path: Path | None = None,
    *,
    cutoff_values: tuple[float, ...] = (0.0,),
    file_patterns: tuple[str, ...] = ("*_d_HR_Max.tif",),
    recursive: bool = False,
    input_band: int = 1,
    sieve_pixels: int | None = None,
    connectedness: Literal[4, 8] = 8,
    keep_intermediate_masks: bool = False,
    profile: RasterProfile = "tuflow",
    vector_format: VectorFormat = "gpkg",
    workers: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Generate raster and polygon flood extents for matching inputs.

    Args:
        paths_to_process: Directories searched for input rasters.
        console_log_level: Loguru console threshold used during the workflow.
        qgis_path: Deprecated compatibility argument; Python GDAL no longer
            requires a QGIS installation path.
        cutoff_values: Depth thresholds in source-raster units.
        file_patterns: Glob patterns used to discover source rasters.
        recursive: Search below each input directory when true.
        input_band: One-based source band used for classification.
        sieve_pixels: Optional minimum connected-region size in pixels.
        connectedness: Four- or eight-connected neighbourhood for sieving.
        keep_intermediate_masks: Retain unsieved masks beside their sources.
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
                for pattern in file_patterns
                for path in (root.resolve().rglob(pattern) if recursive else root.resolve().glob(pattern))
                if path.is_file() and "_FE_" not in path.stem
            }
        )
        if not matched_files:
            logger.warning(f"No files matching {file_patterns} found to process.")
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
                input_band=input_band,
                sieve_pixels=sieve_pixels,
                connectedness=connectedness,
                keep_intermediate_masks=keep_intermediate_masks,
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
    input_band: int = 1,
    sieve_pixels: int | None = None,
    connectedness: Literal[4, 8] = 8,
    keep_intermediate_masks: bool = False,
    profile: RasterProfile = "tuflow",
    vector_format: VectorFormat = "gpkg",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> list[Path]:
    """Create flood-mask rasters and vector datasets beside one depth raster.

    Output names follow ``<input-stem>_FE_<cutoff>m``. When sieving is enabled,
    the classified mask is filtered before polygonization. A pair is current
    only when the final mask is newer than the source and the vector dataset is
    newer than the final mask.
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
            with ExitStack() as stack:
                calculation_output: Path = output_raster
                if sieve_pixels is not None:
                    if keep_intermediate_masks:
                        calculation_output = output_raster.with_name(f"{output_raster.stem}_mask.tif")
                    else:
                        temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="ryan_fe_")))
                        calculation_output = temp_dir / output_raster.name
                calculate_flood_extent(
                    input_file=filepath,
                    output_raster=calculation_output,
                    cutoff=cutoff,
                    input_band=input_band,
                    profile=profile,
                    threads=threads,
                    overwrite=calculation_output.exists(),
                )
                if sieve_pixels is not None:
                    sieve_raster(
                        input_raster=calculation_output,
                        output_raster=output_raster,
                        threshold_pixels=sieve_pixels,
                        connectedness=connectedness,
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
