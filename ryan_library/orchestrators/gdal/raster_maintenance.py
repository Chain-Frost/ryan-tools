"""Batch orchestration for raster metadata edits and vector footprints."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    VectorFormat,
    create_raster_footprint,
    plan_gdal_concurrency,
    set_raster_nodata,
)


def set_nodata_in_directory(
    directory: Path,
    *,
    pattern: str = "*.tif",
    nodata: float = -9999.0,
    bands: tuple[int, ...] | None = None,
    recursive: bool = False,
    workers: int | None = None,
) -> list[Path]:
    """Set NoData metadata in place for all rasters matching a glob."""
    rasters = _find_files(directory, pattern, recursive)
    if not rasters:
        logger.warning(f"No files matching {pattern!r} found in: {directory}")
        return []
    concurrency = plan_gdal_concurrency(len(rasters), workers)

    def process(raster: Path) -> Path:
        return set_raster_nodata(raster, nodata, bands=bands)

    if concurrency.workers == 1:
        return [process(raster) for raster in rasters]
    with ThreadPoolExecutor(max_workers=concurrency.workers) as executor:
        return list(executor.map(process, rasters))


def create_footprints_in_directory(
    directory: Path,
    *,
    pattern: str = "*.tif",
    vector_format: VectorFormat = "gpkg",
    layer_name: str = "raster_footprint",
    recursive: bool = True,
    workers: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Create one valid-data footprint beside every matching raster."""
    rasters = [path for path in _find_files(directory, pattern, recursive) if "_footprint" not in path.stem]
    if not rasters:
        logger.warning(f"No files matching {pattern!r} found in: {directory}")
        return []
    concurrency = plan_gdal_concurrency(len(rasters), workers)
    extension = ".gpkg" if vector_format == "gpkg" else ".shp"

    def process(raster: Path) -> Path:
        output = raster.with_name(f"{raster.stem}_footprint{extension}")
        if output.exists() and not overwrite and output.stat().st_mtime >= raster.stat().st_mtime:
            logger.info(f"Raster footprint is current: {output}")
            return output
        return create_raster_footprint(
            raster,
            output,
            vector_format=vector_format,
            layer_name=layer_name,
            overwrite=output.exists(),
        )

    if concurrency.workers == 1:
        return [process(raster) for raster in rasters]
    with ThreadPoolExecutor(max_workers=concurrency.workers) as executor:
        return list(executor.map(process, rasters))


def _find_files(directory: Path, pattern: str, recursive: bool) -> list[Path]:
    """Return sorted matching files using recursive or direct globbing."""
    directory = directory.resolve()
    glob_pattern = f"**/{pattern}" if recursive else pattern
    return sorted(path for path in directory.glob(glob_pattern) if path.is_file())
