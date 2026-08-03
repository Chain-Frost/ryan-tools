"""Discover raster files and coordinate concurrent GDAL batch operations.

This module owns directory traversal, timestamp-based skip decisions, and CPU
allocation. Individual raster operations remain in
``ryan_library.functions.gdal.raster_processing`` so wrappers stay small.
"""

from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    DEFAULT_OVERVIEW_LEVELS,
    GdalConcurrency,
    OverviewResampling,
    RasterProfile,
    build_external_overviews,
    plan_gdal_concurrency,
    translate_to_geotiff,
)

DEFAULT_SOURCE_EXTENSIONS: tuple[str, ...] = (".flt", ".asc", ".rst", ".xyz")


def convert_rasters(
    directory: Path,
    *,
    extensions: Sequence[str] = DEFAULT_SOURCE_EXTENSIONS,
    output_directory: Path | None = None,
    output_suffix: str = "",
    recursive: bool = True,
    profile: RasterProfile = "tuflow",
    build_overviews: bool = True,
    overview_levels: Sequence[int] = DEFAULT_OVERVIEW_LEVELS,
    overview_resampling: OverviewResampling = "nearest",
    workers: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Convert supported rasters and optionally create external overviews.

    Each input is written beside its source with the same stem and a ``.tif``
    extension. A current output is skipped unless ``overwrite`` is true. When
    a TIFF is regenerated, its external overviews are refreshed automatically.

    Args:
        directory: Root directory searched for source rasters.
        extensions: Case-insensitive input extensions, with or without dots.
        output_directory: Optional output root. The source directory structure
            is reproduced below it.
        output_suffix: Text appended to each source stem before ``.tif``.
        recursive: Search child directories when true.
        profile: GeoTIFF and overview storage profile.
        build_overviews: Create ``.tif.ovr`` sidecars after conversion.
        overview_levels: Overview decimation factors.
        overview_resampling: Resampling algorithm for overview pixels.
        workers: Optional upper limit for concurrently processed rasters.
        overwrite: Regenerate outputs even when timestamps indicate they are current.

    Returns:
        Output TIFF paths, including current files that required no work.
    """
    sources: list[Path] = _find_sources(directory, extensions, recursive)
    if not sources:
        logger.warning(f"No source rasters found in: {directory}")
        return []

    concurrency: GdalConcurrency = plan_gdal_concurrency(len(sources), workers)
    output_root = output_directory.resolve() if output_directory is not None else directory.resolve()
    logger.info(
        f"Processing {len(sources)} raster(s) with {concurrency.workers} worker(s) and "
        f"{concurrency.threads_per_dataset} GDAL thread(s) per raster."
    )

    def process(source: Path) -> Path:
        relative = source.relative_to(directory.resolve())
        output = output_root / relative.parent / f"{source.stem}{output_suffix}.tif"
        # Timestamp checks avoid recompressing large terrain rasters on routine reruns.
        needs_conversion: bool = overwrite or not output.exists() or output.stat().st_mtime < source.stat().st_mtime
        if needs_conversion:
            translate_to_geotiff(
                source,
                output,
                profile=profile,
                threads=concurrency.threads_per_dataset,
                overwrite=output.exists(),
            )
        else:
            logger.info(f"GeoTIFF is current: {output}")
        if build_overviews:
            build_external_overviews(
                output,
                levels=overview_levels,
                resampling=overview_resampling,
                profile=profile,
                threads=concurrency.threads_per_dataset,
                refresh=needs_conversion,
            )
        return output

    return _run_batch(items=sources, workers=concurrency.workers, operation=process)


def add_overviews(
    directory: Path,
    *,
    recursive: bool = True,
    profile: RasterProfile = "tuflow",
    levels: Sequence[int] = DEFAULT_OVERVIEW_LEVELS,
    resampling: OverviewResampling = "nearest",
    workers: int | None = None,
    refresh: bool = False,
) -> list[Path]:
    """Create or refresh external overviews for GeoTIFFs below a directory.

    Args:
        directory: Root directory searched for ``.tif`` files.
        recursive: Search child directories when true.
        profile: Compression profile for the overview sidecars.
        levels: Overview decimation factors.
        resampling: Resampling algorithm for overview pixels.
        workers: Optional upper limit for concurrent rasters.
        refresh: Rebuild sidecars even when they are current.

    Returns:
        Paths to the external overview sidecars.
    """
    pattern: Literal["**/*.tif"] | Literal["*.tif"] = "**/*.tif" if recursive else "*.tif"
    rasters: list[Path] = sorted(path for path in directory.resolve().glob(pattern) if path.is_file())
    if not rasters:
        logger.warning(f"No GeoTIFFs found in: {directory}")
        return []

    concurrency: GdalConcurrency = plan_gdal_concurrency(file_count=len(rasters), requested_workers=workers)
    logger.info(
        f"Processing {len(rasters)} raster(s) with {concurrency.workers} worker(s) and "
        f"{concurrency.threads_per_dataset} GDAL thread(s) per raster."
    )

    def process(raster: Path) -> Path:
        return build_external_overviews(
            raster,
            levels=levels,
            resampling=resampling,
            profile=profile,
            threads=concurrency.threads_per_dataset,
            refresh=refresh,
        )

    return _run_batch(rasters, concurrency.workers, process)


def _find_sources(directory: Path, extensions: Sequence[str], recursive: bool) -> list[Path]:
    """Return sorted files matching normalized, case-insensitive extensions."""
    normalized: set[str] = {
        extension.lower() if extension.startswith(".") else f".{extension.lower()}" for extension in extensions
    }
    pattern = "**/*" if recursive else "*"
    return sorted(
        path for path in directory.resolve().glob(pattern) if path.is_file() and path.suffix.lower() in normalized
    )


def _run_batch[T](items: Sequence[Path], workers: int, operation: Callable[[Path], T]) -> list[T]:
    """Run one path operation serially or in a bounded thread pool."""
    if workers == 1:
        return [operation(item) for item in items]

    results: list[T] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[Future[T], Path] = {executor.submit(operation, item): item for item in items}
        for future in as_completed(futures):
            item: Path = futures[future]
            try:
                results.append(future.result())
            except Exception:
                logger.exception(f"GDAL processing failed for: {item}")
                raise
    return results
