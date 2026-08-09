# ryan_library/orchestrators/gdal/raster_mosaic.py
"""Group similarly named raster tiles and create persistent GeoTIFF mosaics.

The grouping rule supports established TUFLOW result names where one delimited
field identifies a tile or scenario that should not appear in the merged name.
Temporary VRTs provide the mosaic view without leaving intermediate files in
the results directory.
"""

from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import tempfile

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    DEFAULT_OVERVIEW_LEVELS,
    OverviewResampling,
    RasterProfile,
    build_external_overviews,
    build_vrt,
    plan_gdal_concurrency,
    translate_to_geotiff,
)

DEFAULT_ALLOWED_SUFFIXES: tuple[str, ...] = ("d_HR_Max", "h_HR_Max", "V_Max", "DEM_Z_HR")


def create_grouped_mosaics(
    directory: Path,
    *,
    group_remove_index: int = 2,
    allowed_suffixes: Sequence[str] = DEFAULT_ALLOWED_SUFFIXES,
    profile: RasterProfile = "tuflow",
    overview_levels: Sequence[int] = DEFAULT_OVERVIEW_LEVELS,
    overview_resampling: OverviewResampling = "nearest",
    workers: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Mosaic TIFFs grouped by removing one delimited filename field.

    For the default ``group_remove_index=2``, both
    ``01_X143_DEV_d_HR_Max.tif`` and ``01_X144_DEV_d_HR_Max.tif`` map to the
    group ``01_DEV_d_HR_Max``. The resulting file is named
    ``merged_01_DEV_d_HR_Max.tif``.

    Args:
        directory: Root searched recursively for GeoTIFF tiles; outputs are
            also written here.
        group_remove_index: One-based field removed after splitting stems on
            underscores and plus signs.
        allowed_suffixes: Raster-name suffixes eligible for mosaicking.
        profile: GeoTIFF and overview storage profile.
        overview_levels: Overview decimation factors.
        overview_resampling: Resampling algorithm for overview pixels.
        workers: Optional upper limit for concurrent mosaic groups.
        overwrite: Force regeneration even when outputs are newer than inputs.

    Returns:
        Paths to the merged GeoTIFF outputs.
    """
    directory = directory.resolve()
    files = sorted(
        path
        for path in directory.rglob("*.tif")
        if not path.name.startswith("merged_") and any(path.stem.endswith(suffix) for suffix in allowed_suffixes)
    )
    groups = _group_files(files, group_remove_index)
    if not groups:
        logger.warning(f"No matching GeoTIFF groups found in: {directory}")
        return []

    concurrency = plan_gdal_concurrency(len(groups), workers)
    logger.info(
        f"Processing {len(groups)} mosaic group(s) with {concurrency.workers} worker(s) and "
        f"{concurrency.threads_per_dataset} GDAL thread(s) per mosaic."
    )

    def process(item: tuple[str, list[Path]]) -> Path:
        group_name, group_files = item
        output = directory / f"merged_{group_name}.tif"
        needs_conversion = (
            overwrite
            or not output.exists()
            or any(path.stat().st_mtime > output.stat().st_mtime for path in group_files)
        )
        if not needs_conversion:
            logger.info(f"Mosaic already exists: {output}")
        else:
            # VRTs are disposable views; only the translated GeoTIFF is retained.
            with tempfile.TemporaryDirectory(prefix="ryan_gdal_vrt_") as temporary_directory:
                vrt = Path(temporary_directory) / f"{group_name}.vrt"
                build_vrt(group_files, vrt)
                translate_to_geotiff(
                    vrt,
                    output,
                    profile=profile,
                    threads=concurrency.threads_per_dataset,
                    overwrite=output.exists(),
                )
        build_external_overviews(
            output,
            levels=overview_levels,
            resampling=overview_resampling,
            profile=profile,
            threads=concurrency.threads_per_dataset,
            refresh=needs_conversion,
        )
        return output

    items = list(groups.items())
    if concurrency.workers == 1:
        return [process(item) for item in items]
    with ThreadPoolExecutor(max_workers=concurrency.workers) as executor:
        return list(executor.map(process, items))


def _group_files(files: Sequence[Path], group_remove_index: int) -> dict[str, list[Path]]:
    """Group paths by stems with one underscore/plus-delimited field removed."""
    if group_remove_index < 1:
        raise ValueError("group_remove_index must be one or greater.")
    groups: defaultdict[str, list[Path]] = defaultdict(list)
    for path in files:
        parts = re.split(r"[+_]", path.stem)
        index = group_remove_index - 1
        if index >= len(parts):
            logger.warning(f"Skipping filename without field {group_remove_index}: {path.name}")
            continue
        del parts[index]
        groups["_".join(parts)].append(path)
    return dict(groups)
