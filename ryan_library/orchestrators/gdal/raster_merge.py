# ryan_library/orchestrators/gdal/raster_merge.py
"""General raster mosaicking, including clipping to a vector extent."""

from pathlib import Path
import tempfile

from loguru import logger

from ryan_library.functions.gdal.raster_processing import (
    RasterProfile,
    build_external_overviews,
    build_vrt,
    get_raster_extent,
    get_vector_extent,
    translate_to_geotiff,
)


def merge_directory(
    input_directory: Path,
    *,
    file_pattern: str = "*.tif",
    output_tif: Path | None = None,
    output_vrt: Path | None = None,
    output_bounds: tuple[float, float, float, float] | None = None,
    output_srs: str | None = None,
    nodata: float | None = None,
    profile: RasterProfile = "tuflow",
    build_overviews: bool = False,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Merge files matching one non-recursive glob into a VRT and GeoTIFF.

    Output paths default to the input directory name. Existing output files are
    excluded from the input list, preventing a rerun from mosaicking its own
    previous result.
    """
    input_directory = input_directory.resolve()
    base_name = input_directory.name or "merged"
    resolved_tif = (output_tif or input_directory / f"{base_name}.tif").resolve()
    resolved_vrt = (output_vrt or input_directory / f"{base_name}.vrt").resolve()
    inputs = sorted(
        path.resolve()
        for path in input_directory.glob(file_pattern)
        if path.is_file() and path.resolve() not in (resolved_tif, resolved_vrt)
    )
    if not inputs:
        raise FileNotFoundError(f"No files matching {file_pattern!r} found in {input_directory}")
    if not overwrite and (resolved_tif.exists() or resolved_vrt.exists()):
        existing = resolved_tif if resolved_tif.exists() else resolved_vrt
        raise FileExistsError(f"Output already exists: {existing}")

    logger.info(f"Merging {len(inputs)} raster(s) from: {input_directory}")
    build_vrt(
        inputs,
        resolved_vrt,
        output_bounds=output_bounds,
        output_srs=output_srs,
        source_nodata=nodata,
        vrt_nodata=nodata,
    )
    translate_to_geotiff(
        resolved_vrt,
        resolved_tif,
        profile=profile,
        nodata=nodata,
        overwrite=resolved_tif.exists(),
    )
    if build_overviews:
        build_external_overviews(resolved_tif, profile=profile, refresh=True)
    return resolved_vrt, resolved_tif


def merge_directory_by_vector_extent(
    input_directory: Path,
    extent_vector: Path,
    *,
    file_pattern: str = "*.xyz",
    output_tif: Path | None = None,
    output_vrt: Path | None = None,
    output_srs: str | None = None,
    nodata: float | None = -9999.0,
    profile: RasterProfile = "tuflow",
    build_overviews: bool = False,
    overwrite: bool = False,
    list_only: bool = False,
) -> list[Path]:
    """Select rasters intersecting a vector bounding box and optionally merge them.

    Extents are compared in their native coordinates; callers must ensure the
    vector and source rasters use the same CRS. Selection uses actual GDAL
    raster extents rather than relying on a particular filename convention.
    """
    input_directory = input_directory.resolve()
    bounds = get_vector_extent(extent_vector)
    candidates = sorted(path for path in input_directory.glob(file_pattern) if path.is_file())
    selected = [path for path in candidates if _extents_intersect(get_raster_extent(path), bounds)]
    if not selected:
        raise FileNotFoundError(f"No rasters matching {file_pattern!r} intersect {extent_vector}")
    logger.info(f"Selected {len(selected)} of {len(candidates)} raster(s) intersecting {extent_vector}")
    if list_only:
        for path in selected:
            logger.info(f"Selected: {path}")
        return selected

    base_name = f"{input_directory.name}_extent"
    resolved_tif = (output_tif or input_directory / f"{base_name}.tif").resolve()
    resolved_vrt = (output_vrt or input_directory / f"{base_name}.vrt").resolve()
    if not overwrite and (resolved_tif.exists() or resolved_vrt.exists()):
        existing = resolved_tif if resolved_tif.exists() else resolved_vrt
        raise FileExistsError(f"Output already exists: {existing}")

    build_vrt(
        selected,
        resolved_vrt,
        output_bounds=bounds,
        output_srs=output_srs,
        source_nodata=nodata,
        vrt_nodata=nodata,
    )
    translate_to_geotiff(
        resolved_vrt,
        resolved_tif,
        profile=profile,
        nodata=nodata,
        overwrite=resolved_tif.exists(),
    )
    if build_overviews:
        build_external_overviews(resolved_tif, profile=profile, refresh=True)
    return selected


def merge_rasters_to_temporary_vrt(
    input_files: list[Path],
    output_tif: Path,
    *,
    profile: RasterProfile = "tuflow",
    overwrite: bool = False,
) -> Path:
    """Merge explicit inputs while discarding the intermediate VRT."""
    with tempfile.TemporaryDirectory(prefix="ryan_gdal_merge_") as temporary_directory:
        vrt = Path(temporary_directory) / "merge.vrt"
        build_vrt(input_files, vrt)
        return translate_to_geotiff(vrt, output_tif, profile=profile, overwrite=overwrite)


def _extents_intersect(first: tuple[float, float, float, float], second: tuple[float, float, float, float]) -> bool:
    """Return whether two closed axis-aligned extents overlap."""
    return first[2] >= second[0] and first[0] <= second[2] and first[3] >= second[1] and first[1] <= second[3]
