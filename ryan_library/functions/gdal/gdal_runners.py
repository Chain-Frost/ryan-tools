"""Stable runner names for the Python-native flood-extent operations.

Older orchestrators imported ``run_gdal_calc`` and ``run_gdal_polygonize``
when those functions launched command-line utilities. Keeping these small
adapters preserves the public names while delegating all work to the shared
``osgeo`` implementation.
"""

from pathlib import Path
import warnings

from ryan_library.functions.gdal.raster_processing import (
    RasterProfile,
    VectorFormat,
    calculate_flood_extent,
    polygonize_flood_extent,
)

warnings.warn(
    "ryan_library.functions.gdal.gdal_runners is deprecated; import calculate_flood_extent and "
    "polygonize_flood_extent from ryan_library.functions.gdal.raster_processing instead. Backwards compatibility "
    "is supported until 31 December 2026.",
    DeprecationWarning,
    stacklevel=2,
)


def run_gdal_calc(
    input_file: Path,
    output_file: str | Path,
    cutoff: float,
    *,
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> Path:
    """Create a flood-extent mask using the in-process GDAL calculator.

    Args:
        input_file: Source depth raster.
        output_file: Destination Byte GeoTIFF.
        cutoff: Minimum depth assigned to the flooded class.
        profile: GeoTIFF storage profile.
        threads: GDAL compression thread setting.
        overwrite: Permit replacement of an existing output.

    Returns:
        The resolved output raster path.
    """
    return calculate_flood_extent(
        input_file,
        Path(output_file),
        cutoff,
        profile=profile,
        threads=threads,
        overwrite=overwrite,
    )


def run_gdal_polygonize(
    input_file: str | Path,
    output_vector: str | Path,
    *,
    vector_format: VectorFormat = "gpkg",
    overwrite: bool = False,
) -> Path:
    """Polygonize a flood-extent mask using the in-process GDAL API.

    Args:
        input_file: Byte flood-mask raster.
        output_vector: Destination GeoPackage or ESRI Shapefile.
        vector_format: ``gpkg`` or ``shp``; defaults to ``gpkg``.
        overwrite: Permit recreation of an existing vector dataset.

    Returns:
        The resolved vector dataset path.
    """
    return polygonize_flood_extent(
        Path(input_file),
        Path(output_vector),
        vector_format=vector_format,
        overwrite=overwrite,
    )
