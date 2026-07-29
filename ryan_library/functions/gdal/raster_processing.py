"""Low-level, Python-native GDAL operations used by the raster workflows.

The helpers in this module deliberately avoid QGIS/OSGeo4W executable paths
and subprocess calls. They operate through the installed ``osgeo`` bindings,
raise Python exceptions on GDAL failures, and verify newly created rasters
before returning them to an orchestrator.

``RasterProfile`` controls only storage characteristics; both profiles are
lossless. ``tuflow`` prioritises broad TUFLOW/GeoTIFF compatibility, while
``efficient`` opts into tiled ZSTD compression and sparse blocks.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from collections.abc import Sequence
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Literal

from loguru import logger
from osgeo import gdal, ogr
from osgeo_utils.gdal_calc import Calc

RasterProfile = Literal["tuflow", "efficient"]
OverviewResampling = Literal["nearest", "average", "bilinear", "cubic", "mode"]
VectorFormat = Literal["gpkg", "shp"]

DEFAULT_OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)


@dataclass(frozen=True, slots=True)
class GdalConcurrency:
    """Describe how CPU capacity is divided across a GDAL batch.

    Attributes:
        workers: Number of files or mosaics that may be processed concurrently.
        threads_per_dataset: Value passed to GDAL's ``NUM_THREADS`` setting.
        cpu_count: Logical CPU count detected when the plan was created.
    """

    workers: int
    threads_per_dataset: str
    cpu_count: int


def plan_gdal_concurrency(file_count: int, requested_workers: int | None = None) -> GdalConcurrency:
    """Allocate CPUs without allowing every concurrent job to use every CPU.

    A single input receives ``ALL_CPUS`` as requested. For a batch, the
    available logical CPUs are divided across the active workers. For example,
    4 files on an 8-CPU machine use 4 workers with 2 GDAL threads each.

    Args:
        file_count: Number of independent files or mosaic groups in the batch.
        requested_workers: Optional upper limit for concurrent jobs.

    Returns:
        The worker count and per-dataset GDAL thread setting.
    """
    if file_count < 1:
        return GdalConcurrency(workers=0, threads_per_dataset="1", cpu_count=max(1, os.cpu_count() or 1))

    cpu_count = max(1, os.cpu_count() or 1)
    if file_count == 1:
        # Avoid an outer worker pool when GDAL itself can parallelise the one dataset.
        return GdalConcurrency(workers=1, threads_per_dataset="ALL_CPUS", cpu_count=cpu_count)

    worker_limit = cpu_count if requested_workers is None else max(1, requested_workers)
    workers = min(file_count, worker_limit)
    threads_per_dataset = str(max(1, cpu_count // workers))
    return GdalConcurrency(workers=workers, threads_per_dataset=threads_per_dataset, cpu_count=cpu_count)


def geotiff_creation_options(profile: RasterProfile, threads: str) -> list[str]:
    """Return lossless GeoTIFF creation options for the selected profile.

    Args:
        profile: ``tuflow`` for conservative DEFLATE output or ``efficient``
            for tiled ZSTD output with sparse zero blocks.
        threads: GDAL thread value, normally ``ALL_CPUS`` or a positive integer
            encoded as a string.

    Raises:
        ValueError: If an unknown profile is supplied.
    """
    common = ["BIGTIFF=IF_SAFER", f"NUM_THREADS={threads}"]
    if profile == "tuflow":
        # Conservative GeoTIFF: widely supported DEFLATE, normal strips, and allocated zero blocks.
        return ["COMPRESS=DEFLATE", "PREDICTOR=2", *common]
    if profile == "efficient":
        # Still lossless, but requires readers whose GDAL/libtiff build supports ZSTD.
        return [
            "COMPRESS=ZSTD",
            "PREDICTOR=2",
            "ZSTD_LEVEL=9",
            "TILED=YES",
            "BLOCKXSIZE=512",
            "BLOCKYSIZE=512",
            "SPARSE_OK=TRUE",
            *common,
        ]
    raise ValueError(f"Unsupported raster profile: {profile}")


def translate_to_geotiff(
    source: Path,
    output: Path,
    *,
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> Path:
    """Convert any GDAL-readable raster to a verified, lossless GeoTIFF.

    Dataset statistics are calculated during translation. The output parent
    directory is created automatically, but an existing output is protected
    unless ``overwrite`` is true.

    Args:
        source: Input raster supported by the installed GDAL drivers.
        output: Destination ``.tif`` path.
        profile: GeoTIFF storage profile.
        threads: GDAL compression thread setting.
        overwrite: Permit replacement of an existing output.

    Returns:
        The resolved destination path.
    """
    source = source.resolve()
    output = output.resolve()
    if source == output:
        raise ValueError(f"Input and output paths are identical: {source}")
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output}")

    output.parent.mkdir(parents=True, exist_ok=True)
    options = gdal.TranslateOptions(
        format="GTiff",
        creationOptions=geotiff_creation_options(profile, threads),
        stats=True,
    )
    logger.debug("Translating raster {} to {} with profile {}", source, output, profile)
    with gdal.ExceptionMgr():
        dataset = gdal.Translate(str(output), str(source), options=options)
        if dataset is None:
            raise RuntimeError(f"GDAL could not translate {source}")
        dataset.FlushCache()
        # Releasing the dataset commits and closes the output before verification.
        dataset = None
        _verify_raster(output)
    logger.info(f"Created GeoTIFF: {output}")
    return output


def build_external_overviews(
    raster: Path,
    *,
    levels: Sequence[int] = DEFAULT_OVERVIEW_LEVELS,
    resampling: OverviewResampling = "nearest",
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    refresh: bool = False,
) -> Path:
    """Build or refresh an external ``.ovr`` overview sidecar.

    The source is opened read-only, which makes GDAL place the overview levels
    in ``<raster>.ovr`` rather than modifying the TIFF. An existing sidecar is
    reused when it is newer than the source and contains at least the requested
    number of levels.

    Args:
        raster: Source GeoTIFF.
        levels: Integer decimation factors such as ``(2, 4, 8, 16)``.
        resampling: Sampling algorithm used to produce smaller levels.
        profile: Compression profile used for the overview sidecar.
        threads: GDAL overview thread setting.
        refresh: Rebuild even when the current sidecar passes freshness checks.

    Returns:
        The external ``.ovr`` path.
    """
    raster = raster.resolve()
    overview = Path(f"{raster}.ovr")
    requested_levels = tuple(level for level in levels if level > 1)
    if not requested_levels:
        raise ValueError("At least one overview level greater than 1 is required.")
    if not refresh and _external_overviews_are_current(raster, overview, len(requested_levels)):
        logger.info(f"External overviews are current: {overview}")
        return overview

    config = {
        "COMPRESS_OVERVIEW": "DEFLATE" if profile == "tuflow" else "ZSTD",
        "PREDICTOR_OVERVIEW": "2",
        "BIGTIFF_OVERVIEW": "IF_SAFER",
        "GDAL_NUM_THREADS": threads,
        "SPARSE_OK_OVERVIEW": "NO" if profile == "tuflow" else "YES",
    }
    logger.debug("Building external overviews for {} at levels {}", raster, requested_levels)
    with gdal.ExceptionMgr(), gdal.config_options(config):
        # Read-only access is intentional: update access can create internal overviews.
        dataset = gdal.Open(str(raster), gdal.GA_ReadOnly)
        if dataset is None:
            raise RuntimeError(f"GDAL could not open {raster}")
        result = dataset.BuildOverviews(resampling.upper(), list(requested_levels))
        dataset = None
        if result != gdal.CE_None:
            raise RuntimeError(f"GDAL failed to build overviews for {raster} (error {result})")
    if not overview.is_file():
        raise RuntimeError(f"GDAL did not create the expected external overview: {overview}")
    logger.info(f"Created external overviews: {overview}")
    return overview


def build_vrt(input_files: Sequence[Path], output: Path) -> Path:
    """Build a virtual mosaic from one or more source rasters.

    The VRT references its input files; callers must keep those files available
    until any subsequent translation has finished.

    Args:
        input_files: Ordered source rasters included in the mosaic.
        output: Destination ``.vrt`` path.

    Returns:
        The resolved VRT path.
    """
    if not input_files:
        raise ValueError("At least one input raster is required to build a VRT.")
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Building VRT {} from {} inputs", output, len(input_files))
    with gdal.ExceptionMgr():
        dataset = gdal.BuildVRT(str(output), [str(path.resolve()) for path in input_files])
        if dataset is None:
            raise RuntimeError(f"GDAL could not build VRT {output}")
        dataset.FlushCache()
        dataset = None
    logger.info(f"Created VRT: {output}")
    return output


def calculate_flood_extent(
    input_file: Path,
    output_raster: Path,
    cutoff: float,
    *,
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> Path:
    """Create a Byte flood mask from the first band of a depth raster.

    Cells greater than or equal to ``cutoff`` receive value 1. Cells below the
    cutoff receive value 0, which is also assigned as the output NoData value.
    Input NoData cells remain masked by ``gdal_calc``.

    Args:
        input_file: Source depth raster.
        output_raster: Destination flood-mask GeoTIFF.
        cutoff: Minimum depth treated as flooded.
        profile: GeoTIFF storage profile.
        threads: GDAL compression thread setting.
        overwrite: Permit replacement of an existing mask.

    Returns:
        The resolved flood-mask path.
    """
    output_raster = output_raster.resolve()
    if output_raster.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_raster}")
    output_raster.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Calculating flood extent for {} at cutoff {}", input_file, cutoff)
    with gdal.ExceptionMgr():
        result = Calc(
            A=str(input_file.resolve()),
            outfile=str(output_raster),
            calc=f"where(A >= {cutoff!r}, 1, 0)",
            type="Byte",
            NoDataValue=0,  # pyright: ignore[reportArgumentType] -- accepted by GDAL; its stub is too narrow.
            creation_options=geotiff_creation_options(profile, threads),
            overwrite=overwrite,
            quiet=True,
        )
        if result is None:
            raise RuntimeError(f"GDAL could not calculate flood extent for {input_file}")
        result.FlushCache()
        result = None
        _verify_raster(output_raster)
    logger.info(f"Created flood extent raster: {output_raster}")
    return output_raster


def polygonize_flood_extent(
    input_raster: Path,
    output_vector: Path,
    *,
    vector_format: VectorFormat = "gpkg",
    overwrite: bool = False,
) -> Path:
    """Polygonize flooded cells from a Byte mask into a vector dataset.

    The raster mask band excludes the NoData value 0, so only flooded cells
    become polygons. The integer ``value`` field records the source cell value.
    GeoPackage is the default; ESRI Shapefile remains available for systems
    that require it.

    Args:
        input_raster: Flood mask created by :func:`calculate_flood_extent`.
        output_vector: Destination ``.gpkg`` or ``.shp`` path.
        vector_format: ``gpkg`` for GeoPackage or ``shp`` for ESRI Shapefile.
        overwrite: Permit deletion and recreation of an existing dataset.

    Returns:
        The resolved vector dataset path.
    """
    input_raster = input_raster.resolve()
    output_vector = output_vector.resolve()
    driver_name = {"gpkg": "GPKG", "shp": "ESRI Shapefile"}.get(vector_format)
    if driver_name is None:
        raise ValueError(f"Unsupported vector format: {vector_format}")
    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise RuntimeError(f"The GDAL {driver_name} driver is unavailable.")
    if output_vector.exists():
        if not overwrite:
            raise FileExistsError(f"Output already exists: {output_vector}")
        driver.DeleteDataSource(str(output_vector))

    output_vector.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Polygonizing flood extent {} to {}", input_raster, output_vector)
    with gdal.ExceptionMgr():
        source = gdal.Open(str(input_raster), gdal.GA_ReadOnly)
        if source is None:
            raise RuntimeError(f"GDAL could not open {input_raster}")
        band = source.GetRasterBand(1)
        destination = driver.CreateDataSource(str(output_vector))
        if destination is None:
            raise RuntimeError(f"GDAL could not create {output_vector}")
        layer = destination.CreateLayer(output_vector.stem, srs=source.GetSpatialRef(), geom_type=ogr.wkbPolygon)
        field = ogr.FieldDefn("value", ogr.OFTInteger)
        if layer.CreateField(field) != ogr.OGRERR_NONE:
            raise RuntimeError(f"GDAL could not create the value field in {output_vector}")
        result = gdal.Polygonize(band, band.GetMaskBand(), layer, 0, [], callback=None)
        destination = None
        source = None
        if result != gdal.CE_None:
            raise RuntimeError(f"GDAL failed to polygonize {input_raster} (error {result})")
    logger.info(f"Created flood extent polygons: {output_vector}")
    return output_vector


def _external_overviews_are_current(raster: Path, overview: Path, expected_count: int) -> bool:
    """Return whether a sidecar is newer than its raster and exposes enough levels."""
    if not overview.is_file() or overview.stat().st_mtime < raster.stat().st_mtime:
        return False
    with gdal.ExceptionMgr():
        dataset = gdal.Open(str(raster), gdal.GA_ReadOnly)
        if dataset is None or dataset.RasterCount < 1:
            return False
        count = dataset.GetRasterBand(1).GetOverviewCount()
        dataset = None
    return count >= expected_count


def _verify_raster(path: Path) -> None:
    """Raise when GDAL cannot reopen a raster with at least one non-empty band."""
    dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
    if dataset is None or dataset.RasterCount < 1 or dataset.RasterXSize < 1 or dataset.RasterYSize < 1:
        raise RuntimeError(f"GDAL created an unreadable or empty raster: {path}")
    dataset = None
