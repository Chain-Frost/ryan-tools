# ryan_library/functions/gdal/raster_processing.py
"""Low-level, Python-native GDAL operations used by the raster workflows.

The helpers in this module deliberately avoid QGIS/OSGeo4W executable paths
and subprocess calls. They operate through the installed ``osgeo`` bindings,
raise Python exceptions on GDAL failures, and verify newly created rasters
before returning them to an orchestrator.

``RasterProfile`` controls only storage characteristics; both profiles are
lossless. ``tuflow`` prioritises broad TUFLOW/GeoTIFF compatibility, while
``efficient`` opts into tiled ZSTD compression and sparse blocks.
"""

__lazy_modules__ = ["numpy"]

# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from collections.abc import Sequence
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Literal

from loguru import logger
import numpy as np
from osgeo import gdal, ogr

RasterProfile = Literal["tuflow", "efficient"]
OverviewResampling = Literal["nearest", "average", "bilinear", "cubic", "mode"]
VectorFormat = Literal["gpkg", "shp"]

DEFAULT_OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)


def read_raster_band(
    raster: str | Path,
    *,
    band: int = 1,
    window: tuple[int, int, int, int] | None = None,
) -> np.ndarray:
    """Read one raster band through GDAL without Rasterio's NumPy shape shim.

    Args:
        raster: Raster path supported by GDAL.
        band: One-based band number.
        window: Optional ``(x_offset, y_offset, width, height)`` window.

    Returns:
        A NumPy array containing the requested raster cells.
    """
    if band < 1:
        raise ValueError("band must be one or greater.")

    raster_path = Path(raster).resolve()
    with gdal.ExceptionMgr():
        dataset = gdal.Open(str(raster_path), gdal.GA_ReadOnly)
        if dataset is None:
            raise RuntimeError(f"GDAL could not open {raster_path}")
        if band > dataset.RasterCount:
            raise ValueError(f"Raster {raster_path} has {dataset.RasterCount} band(s); requested band {band}.")

        source_band = dataset.GetRasterBand(band)
        if window is None:
            values = source_band.ReadAsArray()
        else:
            x_offset, y_offset, width, height = window
            if x_offset < 0 or y_offset < 0 or width < 1 or height < 1:
                raise ValueError("Raster window offsets must be non-negative and dimensions must be positive.")
            if x_offset + width > dataset.RasterXSize or y_offset + height > dataset.RasterYSize:
                raise ValueError(f"Raster window {window} lies outside {raster_path}.")
            values = source_band.ReadAsArray(x_offset, y_offset, width, height)
        dataset = None

    if values is None:
        raise RuntimeError(f"GDAL could not read band {band} from {raster_path}")
    return np.asarray(values)


def read_masked_raster_band(
    raster: str | Path,
    *,
    band: int = 1,
    window: tuple[int, int, int, int] | None = None,
) -> np.ma.MaskedArray:
    """Read one raster band and apply its GDAL validity mask."""
    if band < 1:
        raise ValueError("band must be one or greater.")

    raster_path = Path(raster).resolve()
    with gdal.ExceptionMgr():
        dataset = gdal.Open(str(raster_path), gdal.GA_ReadOnly)
        if dataset is None:
            raise RuntimeError(f"GDAL could not open {raster_path}")
        if band > dataset.RasterCount:
            raise ValueError(f"Raster {raster_path} has {dataset.RasterCount} band(s); requested band {band}.")

        source_band = dataset.GetRasterBand(band)
        mask_band = source_band.GetMaskBand()
        if window is None:
            values = source_band.ReadAsArray()
            validity = mask_band.ReadAsArray()
        else:
            x_offset, y_offset, width, height = window
            if x_offset < 0 or y_offset < 0 or width < 1 or height < 1:
                raise ValueError("Raster window offsets must be non-negative and dimensions must be positive.")
            if x_offset + width > dataset.RasterXSize or y_offset + height > dataset.RasterYSize:
                raise ValueError(f"Raster window {window} lies outside {raster_path}.")
            values = source_band.ReadAsArray(x_offset, y_offset, width, height)
            validity = mask_band.ReadAsArray(x_offset, y_offset, width, height)
        dataset = None

    if values is None or validity is None:
        raise RuntimeError(f"GDAL could not read band {band} and its validity mask from {raster_path}")
    return np.ma.array(np.asarray(values), mask=np.asarray(validity) == 0, copy=False)


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
    nodata: float | None = None,
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
        nodata: Optional NoData value assigned to every output band.
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
        noData=nodata,
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


def build_vrt(
    input_files: Sequence[Path],
    output: Path,
    *,
    output_bounds: tuple[float, float, float, float] | None = None,
    output_srs: str | None = None,
    source_nodata: float | None = None,
    vrt_nodata: float | None = None,
) -> Path:
    """Build a virtual mosaic from one or more source rasters.

    The VRT references its input files; callers must keep those files available
    until any subsequent translation has finished.

    Args:
        input_files: Ordered source rasters included in the mosaic.
        output: Destination ``.vrt`` path.
        output_bounds: Optional ``(xmin, ymin, xmax, ymax)`` crop bounds.
        output_srs: Optional CRS assigned to the VRT, such as ``EPSG:7851``.
        source_nodata: Optional source value treated as NoData.
        vrt_nodata: Optional NoData value exposed by the VRT.

    Returns:
        The resolved VRT path.
    """
    if not input_files:
        raise ValueError("At least one input raster is required to build a VRT.")
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Building VRT {} from {} inputs", output, len(input_files))
    options = gdal.BuildVRTOptions(
        outputBounds=output_bounds,
        outputSRS=output_srs,
        srcNodata=source_nodata,
        VRTNodata=vrt_nodata,
    )
    with gdal.ExceptionMgr():
        dataset = gdal.BuildVRT(str(output), [str(path.resolve()) for path in input_files], options=options)
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
    input_band: int = 1,
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> Path:
    """Create a Byte flood mask from the first band of a depth raster.

    Cells greater than or equal to ``cutoff`` receive value 1. Cells below the
    cutoff receive value 0, which is also assigned as the output NoData value.
    Input NoData cells remain masked through the source band's GDAL validity
    mask.

    Args:
        input_file: Source depth raster.
        output_raster: Destination flood-mask GeoTIFF.
        cutoff: Minimum depth treated as flooded.
        input_band: One-based source band used for the threshold calculation.
        profile: GeoTIFF storage profile.
        threads: GDAL compression thread setting.
        overwrite: Permit replacement of an existing mask.

    Returns:
        The resolved flood-mask path.
    """
    input_file = input_file.resolve()
    output_raster = output_raster.resolve()
    if input_file == output_raster:
        raise ValueError(f"Input and output paths are identical: {input_file}")
    if output_raster.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_raster}")
    output_raster.parent.mkdir(parents=True, exist_ok=True)
    if input_band < 1:
        raise ValueError("input_band must be one or greater.")
    logger.debug("Calculating flood extent for {} at cutoff {}", input_file, cutoff)
    with gdal.ExceptionMgr():
        source = gdal.Open(str(input_file), gdal.GA_ReadOnly)
        if source is None:
            raise RuntimeError(f"GDAL could not open {input_file}")
        if input_band > source.RasterCount:
            raise ValueError(f"Raster {input_file} has {source.RasterCount} band(s); requested band {input_band}.")

        source_band = source.GetRasterBand(input_band)
        source_mask = source_band.GetMaskBand()
        driver = gdal.GetDriverByName("GTiff")
        destination = driver.Create(
            str(output_raster),
            source.RasterXSize,
            source.RasterYSize,
            1,
            gdal.GDT_Byte,
            options=geotiff_creation_options(profile, threads),
        )
        if destination is None:
            raise RuntimeError(f"GDAL could not create {output_raster}")
        destination.SetGeoTransform(source.GetGeoTransform())
        destination.SetProjection(source.GetProjection())
        destination_band = destination.GetRasterBand(1)
        destination_band.SetNoDataValue(0)

        block_width, block_height = source_band.GetBlockSize()
        chunk_width = block_width if block_width > 0 else min(source.RasterXSize, 1024)
        chunk_height = block_height if block_height > 0 else min(source.RasterYSize, 1024)
        for y_offset in range(0, source.RasterYSize, chunk_height):
            height = min(chunk_height, source.RasterYSize - y_offset)
            for x_offset in range(0, source.RasterXSize, chunk_width):
                width = min(chunk_width, source.RasterXSize - x_offset)
                values = source_band.ReadAsArray(x_offset, y_offset, width, height)
                validity = source_mask.ReadAsArray(x_offset, y_offset, width, height)
                if values is None or validity is None:
                    raise RuntimeError(f"GDAL could not read flood extent source data from {input_file}")
                flood_mask = np.where(
                    (np.asarray(validity) != 0) & (np.asarray(values) >= cutoff),
                    1,
                    0,
                ).astype(np.uint8, copy=False)
                destination_band.WriteArray(flood_mask, x_offset, y_offset)

        destination.FlushCache()
        destination = None
        source = None
        _verify_raster(output_raster)
    logger.info(f"Created flood extent raster: {output_raster}")
    return output_raster


def sieve_raster(
    input_raster: Path,
    output_raster: Path,
    *,
    threshold_pixels: int = 8,
    connectedness: Literal[4, 8] = 8,
    profile: RasterProfile = "tuflow",
    threads: str = "ALL_CPUS",
    overwrite: bool = False,
) -> Path:
    """Remove raster regions smaller than a pixel-count threshold.

    This is the Python equivalent of ``gdal_sieve``. The first source band is
    sieved with all classification pixels participating, while georeferencing,
    data type, and NoData metadata are retained. Including background pixels is
    necessary for small foreground regions to be replaced by that background.
    """
    if threshold_pixels < 1:
        raise ValueError("threshold_pixels must be one or greater.")
    if connectedness not in (4, 8):
        raise ValueError("connectedness must be 4 or 8.")
    input_raster = input_raster.resolve()
    output_raster = output_raster.resolve()
    if output_raster.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_raster}")

    with gdal.ExceptionMgr():
        source = gdal.Open(str(input_raster), gdal.GA_ReadOnly)
        if source is None:
            raise RuntimeError(f"GDAL could not open {input_raster}")
        source_band = source.GetRasterBand(1)
        driver = gdal.GetDriverByName("GTiff")
        output_raster.parent.mkdir(parents=True, exist_ok=True)
        destination = driver.Create(
            str(output_raster),
            source.RasterXSize,
            source.RasterYSize,
            1,
            source_band.DataType,
            options=geotiff_creation_options(profile, threads),
        )
        if destination is None:
            raise RuntimeError(f"GDAL could not create {output_raster}")
        destination.SetGeoTransform(source.GetGeoTransform())
        destination.SetProjection(source.GetProjection())
        destination_band = destination.GetRasterBand(1)
        nodata = source_band.GetNoDataValue()
        if nodata is not None:
            destination_band.SetNoDataValue(nodata)
        result = gdal.SieveFilter(
            source_band,
            None,
            destination_band,
            threshold_pixels,
            connectedness,
        )
        destination.FlushCache()
        destination = None
        source = None
        if result != gdal.CE_None:
            raise RuntimeError(f"GDAL failed to sieve {input_raster} (error {result})")
        _verify_raster(output_raster)
    logger.info(f"Created sieved raster: {output_raster}")
    return output_raster


def set_raster_nodata(raster: Path, nodata: float, *, bands: Sequence[int] | None = None) -> Path:
    """Assign NoData metadata to selected bands of an existing raster in place.

    Pixel values are not changed. This is equivalent to ``gdal_edit
    -a_nodata`` and therefore requires a driver that supports update access.
    """
    raster = raster.resolve()
    with gdal.ExceptionMgr():
        dataset = gdal.Open(str(raster), gdal.GA_Update)
        if dataset is None:
            raise RuntimeError(f"GDAL could not open {raster} for update")
        selected_bands = tuple(bands) if bands is not None else tuple(range(1, dataset.RasterCount + 1))
        for band_number in selected_bands:
            if band_number < 1 or band_number > dataset.RasterCount:
                raise ValueError(f"Band {band_number} does not exist in {raster}")
            dataset.GetRasterBand(band_number).SetNoDataValue(nodata)
        dataset.FlushCache()
        dataset = None
    logger.info(f"Set NoData={nodata:g} on {raster}")
    return raster


def create_raster_footprint(
    input_raster: Path,
    output_vector: Path,
    *,
    vector_format: VectorFormat = "gpkg",
    layer_name: str = "raster_footprint",
    overwrite: bool = False,
) -> Path:
    """Create a valid-data footprint excluding source NoData pixels."""
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
    options = gdal.FootprintOptions(format=driver_name, layerName=layer_name, writeAbsolutePath=True)
    with gdal.ExceptionMgr():
        result = gdal.Footprint(str(output_vector), str(input_raster), options=options)
        if result is None:
            raise RuntimeError(f"GDAL could not create a footprint for {input_raster}")
        result = None
    logger.info(f"Created raster footprint: {output_vector}")
    return output_vector


def get_vector_extent(vector_path: Path) -> tuple[float, float, float, float]:
    """Return the combined ``(xmin, ymin, xmax, ymax)`` extent of all vector layers."""
    vector_path = vector_path.resolve()
    with gdal.ExceptionMgr():
        dataset = gdal.OpenEx(str(vector_path), gdal.OF_VECTOR)
        if dataset is None or dataset.GetLayerCount() < 1:
            raise RuntimeError(f"GDAL could not open vector layers from {vector_path}")
        extents = [dataset.GetLayerByIndex(index).GetExtent() for index in range(dataset.GetLayerCount())]
        dataset = None
    return (
        min(extent[0] for extent in extents),
        min(extent[2] for extent in extents),
        max(extent[1] for extent in extents),
        max(extent[3] for extent in extents),
    )


def get_raster_extent(raster: Path) -> tuple[float, float, float, float]:
    """Return an axis-aligned extent calculated from all four raster corners."""
    raster = raster.resolve()
    with gdal.ExceptionMgr():
        dataset = gdal.Open(str(raster), gdal.GA_ReadOnly)
        if dataset is None:
            raise RuntimeError(f"GDAL could not open {raster}")
        transform = dataset.GetGeoTransform()
        corners = [
            gdal.ApplyGeoTransform(transform, pixel, line)
            for pixel, line in (
                (0, 0),
                (dataset.RasterXSize, 0),
                (0, dataset.RasterYSize),
                (dataset.RasterXSize, dataset.RasterYSize),
            )
        ]
        dataset = None
    return (
        min(point[0] for point in corners),
        min(point[1] for point in corners),
        max(point[0] for point in corners),
        max(point[1] for point in corners),
    )


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
