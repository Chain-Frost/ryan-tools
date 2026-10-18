"""Reusable TUFLOW raster maximum discovery, GDAL processing and reporting.

No project paths, CLI parsing, interactive pause or process exit behavior live
here. The invoking wrapper supplies roots, templates and output-zone mappings.
GDAL 3.12+ with Python osgeo bindings is required for pixel-function maxima.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from loguru import logger
from osgeo import gdal, ogr, osr

from ...functions.gdal.raster_processing import (
    DEFAULT_OVERVIEW_LEVELS,
    build_vrt,
    geotiff_creation_options,
)
from ...functions.gdal.raster_processing import (
    build_external_overviews as shared_build_external_overviews,
)
from ...functions.tuflow.tuflow_result_naming import format_user_template, validate_output_filename

MODEL_FIELD = "Model"
DEFAULT_LEGACY_LOCAL_CRS = "EPSG:28350"
ALGORITHM_ID = "gdal-vrt-max-clip-v3"
OVERVIEW_LEVELS = DEFAULT_OVERVIEW_LEVELS
OVERVIEW_RESAMPLING = "NEAREST"

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ZoneCatalog:
    path: Path
    layer_name: str
    field_name: str
    models: dict[str, str]


@dataclass(frozen=True)
class Job:
    version: str
    model: str
    aep: str
    grid_type: str
    sources: tuple[Path, ...]
    output: Path
    zones: ZoneCatalog
    target_aligned: bool = False
    discovery_error: str | None = None
    legacy_local_crs: str = DEFAULT_LEGACY_LOCAL_CRS
    strict_alignment: bool = False


@dataclass
class JobResult:
    version: str
    model: str
    aep: str
    grid_type: str
    output: Path
    source_count: int
    status: str
    message: str
    runtime_seconds: float = 0.0
    output_size_mb: float = 0.0


@dataclass(frozen=True)
class SourceInfo:
    x_resolution: float
    y_resolution: float
    nodata: float
    data_type: int


@dataclass(frozen=True)
class SourceGroup:
    """A named output zone assembled from one or more source extent folders."""

    model: str
    extents: tuple[str, ...]
    zone_set: Literal["main", "fg"] = "main"


def parse_source_group(value: str) -> SourceGroup:
    """Read a group setting: [main|fg:]OUTPUT=SOURCE1,SOURCE2."""
    left, separator, right = value.partition("=")
    if not separator:
        msg = f"Invalid group {value!r}; expected [main|fg:]OUTPUT=SOURCE1,SOURCE2."
        raise ValueError(msg)
    prefix, colon, model = left.partition(":")
    if colon:
        if prefix not in {"main", "fg"}:
            msg = f"Invalid zone set {prefix!r}; expected main or fg."
            raise ValueError(msg)
        zone_set: Literal["main", "fg"] = "fg" if prefix == "fg" else "main"
    else:
        model = left
        zone_set = "main"
    model = model.strip()
    extents = tuple(part.strip() for part in right.split(","))
    if not model or not extents or any(not part for part in extents):
        msg = f"Invalid source group {value!r}."
        raise ValueError(msg)
    if len({name.casefold() for name in extents}) != len(extents):
        msg = f"Duplicate source extents in group {value!r}."
        raise ValueError(msg)
    return SourceGroup(model=model, extents=extents, zone_set=zone_set)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def load_zone_catalog(path: Path, field_name: str = MODEL_FIELD) -> ZoneCatalog:
    if not path.is_file():
        msg = f"Output-zone GeoPackage not found: {path}"
        raise RuntimeError(msg)

    dataset = ogr.Open(str(path), 0)
    if dataset is None:
        msg = f"OGR could not open: {path}"
        raise RuntimeError(msg)
    if dataset.GetLayerCount() != 1:
        msg = f"Expected one layer in {path.name}, found {dataset.GetLayerCount()}."
        raise RuntimeError(msg)

    layer = dataset.GetLayerByIndex(0)
    definition = layer.GetLayerDefn()
    if definition.GetFieldIndex(field_name) < 0:
        msg = f"Layer {layer.GetName()!r} in {path.name} has no {field_name!r} field."
        raise RuntimeError(msg)

    models: dict[str, str] = {}
    for feature in layer:
        value = feature.GetField(field_name)
        geometry = feature.GetGeometryRef()
        if value is None or not str(value).strip():
            msg = f"Feature {feature.GetFID()} in {path.name} has a blank Model."
            raise RuntimeError(msg)
        model = str(value).strip()
        key = model.casefold()
        if key in models:
            msg = f"Duplicate Model value {model!r} in {path.name}."
            raise RuntimeError(msg)
        if geometry is None or geometry.IsEmpty():
            msg = f"Model {model!r} has empty geometry in {path.name}."
            raise RuntimeError(msg)
        models[key] = model

    layer_name = layer.GetName()
    dataset = None
    if not models:
        msg = f"No model polygons found in {path.name}."
        raise RuntimeError(msg)
    return ZoneCatalog(path=path.absolute(), layer_name=layer_name, field_name=field_name, models=models)


def collect_inputs(grids_dir: Path, grid_type: str) -> tuple[Path, ...]:
    suffix = f"_{grid_type}.tif".casefold()
    return tuple(
        path
        for path in sorted(grids_dir.iterdir())
        if path.is_file()
        and path.name.casefold().endswith(suffix)
        and "maximum" not in path.name.casefold()
        and not path.stem.casefold().endswith("_src")
    )


def discover_extent_sources(
    version_root: Path,
    aep: str,
    grid_types: tuple[str, ...],
) -> dict[str, tuple[str, Path, dict[str, tuple[Path, ...]]]]:
    discovered: dict[str, tuple[str, Path, dict[str, tuple[Path, ...]]]] = {}
    if not version_root.is_dir():
        return discovered

    for extent_dir in sorted(version_root.iterdir()):
        grids_dir = extent_dir / aep / "grids"
        if not extent_dir.is_dir() or not grids_dir.is_dir():
            continue
        inputs_by_grid = {grid_type: collect_inputs(grids_dir, grid_type) for grid_type in grid_types}
        if any(inputs_by_grid.values()):
            discovered[extent_dir.name.casefold()] = (
                extent_dir.name,
                grids_dir,
                inputs_by_grid,
            )
    return discovered


def output_path(
    version: str,
    model: str,
    aep: str,
    grid_type: str,
    fg: bool,
    *,
    output_root: Path,
    template: str,
) -> Path:
    filename = validate_output_filename(
        format_user_template(
            template=template,
            values={"version": version, "model": model, "aep": aep, "grid": grid_type},
            description="TUFLOW maximum output name",
        )
    )
    if fg:
        path = output_root / "FG" / aep / filename
        return path.with_name(f"{path.stem}-clip{path.suffix}")
    return output_root / aep / filename


def discover_jobs(
    versions: tuple[str, ...],
    aeps: tuple[str, ...],
    grid_types: tuple[str, ...],
    wanted_models: set[str] | None,
    main_zones: ZoneCatalog,
    fg_zones: ZoneCatalog | None,
    *,
    input_root: Path,
    output_root: Path,
    output_template: str,
    groups: tuple[SourceGroup, ...] = (),
    legacy_local_crs: str = DEFAULT_LEGACY_LOCAL_CRS,
    strict_alignment: bool = False,
) -> tuple[list[Job], list[str]]:
    """Discover direct model zones and configurable composite output zones.

    A group is a source-folder mapping, not a hard-coded FG algorithm.
    The original FG01/FG02 and FG03 rules remain the defaults.
    """
    jobs: list[Job] = []
    notes: list[str] = []
    group_models: set[str] = set()
    for group in groups:
        key = group.model.casefold()
        if key in group_models:
            msg = f"Duplicate output group {group.model!r}."
            raise ValueError(msg)
        group_models.add(key)

    def wanted(model: str) -> bool:
        return wanted_models is None or model.casefold() in wanted_models

    for version in versions:
        version_root = input_root / version
        for aep in aeps:
            extents = discover_extent_sources(version_root, aep, grid_types)
            if not extents:
                notes.append(f"{version} {aep}: no extent folders containing {aep}\\grids")
                continue

            handled: set[str] = set()
            # Direct models using the main output-zone GeoPackage.
            for extent_key, (_extent_name, _grids_dir, inputs_by_grid) in extents.items():
                model = main_zones.models.get(extent_key)
                if model is None or not wanted(model) or model.casefold() in group_models:
                    continue
                handled.add(extent_key)
                for grid_type in grid_types:
                    sources = inputs_by_grid[grid_type]
                    if sources:
                        jobs.append(
                            Job(
                                version=version,
                                model=model,
                                aep=aep,
                                grid_type=grid_type,
                                sources=sources,
                                output=output_path(
                                    version,
                                    model,
                                    aep,
                                    grid_type,
                                    fg=False,
                                    output_root=output_root,
                                    template=output_template,
                                ),
                                zones=main_zones,
                                legacy_local_crs=legacy_local_crs,
                                strict_alignment=strict_alignment,
                            )
                        )

            # Any number of input extents can contribute to one output zone.
            for group in groups:
                zone_catalog = fg_zones if group.zone_set == "fg" else main_zones
                if zone_catalog is None:
                    msg = f"Group {group.model!r} needs an FG zone GeoPackage."
                    raise ValueError(msg)
                model = zone_catalog.models.get(group.model.casefold())
                if model is None:
                    notes.append(f"{version} {aep}: group zone {group.model!r} missing from {zone_catalog.path}")
                    continue
                if not wanted(model):
                    continue
                records = [extents.get(name.casefold()) for name in group.extents]
                present = [record for record in records if record is not None]
                if not present:
                    continue
                handled.update(name.casefold() for name in group.extents if name.casefold() in extents)
                for grid_type in grid_types:
                    sources = tuple(source for record in present for source in record[2][grid_type])
                    if not sources:
                        continue
                    missing = [
                        name
                        for name, record in zip(group.extents, records, strict=True)
                        if record is None or not record[2][grid_type]
                    ]
                    error = (
                        f"Combined {model} {aep} maximum requires "
                        f"{', '.join(group.extents)}; missing {grid_type} sources for {', '.join(missing)}."
                        if missing
                        else None
                    )
                    jobs.append(
                        Job(
                            version=version,
                            model=model,
                            aep=aep,
                            grid_type=grid_type,
                            sources=sources,
                            output=output_path(
                                version,
                                model,
                                aep,
                                grid_type,
                                fg=(group.zone_set == "fg"),
                                output_root=output_root,
                                template=output_template,
                            ),
                            zones=zone_catalog,
                            target_aligned=len(group.extents) > 1,
                            discovery_error=error,
                            legacy_local_crs=legacy_local_crs,
                            strict_alignment=strict_alignment,
                        )
                    )

            ignored = [record[0] for key, record in extents.items() if key not in handled]
            if ignored:
                notes.append(
                    f"{version} {aep}: ignored extents without a selected output-zone model: "
                    + ", ".join(sorted(ignored))
                )

    jobs.sort(
        key=lambda job: (
            job.version,
            job.aep.casefold(),
            job.model.casefold(),
            job.grid_type.casefold(),
        )
    )
    validate_unique_outputs(jobs)
    return jobs, notes


def validate_unique_outputs(jobs: list[Job]) -> None:
    """Refuse ambiguous output templates before any raster can be overwritten."""
    occupied: dict[str, Job] = {}
    for job in jobs:
        key = str(job.output.absolute()).casefold()
        if previous := occupied.get(key):
            msg = (
                f"Multiple maximum-raster jobs target {job.output}: "
                f"{previous.version}/{previous.model}/{previous.aep}/{previous.grid_type} "
                f"and {job.version}/{job.model}/{job.aep}/{job.grid_type}. "
                "Include distinguishing placeholders in the output template."
            )
            raise ValueError(msg)
        occupied[key] = job


# ---------------------------------------------------------------------------
# Incremental state
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Incremental state
# ---------------------------------------------------------------------------


def file_inventory(paths: tuple[Path, ...]) -> list[dict[str, int | str]]:
    inventory: list[dict[str, int | str]] = []
    for path in sorted(paths, key=lambda item: str(item).casefold()):
        stat = path.stat()
        inventory.append(
            {
                "path": str(path.absolute()),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return inventory


def digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def source_signature(paths: tuple[Path, ...]) -> str:
    return digest(file_inventory(paths))


def zone_signature(path: Path) -> str:
    stat = path.stat()
    return digest(
        {
            "path": str(path.absolute()),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    )


def output_metadata(path: Path) -> dict[str, str]:
    dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
    if dataset is None:
        return {}
    metadata = dataset.GetMetadata() or {}
    dataset = None
    return metadata


def overview_path(raster: Path) -> Path:
    return Path(f"{raster}.ovr")


def overviews_are_current(raster: Path) -> bool:
    sidecar = overview_path(raster)
    if not sidecar.is_file() or sidecar.stat().st_mtime_ns < raster.stat().st_mtime_ns:
        return False
    dataset = gdal.Open(str(raster), gdal.GA_ReadOnly)
    if dataset is None:
        return False
    count = dataset.GetRasterBand(1).GetOverviewCount()
    dataset = None
    return count >= len(OVERVIEW_LEVELS)


def determine_action(
    job: Job,
    profile: str,
    build_overviews: bool,
    overwrite: bool,
) -> tuple[str, str, str, str]:
    sources_hash = source_signature(job.sources)
    zones_hash = zone_signature(job.zones.path)

    if overwrite:
        return "rebuild", "--overwrite requested", sources_hash, zones_hash
    if not job.output.is_file():
        return "rebuild", "output does not exist", sources_hash, zones_hash

    metadata = output_metadata(job.output)
    expected = {
        "TUFLOW_MAX_ALGORITHM": ALGORITHM_ID,
        "TUFLOW_SOURCE_SIGNATURE": sources_hash,
        "TUFLOW_ZONE_SIGNATURE": zones_hash,
        "TUFLOW_MODEL_ZONE": job.model,
        "TUFLOW_AEP": job.aep,
        "TUFLOW_STORAGE_PROFILE": profile,
        "TUFLOW_STRICT_ALIGNMENT": str(job.strict_alignment),
        "TUFLOW_LEGACY_LOCAL_CRS": job.legacy_local_crs,
        "TUFLOW_ZONE_FIELD": job.zones.field_name,
    }
    mismatches = [key for key, value in expected.items() if metadata.get(key) != value]
    if mismatches:
        return (
            "rebuild",
            "missing or changed incremental metadata: " + ", ".join(mismatches),
            sources_hash,
            zones_hash,
        )

    if build_overviews and not overviews_are_current(job.output):
        return "overviews", "raster is current but external overviews are missing or stale", sources_hash, zones_hash
    return "skip", "source rasters, output zones, and processing settings are unchanged", sources_hash, zones_hash


# ---------------------------------------------------------------------------
# Maximum VRT and clipping
# ---------------------------------------------------------------------------


def raster_grid_alignment_matches(
    reference: tuple[float, ...],
    other: tuple[float, ...],
) -> bool:
    """Require identical pixel size and whole-cell origin offsets."""
    if len(reference) != 6 or len(other) != 6:
        return False
    if not math.isclose(reference[1], other[1], rel_tol=0.0, abs_tol=1e-8):
        return False
    if not math.isclose(reference[5], other[5], rel_tol=0.0, abs_tol=1e-8):
        return False
    x_cells = (other[0] - reference[0]) / reference[1]
    y_cells = (other[3] - reference[3]) / reference[5]
    return all(math.isclose(value, round(value), rel_tol=0.0, abs_tol=1e-8) for value in (x_cells, y_cells))


def inspect_sources(paths: tuple[Path, ...], *, strict_alignment: bool = False) -> SourceInfo:
    datasets: list[gdal.Dataset] = []
    x_resolutions: list[float] = []
    y_resolutions: list[float] = []
    nodata_values: list[float | None] = []
    data_types: list[int] = []
    reference_transform: tuple[float, ...] | None = None
    unaligned: list[Path] = []

    try:
        for path in paths:
            dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
            if dataset is None:
                msg = f"GDAL could not open source raster: {path}"
                raise RuntimeError(msg)
            if dataset.RasterCount != 1:
                msg = f"Expected one band in {path.name}."
                raise RuntimeError(msg)
            transform = dataset.GetGeoTransform()
            if transform[2] != 0.0 or transform[4] != 0.0:
                msg = f"Rotated raster grids are not supported: {path.name}"
                raise RuntimeError(msg)
            if transform[1] <= 0.0 or transform[5] >= 0.0:
                msg = f"Expected a north-up raster: {path.name}"
                raise RuntimeError(msg)
            if reference_transform is None:
                reference_transform = transform
            elif not raster_grid_alignment_matches(reference_transform, transform):
                unaligned.append(path)

            band = dataset.GetRasterBand(1)
            datasets.append(dataset)
            x_resolutions.append(abs(transform[1]))
            y_resolutions.append(abs(transform[5]))
            nodata_values.append(band.GetNoDataValue())
            data_types.append(band.DataType)

        first_srs = datasets[0].GetSpatialRef()
        if first_srs is None:
            msg = f"Source raster has no CRS: {paths[0].name}"
            raise RuntimeError(msg)
        for path, dataset in zip(paths[1:], datasets[1:], strict=True):
            spatial_ref = dataset.GetSpatialRef()
            if spatial_ref is None or not first_srs.IsSame(spatial_ref):
                msg = f"Source raster CRS differs: {path.name}"
                raise RuntimeError(msg)

        if any(value is None for value in nodata_values):
            msg = "Every source raster must define a NoData value."
            raise RuntimeError(msg)
        nodata = float(nodata_values[0])
        if not all(math.isclose(float(value), nodata) for value in nodata_values[1:]):
            msg = f"Source NoData values differ: {nodata_values}"
            raise RuntimeError(msg)
        if unaligned:
            mismatches = ", ".join(item.name for item in unaligned)
            if strict_alignment:
                msg = f"Source raster grid resolution or origin is misaligned: {mismatches}"
                raise RuntimeError(msg)
            logger.warning(
                "Source rasters are not cell aligned; legacy nearest-neighbour sampling will be used: {}",
                mismatches,
            )
        if len(set(data_types)) != 1:
            names = [gdal.GetDataTypeName(value) for value in data_types]
            msg = f"Source raster data types differ: {names}"
            raise RuntimeError(msg)

        return SourceInfo(
            x_resolution=min(x_resolutions),
            y_resolution=min(y_resolutions),
            nodata=nodata,
            data_type=data_types[0],
        )
    finally:
        datasets.clear()


def create_maximum_vrt(job: Job, source_info: SourceInfo) -> tuple[gdal.Dataset, str]:
    """Build a GDAL 3.12+ maximum VRT through the shared library helper."""
    virtual_path = f"/vsimem/tuflow_max_{uuid.uuid4().hex}.vrt"
    try:
        if job.target_aligned:
            build_vrt(
                job.sources,
                virtual_path,
                source_nodata=source_info.nodata,
                vrt_nodata=source_info.nodata,
                pixel_function="max",
                resample_alg=gdal.GRA_NearestNeighbour,
                strict=True,
                resolution="user",
                x_res=source_info.x_resolution,
                y_res=source_info.y_resolution,
                target_aligned_pixels=True,
            )
        else:
            build_vrt(
                job.sources,
                virtual_path,
                source_nodata=source_info.nodata,
                vrt_nodata=source_info.nodata,
                pixel_function="max",
                resample_alg=gdal.GRA_NearestNeighbour,
                strict=True,
                resolution="highest",
            )

        # The shared helper closes its output. Keep the reopened VRT alive
        # until the polygon clipping has materialised.
        maximum = gdal.Open(virtual_path, gdal.GA_ReadOnly)
        if maximum is None:
            msg = "GDAL failed to reopen the maximum VRT."
            raise RuntimeError(msg)
        return maximum, virtual_path
    except Exception:
        gdal.Unlink(virtual_path)
        raise


def temporary_tif(destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{destination.stem}_building_",
        suffix=destination.suffix,
        dir=destination.parent,
        delete=False,
    )
    path = Path(handle.name)
    handle.close()
    path.unlink()
    return path


def selected_zone_geometry(job: Job) -> tuple[ogr.DataSource, ogr.Layer, ogr.Geometry]:
    vector_dataset = ogr.Open(str(job.zones.path), 0)
    if vector_dataset is None:
        msg = f"OGR could not open output zones: {job.zones.path}"
        raise RuntimeError(msg)
    layer = vector_dataset.GetLayerByName(job.zones.layer_name)
    if layer is None:
        msg = f"Layer {job.zones.layer_name!r} was not found."
        raise RuntimeError(msg)
    escaped = job.model.replace("'", "''")
    layer.SetAttributeFilter(f"\"{job.zones.field_name}\" = '{escaped}'")
    feature = layer.GetNextFeature()
    if feature is None:
        msg = f"No polygon found for Model={job.model!r}."
        raise RuntimeError(msg)
    geometry = feature.GetGeometryRef()
    if geometry is None or geometry.IsEmpty():
        msg = f"Polygon for Model={job.model!r} is empty."
        raise RuntimeError(msg)
    return vector_dataset, layer, geometry.Clone()


def clip_bounds_and_crs(
    maximum: gdal.Dataset,
    job: Job,
) -> tuple[tuple[float, float, float, float], str | None]:
    transform = maximum.GetGeoTransform()
    if transform[2] != 0.0 or transform[4] != 0.0:
        msg = "Rotated maximum grids are not supported."
        raise RuntimeError(msg)

    _vector_dataset, layer, geometry = selected_zone_geometry(job)
    source_wkt = maximum.GetProjection()
    source_srs = osr.SpatialReference()
    if source_wkt:
        source_srs.ImportFromWkt(source_wkt)
    zone_srs = layer.GetSpatialRef()
    if not source_wkt or zone_srs is None:
        msg = "The maximum raster and output zones must both define a CRS."
        raise RuntimeError(msg)

    source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    zone_srs = zone_srs.Clone()
    zone_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    source_name = (source_srs.GetName() or "").casefold()
    legacy_gda94_mg = bool(source_srs.IsLocal() and source_name == "gda94_mg")
    source_srs_override: str | None = None
    if legacy_gda94_mg:
        # LOCAL_CS["GDA94_MG"] is an unresolved datum label, not a CRS.
        # Preserve the original project's MGA50 assumption, but never copy
        # an arbitrary output-zone CRS onto the raster coordinates.
        assumed_srs = osr.SpatialReference()
        if assumed_srs.SetFromUserInput(job.legacy_local_crs) != 0 or not assumed_srs.IsProjected():
            msg = f"Invalid projected legacy raster CRS assumption: {job.legacy_local_crs!r}"
            raise RuntimeError(msg)
        assumed_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if not zone_srs.IsSame(assumed_srs):
            geometry.Transform(osr.CoordinateTransformation(zone_srs, assumed_srs))
        source_srs_override = assumed_srs.ExportToWkt()
    elif source_srs.IsLocal():
        msg = f"Unsupported local raster CRS {source_srs.GetName()!r}; expected GDA94_MG."
        raise RuntimeError(msg)
    elif not zone_srs.IsSame(source_srs):
        geometry.Transform(osr.CoordinateTransformation(zone_srs, source_srs))

    min_x, max_x, min_y, max_y = geometry.GetEnvelope()
    origin_x, pixel_x, _, origin_y, _, pixel_y = transform
    pixel_y_abs = abs(pixel_y)
    first_column = max(0, math.floor((min_x - origin_x) / pixel_x))
    last_column = min(
        maximum.RasterXSize,
        math.ceil((max_x - origin_x) / pixel_x),
    )
    first_row = max(0, math.floor((origin_y - max_y) / pixel_y_abs))
    last_row = min(
        maximum.RasterYSize,
        math.ceil((origin_y - min_y) / pixel_y_abs),
    )
    if first_column >= last_column or first_row >= last_row:
        msg = f"Model {job.model!r} polygon does not overlap its maximum raster."
        raise RuntimeError(msg)

    bounds = (
        origin_x + first_column * pixel_x,
        origin_y - last_row * pixel_y_abs,
        origin_x + last_column * pixel_x,
        origin_y - first_row * pixel_y_abs,
    )
    return bounds, source_srs_override


def build_external_overviews(raster: Path, profile: str) -> None:
    """Retry the shared GDAL overview helper for slow network file shares."""
    sidecar = overview_path(raster)
    sidecar_existed = sidecar.is_file()
    failures: list[str] = []
    for attempt in range(1, 4):
        if attempt > 1:
            time.sleep(float(attempt - 1))
        try:
            shared_build_external_overviews(
                raster,
                levels=OVERVIEW_LEVELS,
                resampling="nearest",
                profile=profile,
                refresh=True,
            )
            return
        except (RuntimeError, OSError) as error:
            failures.append(f"attempt {attempt}: {error}")
            if not sidecar_existed:
                sidecar.unlink(missing_ok=True)
    raise RuntimeError("GDAL failed to build external overviews after 3 attempts: " + "; ".join(failures))


def materialize_clipped_maximum(
    maximum: gdal.Dataset,
    source_info: SourceInfo,
    job: Job,
    destination: Path,
    profile: str,
    sources_hash: str,
    zones_hash: str,
) -> None:
    bounds, source_srs_override = clip_bounds_and_crs(maximum, job)
    transform = maximum.GetGeoTransform()
    escaped = job.model.replace("'", "''")

    warp_options: dict[str, object] = {
        "format": "GTiff",
        "outputBounds": bounds,
        "xRes": abs(transform[1]),
        "yRes": abs(transform[5]),
        "resampleAlg": gdal.GRA_NearestNeighbour,
        "cutlineDSName": str(job.zones.path),
        "cutlineLayer": job.zones.layer_name,
        "cutlineWhere": f"\"{job.zones.field_name}\" = '{escaped}'",
        "cropToCutline": False,
        "srcNodata": source_info.nodata,
        "dstNodata": source_info.nodata,
        "multithread": True,
        "creationOptions": geotiff_creation_options(profile, threads="ALL_CPUS"),
        "warpOptions": ["NUM_THREADS=ALL_CPUS", "UNIFIED_SRC_NODATA=YES"],
    }
    if source_srs_override is not None:
        warp_options.update(srcSRS=source_srs_override, dstSRS=source_srs_override)

    output = gdal.Warp(
        str(destination),
        maximum,
        options=gdal.WarpOptions(**warp_options),
    )
    if output is None:
        raise RuntimeError(gdal.GetLastErrorMsg() or "GDAL did not create clipped output.")

    output.SetMetadataItem("TUFLOW_MAX_ALGORITHM", ALGORITHM_ID)
    output.SetMetadataItem("TUFLOW_SOURCE_SIGNATURE", sources_hash)
    output.SetMetadataItem("TUFLOW_SOURCE_COUNT", str(len(job.sources)))
    output.SetMetadataItem("TUFLOW_ZONE_SIGNATURE", zones_hash)
    output.SetMetadataItem("TUFLOW_MODEL_ZONE", job.model)
    output.SetMetadataItem("TUFLOW_MODEL_ZONES_SOURCE", str(job.zones.path))
    output.SetMetadataItem("TUFLOW_STORAGE_PROFILE", profile)
    output.SetMetadataItem("TUFLOW_STRICT_ALIGNMENT", str(job.strict_alignment))
    output.SetMetadataItem("TUFLOW_LEGACY_LOCAL_CRS", job.legacy_local_crs)
    output.SetMetadataItem("TUFLOW_ZONE_FIELD", job.zones.field_name)
    output.SetMetadataItem("TUFLOW_VERSION", job.version)
    output.SetMetadataItem("TUFLOW_AEP", job.aep)
    output.SetMetadataItem("TUFLOW_GRID_TYPE", job.grid_type)
    output.SetMetadataItem("TUFLOW_MAXIMUM_METHOD", "GDAL VRT pixel function max")
    output.SetMetadataItem("TUFLOW_RESAMPLING", "nearest neighbour")
    if source_srs_override is not None:
        output.SetMetadataItem("TUFLOW_SOURCE_CRS_OVERRIDE", f"GDA94_MG -> {job.legacy_local_crs} (assumed)")
    output.FlushCache()
    output = None


def publish_output(temporary: Path, destination: Path, build_overviews: bool) -> None:
    """Install a raster/overview pair, restoring prior files on failure.

    Two independent filesystem renames are not fully atomic. Backing up the
    existing pair before publishing lets an ordinary exception restore the
    previous consistent generation rather than leave a new TIFF with an old
    overview. Hard power loss still needs manual recovery.
    """
    temporary_overview = overview_path(temporary)
    destination_overview = overview_path(destination)
    if not temporary.is_file():
        msg = f"Temporary raster is missing: {temporary}"
        raise RuntimeError(msg)
    if build_overviews and not temporary_overview.is_file():
        msg = f"Temporary overview is missing: {temporary_overview}"
        raise RuntimeError(msg)

    marker = uuid.uuid4().hex
    raster_backup = destination.with_name(f".{destination.stem}.{marker}.bak{destination.suffix}")
    overview_backup = Path(f"{raster_backup}.ovr")
    backed_up_raster = False
    backed_up_overview = False
    published_raster = False
    published_overview = False
    try:
        if destination.is_file():
            os.replace(destination, raster_backup)
            backed_up_raster = True
        if destination_overview.is_file():
            os.replace(destination_overview, overview_backup)
            backed_up_overview = True
        os.replace(temporary, destination)
        published_raster = True
        if build_overviews:
            os.replace(temporary_overview, destination_overview)
            published_overview = True
    except Exception:
        # Unlink only replacements that actually reached their destination.
        # A failure while moving an old file must not delete that old file.
        if published_raster:
            destination.unlink(missing_ok=True)
        if published_overview:
            destination_overview.unlink(missing_ok=True)
        if backed_up_raster:
            os.replace(raster_backup, destination)
        if backed_up_overview:
            os.replace(overview_backup, destination_overview)
        raise
    else:
        raster_backup.unlink(missing_ok=True)
        overview_backup.unlink(missing_ok=True)


def run_build(
    job: Job,
    profile: str,
    build_overviews: bool,
    sources_hash: str,
    zones_hash: str,
) -> tuple[float, float]:
    started = time.perf_counter()
    temporary = temporary_tif(job.output)
    virtual_path = ""
    maximum: gdal.Dataset | None = None
    try:
        source_info = inspect_sources(job.sources, strict_alignment=job.strict_alignment)
        maximum, virtual_path = create_maximum_vrt(job, source_info)
        materialize_clipped_maximum(
            maximum,
            source_info,
            job,
            temporary,
            profile,
            sources_hash,
            zones_hash,
        )
        maximum = None
        gdal.Unlink(virtual_path)
        virtual_path = ""
        if build_overviews:
            build_external_overviews(temporary, profile)
        publish_output(temporary, job.output, build_overviews)
        elapsed = time.perf_counter() - started
        return elapsed, job.output.stat().st_size / 1e6
    finally:
        maximum = None
        if virtual_path:
            gdal.Unlink(virtual_path)
        temporary.unlink(missing_ok=True)
        overview_path(temporary).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def write_report(
    path: Path,
    results: list[JobResult],
    jobs: list[Job],
    run_info: list[tuple[str, str]],
) -> bool:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
        from openpyxl.worksheet.worksheet import Worksheet
    except ImportError:
        print("! openpyxl is not installed; the Excel summary was skipped.")
        return False

    header_fill = PatternFill("solid", fgColor="1F3864")
    header_font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
    body_font = Font(name="Arial", size=10)
    status_fills = {
        "BUILT": PatternFill("solid", fgColor="E2EFDA"),
        "OVERVIEWS": PatternFill("solid", fgColor="FFF2CC"),
        "SKIPPED": PatternFill("solid", fgColor="F2F2F2"),
        "FAILED": PatternFill("solid", fgColor="FCE4EC"),
        "DRY-RUN": PatternFill("solid", fgColor="DDEBF7"),
    }

    def table(
        worksheet: Worksheet,
        headers: Sequence[str],
        rows: Sequence[Sequence[str | int | float | datetime | None]],
        widths: Sequence[int],
        status_column: int | None = None,
    ) -> None:
        worksheet.append(headers)
        for index, _header in enumerate(headers, start=1):
            cell = worksheet.cell(1, index)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", wrap_text=True)
        for row in rows:
            worksheet.append(row)
        for row_index in range(2, worksheet.max_row + 1):
            status = worksheet.cell(row_index, status_column).value if status_column is not None else None
            for column_index in range(1, len(headers) + 1):
                cell = worksheet.cell(row_index, column_index)
                cell.font = body_font
                if status in status_fills:
                    cell.fill = status_fills[status]
        for index, width in enumerate(widths, start=1):
            worksheet.column_dimensions[get_column_letter(index)].width = width
        worksheet.freeze_panes = "A2"
        if worksheet.max_row >= 2:
            worksheet.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{worksheet.max_row}"

    workbook = Workbook()

    summary_sheet = workbook.active
    if summary_sheet is None:
        message = "New workbook has no active summary sheet."
        raise RuntimeError(message)
    summary_sheet.title = "Summary"
    grouped: dict[tuple[str, str, str], dict[str, int]] = {}
    for result in results:
        counts = grouped.setdefault((result.version, result.aep, result.model), {})
        counts[result.status] = counts.get(result.status, 0) + 1
    summary_rows = [
        [
            version,
            aep,
            model,
            sum(counts.values()),
            counts.get("BUILT", 0),
            counts.get("OVERVIEWS", 0),
            counts.get("SKIPPED", 0),
            counts.get("DRY-RUN", 0),
            counts.get("FAILED", 0),
        ]
        for (version, aep, model), counts in sorted(grouped.items())
    ]
    table(
        summary_sheet,
        [
            "Version",
            "AEP",
            "Model",
            "Jobs",
            "Built",
            "Overviews",
            "Skipped",
            "Dry run",
            "Failed",
        ],
        summary_rows,
        [12, 12, 20, 10, 10, 12, 10, 10, 10],
    )

    jobs_sheet = workbook.create_sheet("Jobs")
    job_rows = [
        [
            result.version,
            result.aep,
            result.model,
            result.grid_type,
            result.output.name,
            result.source_count,
            result.status,
            result.message,
            round(result.runtime_seconds, 1) if result.runtime_seconds else None,
            round(result.output_size_mb, 2) if result.output_size_mb else None,
            str(result.output.parent),
        ]
        for result in results
    ]
    table(
        jobs_sheet,
        [
            "Version",
            "AEP",
            "Model",
            "Grid type",
            "Output raster",
            "Source count",
            "Status",
            "Message",
            "Runtime (s)",
            "Output size (MB)",
            "Output folder",
        ],
        job_rows,
        [12, 12, 20, 14, 72, 14, 12, 68, 14, 18, 72],
        status_column=7,
    )

    inputs_sheet = workbook.create_sheet("Input Rasters")
    input_rows = []
    for job in jobs:
        for source in job.sources:
            stat = source.stat()
            input_rows.append(
                [
                    job.version,
                    job.aep,
                    job.model,
                    job.grid_type,
                    source.name,
                    round(stat.st_size / 1e6, 2),
                    datetime.fromtimestamp(stat.st_mtime),
                    str(source.parent),
                    job.output.name,
                ]
            )
    table(
        inputs_sheet,
        [
            "Version",
            "AEP",
            "Model",
            "Grid type",
            "Input raster",
            "Size (MB)",
            "Last modified",
            "Input folder",
            "Feeds into output",
        ],
        input_rows,
        [12, 12, 20, 14, 76, 12, 20, 80, 72],
    )
    for row_index in range(2, inputs_sheet.max_row + 1):
        inputs_sheet.cell(row_index, 7).number_format = "yyyy-mm-dd hh:mm:ss"

    info_sheet = workbook.create_sheet("Run Info")
    info_sheet["A1"] = "TUFLOW maximum raster pipeline"
    info_sheet["A1"].font = Font(name="Arial", size=12, bold=True)
    for row_index, (label, value) in enumerate(run_info, start=3):
        info_sheet.cell(row_index, 1, label).font = Font(name="Arial", size=10, bold=True)
        info_sheet.cell(row_index, 2, value).font = body_font
    info_sheet.column_dimensions["A"].width = 30
    info_sheet.column_dimensions["B"].width = 110

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return True


def execute_jobs(
    jobs: list[Job],
    *,
    profile: str,
    build_overviews: bool,
    overwrite: bool,
    dry_run: bool,
    notify: Callable[[str], None] | None = None,
    notify_error: Callable[[str], None] | None = None,
) -> list[JobResult]:
    """Execute planned jobs, returning per-job results without process exits.

    Messages are delivered through optional callbacks; a caller decides how
    to present them. GDAL failures in one job do not prevent other jobs.
    """

    def emit(message: str) -> None:
        if notify is not None:
            notify(message)
        else:
            logger.info("{}", message)

    def emit_error(message: str) -> None:
        if notify_error is not None:
            notify_error(message)
        else:
            logger.error("{}", message)

    results: list[JobResult] = []
    for index, job in enumerate(jobs, start=1):
        label = f"[{index}/{len(jobs)}] {job.version} {job.aep} {job.model} {job.grid_type}"
        emit(f"\n{label}")
        emit(f"  Sources: {len(job.sources)}")
        emit(f"  Output : {job.output}")

        if job.discovery_error:
            emit(f"  FAILED: {job.discovery_error}")
            results.append(
                JobResult(
                    job.version,
                    job.model,
                    job.aep,
                    job.grid_type,
                    job.output,
                    len(job.sources),
                    "FAILED",
                    job.discovery_error,
                )
            )
            continue

        try:
            action, reason, sources_hash, zones_hash = determine_action(
                job,
                profile,
                build_overviews,
                overwrite,
            )
            emit(f"  Action : {action} ({reason})")

            if dry_run:
                results.append(
                    JobResult(
                        job.version,
                        job.model,
                        job.aep,
                        job.grid_type,
                        job.output,
                        len(job.sources),
                        "DRY-RUN",
                        f"Would {action}: {reason}",
                        output_size_mb=(job.output.stat().st_size / 1e6 if job.output.is_file() else 0.0),
                    )
                )
                continue

            if action == "skip":
                results.append(
                    JobResult(
                        job.version,
                        job.model,
                        job.aep,
                        job.grid_type,
                        job.output,
                        len(job.sources),
                        "SKIPPED",
                        reason,
                        output_size_mb=job.output.stat().st_size / 1e6,
                    )
                )
                continue

            if action == "overviews":
                started = time.perf_counter()
                build_external_overviews(job.output, profile)
                elapsed = time.perf_counter() - started
                results.append(
                    JobResult(
                        job.version,
                        job.model,
                        job.aep,
                        job.grid_type,
                        job.output,
                        len(job.sources),
                        "OVERVIEWS",
                        reason,
                        elapsed,
                        job.output.stat().st_size / 1e6,
                    )
                )
                emit(f"  OK: external overviews refreshed in {elapsed:.1f}s")
                continue

            elapsed, size_mb = run_build(
                job,
                profile,
                build_overviews,
                sources_hash,
                zones_hash,
            )
            results.append(
                JobResult(
                    job.version,
                    job.model,
                    job.aep,
                    job.grid_type,
                    job.output,
                    len(job.sources),
                    "BUILT",
                    reason,
                    elapsed,
                    size_mb,
                )
            )
            emit(f"  OK: built {size_mb:.1f} MB in {elapsed:.1f}s")
        except Exception as error:
            emit_error(f"  FAILED: {error}")
            results.append(
                JobResult(
                    job.version,
                    job.model,
                    job.aep,
                    job.grid_type,
                    job.output,
                    len(job.sources),
                    "FAILED",
                    str(error),
                )
            )

    return results
