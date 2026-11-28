"""Raster interchange and reference comparisons for point-sampled spatial IFDs."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from decimal import Decimal
from math import isfinite
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import rasterio

from .spatial_ifd import ARR_ENDPOINT, GridSpec, SpatialIFDGrid


class _Dataset(Protocol):
    width: int
    height: int
    count: int
    transform: rasterio.Affine
    crs: object | None

    def read(self, index: int) -> npt.NDArray[np.generic]: ...

    def read_masks(self, index: int) -> npt.NDArray[np.uint8]: ...

    def write(self, values: npt.NDArray[np.float64], index: int) -> None: ...

    def update_tags(self, **tags: str) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ReferenceGrid:
    spec: GridSpec
    depths_mm: npt.NDArray[np.float64]
    valid: npt.NDArray[np.bool_]


@dataclass(frozen=True, slots=True)
class Comparison:
    compared_cells: int
    reference_nodata_cells: int
    mean_bias_mm: float
    max_absolute_mm: float
    mean_absolute_mm: float
    rmse_mm: float
    max_absolute_percent: float


def _decimal_coordinate(value: float) -> Decimal:
    # Raster transforms use binary floats. Remove only sub-nanodegree noise
    # from the coordinate representation, not a whole fraction of a grid cell.
    if not isfinite(value):
        msg = "Raster transform must be finite"
        raise ValueError(msg)
    return Decimal(str(round(value, 10)))


def _reference_spec(dataset: _Dataset, *, assume_wgs84: bool) -> GridSpec:
    crs = dataset.crs
    if str(crs) != "EPSG:4326" and not (crs is None and assume_wgs84):
        msg = "Reference must use WGS84 (EPSG:4326); explicitly confirm the datum for a CRS-less ASCII grid"
        raise ValueError(msg)
    transform = dataset.transform
    if transform.b != 0 or transform.d != 0 or transform.a <= 0 or transform.e != -transform.a:
        msg = "Reference must have a north-up, unrotated, square angular grid"
        raise ValueError(msg)
    spacing = _decimal_coordinate(transform.a)
    return GridSpec(
        _decimal_coordinate(transform.f) - spacing / 2,
        _decimal_coordinate(transform.c) + spacing / 2,
        dataset.height,
        dataset.width,
        spacing,
    )


def read_reference_raster(path: Path, *, assume_wgs84: bool = False) -> ReferenceGrid:
    """Read a single-band, geographic depth raster with explicit datum and NoData handling.

    Caller verifies the event, units (mm) and original raster provenance. No
    reprojection or resampling is performed. A missing CRS is never assumed by
    default. Projected and other-datum rasters require separate reconciliation.
    """
    open_raster = cast("Callable[[Path], _Dataset]", rasterio.open)
    with closing(open_raster(path)) as dataset:
        if dataset.count != 1:
            msg = "Reference must have one band"
            raise ValueError(msg)
        spec = _reference_spec(dataset, assume_wgs84=assume_wgs84)
        depths = np.asarray(dataset.read(1), dtype=np.float64)
        valid = dataset.read_masks(1) != 0
    _validate_depths(spec, depths, valid)
    return ReferenceGrid(spec, depths, valid)


def _validate_depths(spec: GridSpec, values: npt.NDArray[np.float64], valid: npt.NDArray[np.bool_]) -> None:
    shape = (spec.rows, spec.cols)
    if values.shape != shape or valid.shape != shape or not valid.any():
        msg = "Grid dimensions must agree and at least one valid cell is required"
        raise ValueError(msg)
    selected = values[valid]
    if not np.isfinite(selected).all() or np.any(selected <= 0):
        msg = "Valid rainfall depths must be finite and positive"
        raise ValueError(msg)


def compare_reference(grid: SpatialIFDGrid, reference: ReferenceGrid) -> Comparison:
    """Compare exact matching cells, reporting excluded reference NoData explicitly.

    The caller must independently verify that the reference is for the same
    event and source baseline. No numerical acceptance tolerance is inferred.
    A reference with NoData only establishes comparison on its valid footprint.
    """
    if grid.spec != reference.spec:
        msg = "Reference and candidate grids are not exactly aligned"
        raise ValueError(msg)
    _validate_depths(reference.spec, reference.depths_mm, reference.valid)
    _validate_depths(grid.spec, grid.depths_mm, np.ones(grid.depths_mm.shape, dtype=np.bool_))
    observed = reference.depths_mm[reference.valid]
    difference = grid.depths_mm[reference.valid] - observed
    absolute = np.abs(difference)
    return Comparison(
        int(observed.size),
        int(reference.valid.size - observed.size),
        float(np.mean(difference)),
        float(np.max(absolute)),
        float(np.mean(absolute)),
        float(np.sqrt(np.mean(np.square(difference)))),
        float(np.max(absolute / observed) * 100),
    )


def weighted_depth(grid: SpatialIFDGrid, areas_m2: npt.NDArray[np.float64]) -> float:
    """Mean rainfall over supplied physical intersection areas in square metres.

    Zero weights exclude cells. This does not derive areas from geographic cell
    counts; callers must compute actual catchment/cell intersection areas.
    """
    if areas_m2.shape != grid.depths_mm.shape or not np.isfinite(areas_m2).all() or np.any(areas_m2 < 0):
        msg = "Areas must match the grid and be finite and nonnegative"
        raise ValueError(msg)
    valid = areas_m2 > 0
    _validate_depths(grid.spec, grid.depths_mm, valid)
    weights = areas_m2[valid]
    weights = weights / np.max(weights)
    return float(np.average(grid.depths_mm[valid], weights=weights))


def write_geotiff(path: Path, grid: SpatialIFDGrid, *, reference_depth_mm: float | None = None) -> None:
    """Export depth or multiplier with explicit WGS84 CRS and provenance tags.

    Stage a complete single-file GeoTIFF beside the destination, then publish
    via a hard link (atomic no-clobber). A filesystem without hard-link support
    fails closed. No sidecars or existing files are replaced.
    """
    spec = grid.spec
    values = grid.depths_mm if reference_depth_mm is None else grid.multipliers(reference_depth_mm)
    _validate_depths(spec, values, np.ones(values.shape, dtype=np.bool_))
    if path.suffix.lower() not in {".tif", ".tiff"}:
        msg = "GeoTIFF output must have a .tif or .tiff extension"
        raise ValueError(msg)
    if path.exists() or path.is_symlink():
        raise FileExistsError(path)
    transform = rasterio.Affine(
        float(spec.spacing),
        0.0,
        float(spec.xllcorner),
        0.0,
        -float(spec.spacing),
        float(spec.north_lat + spec.spacing / 2),
    )
    with TemporaryDirectory(dir=path.parent) as directory:
        staged = Path(directory) / "grid.tif"
        dataset = cast(
            "_Dataset",
            rasterio.open(
                staged,
                "w",
                driver="GTiff",
                width=spec.cols,
                height=spec.rows,
                count=1,
                dtype="float64",
                crs="EPSG:4326",
                transform=transform,
                compress="deflate",
            ),
        )
        with closing(dataset):
            dataset.write(values, 1)
            dataset.update_tags(
                IFD_DATASET_PATH=json.dumps(grid.source.table_path),
                IFD_CACHE_SNAPSHOT=grid.source.snapshot,
                IFD_SOURCE_ENDPOINT=ARR_ENDPOINT,
                IFD_SOURCE_STATUS="point-sampled; BoM raster equivalence not established",
                IFD_DURATION_MIN=str(grid.duration_min),
                IFD_AEP_PERCENT=str(grid.aep_percent),
                IFD_UNITS="mm" if reference_depth_mm is None else "dimensionless",
                IFD_REFERENCE_DEPTH_MM="" if reference_depth_mm is None else str(reference_depth_mm),
                IFD_ATTRIBUTION="ARR Data Hub; BoM 2016 IFD material: Commonwealth of Australia 2016, CC BY 4.0",
            )
        path.hardlink_to(staged)
