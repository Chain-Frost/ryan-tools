"""Terrain/AOI checks and deterministic domain generation."""

from dataclasses import dataclass
from pathlib import Path

import fiona
import numpy as np
import rasterio
from numpy.typing import NDArray
from rasterio.features import rasterize, shapes
from shapely.geometry import Polygon, box, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from .d8 import contributing_cells


@dataclass(frozen=True, slots=True)
class Domain:
    """Analysis geometry in DEM horizontal coordinates (metres)."""

    aoi: BaseGeometry
    contributing: BaseGeometry
    buffer: BaseGeometry
    active: BaseGeometry
    location: Polygon
    contributing_cells: NDArray[np.bool_]
    exclusions: BaseGeometry | None = None
    excluded_area: BaseGeometry | None = None


def read_aoi_features(path: Path, *, expected_crs: rasterio.crs.CRS) -> list[BaseGeometry]:
    """Read ordered AOI features without silently merging drainage identities."""
    with fiona.open(path) as src:
        if not src.crs_wkt or rasterio.crs.CRS.from_wkt(src.crs_wkt) != expected_crs:
            msg = "AOI CRS must match the DEM CRS; reproject explicitly"
            raise ValueError(msg)
        polygons: list[BaseGeometry] = []
        for feature in src:
            if feature.geometry is None:
                continue
            geom = shape(feature.geometry)
            if geom.geom_type not in {"Polygon", "MultiPolygon"} or not geom.is_valid:
                msg = "AOI must contain valid polygon geometries"
                raise ValueError(msg)
            if not geom.is_empty:
                polygons.append(geom)
    if not polygons:
        msg = "AOI has no polygon features"
        raise ValueError(msg)
    return polygons


def read_aoi(path: Path, *, expected_crs: rasterio.crs.CRS) -> BaseGeometry:
    """Read the union for the TUFLOW footprint; retain identities separately."""
    result = unary_union(read_aoi_features(path, expected_crs=expected_crs))
    if not result.is_valid or result.is_empty:
        msg = "Invalid AOI union"
        raise ValueError(msg)
    return result


def check_dem(dem: rasterio.io.DatasetReader) -> None:
    """Reject incompatible DEMs rather than silently warp input."""
    if dem.count != 1 or dem.crs is None or not dem.crs.is_projected:
        msg = "DEM must have one band and projected coordinate system"
        raise ValueError(msg)
    units, factor = dem.crs.linear_units_factor
    if not np.isclose(factor, 1.0):
        msg = f"DEM horizontal units must be metres; found {units}"
        raise ValueError(msg)
    if dem.width < 3 or dem.height < 3:
        msg = "DEM too small for watershed delineation"
        raise ValueError(msg)
    if dem.transform.b != 0 or dem.transform.d != 0:
        msg = "Rotated/skewed DEM not supported"
        raise ValueError(msg)
    if not (dem.res[0] > 0 and dem.res[1] > 0):
        msg = "Invalid DEM resolution"
        raise ValueError(msg)


def mask_to_polygon(mask: NDArray[np.bool_], transform: rasterio.Affine) -> BaseGeometry:
    """Polygonise selected raster cells and reject invalid/empty results."""
    if not np.any(mask):
        msg = "Cannot polygonise an empty raster mask"
        raise ValueError(msg)
    parts = [shape(g) for g, value in shapes(mask.astype(np.uint8), mask=mask, transform=transform) if value == 1]
    geom = unary_union(parts)
    if geom.is_empty or not geom.is_valid:
        msg = "Invalid polygonised raster mask"
        raise ValueError(msg)
    return geom


def make_domain(
    directions: NDArray[np.integer],
    *,
    aoi: BaseGeometry,
    transform: rasterio.Affine,
    raster_bounds: tuple[float, float, float, float],
    buffer_m: float = 500.0,
    exclusions: BaseGeometry | None = None,
    valid_cells: NDArray[np.bool_] | None = None,
) -> Domain:
    """Delineate all AOI contributing cells and union with circular AOI buffer."""
    if buffer_m <= 0 or not np.isfinite(buffer_m):
        msg = "AOI buffer must be finite and positive"
        raise ValueError(msg)
    if not box(*raster_bounds).covers(aoi):
        msg = "AOI must lie entirely inside DEM"
        raise ValueError(msg)
    seed = rasterize([(aoi, 1)], out_shape=directions.shape, transform=transform, fill=0, all_touched=True).astype(bool)
    upstream = contributing_cells(directions, seed, valid_cells=valid_cells)
    contributing = mask_to_polygon(upstream, transform)
    expanded = aoi.buffer(buffer_m)
    if not box(*raster_bounds).covers(expanded):
        msg = "AOI downstream buffer reaches DEM edge; supply larger terrain"
        raise ValueError(msg)
    full_active = unary_union([contributing, expanded])
    excluded_area: BaseGeometry | None = None
    if exclusions is not None:
        if exclusions.is_empty or not exclusions.is_valid:
            msg = "Exclusion polygons must be nonempty and valid"
            raise ValueError(msg)
        if exclusions.intersection(expanded).area > 1e-6:
            msg = "Upstream exclusions may not overlap AOI or its required circular buffer"
            raise ValueError(msg)
        excluded_area = exclusions.intersection(contributing)
        if excluded_area.is_empty or excluded_area.area <= 0:
            msg = "Exclusions do not intersect the contributing catchment"
            raise ValueError(msg)
    active = full_active.difference(excluded_area) if excluded_area is not None else full_active
    if not active.is_valid or active.is_empty or not active.covers(aoi):
        msg = "Invalid or incomplete active model polygon"
        raise ValueError(msg)
    xmin, ymin, xmax, ymax = active.bounds
    loc = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
    if not loc.is_valid or not loc.covers(active):
        msg = "Cardinal 2d_loc rectangle does not cover active area"
        raise ValueError(msg)
    if excluded_area is not None and active.intersection(excluded_area).area > 1e-6:
        msg = "Excluded upstream areas remain active"
        raise ValueError(msg)
    return Domain(aoi, contributing, expanded, active, loc, upstream, exclusions, excluded_area)


def detailed_analysis_cells(
    domain: Domain,
    *,
    transform: rasterio.Affine,
    analyse_exclusions: bool = False,
) -> NDArray[np.bool_]:
    """Select the stream/subcatchment analysis cells, independently of model code.

    Excluded terrain always remains in the *full* AOI contributing-area
    delineation. Unless explicitly requested, it is omitted from the detailed
    stream and stream-headwater analysis. Rasterise conservatively with
    all_touched=True so even partially excluded cells are not analysed.
    """
    cells = domain.contributing_cells.copy()
    if domain.excluded_area is None or analyse_exclusions:
        return cells
    excluded_cells = rasterize(
        [(domain.excluded_area, 1)],
        out_shape=cells.shape,
        transform=transform,
        fill=0,
        all_touched=True,
    )
    return cells & (excluded_cells == 0)
