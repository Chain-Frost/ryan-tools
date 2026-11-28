"""Physical catchment/cell intersection weights for geographic spatial IFD grids.

Intersect in the stated WGS84 lon/lat geometry before calculating signed
ellipsoidal areas. Densification approximates the straight lon/lat edges
(including parallels) by short geodesic segments. No equal-cell-area shortcut
or silent partial-extent normalisation is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import fsum, isclose, isfinite
from pathlib import Path

import fiona
import numpy as np
import numpy.typing as npt
from pyproj import Geod
from rasterio.crs import CRS
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, box, shape
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import orient
from shapely.ops import unary_union

from .spatial_ifd import GridSpec, SpatialIFDGrid
from .spatial_ifd_rasters import weighted_depth

# Explicit approximation controls, not BoM/ARR source-equivalence tolerances.
_EDGE_SEGMENT_DEGREES = 0.001
_AREA_CLOSURE_REL_TOL = 1e-6


@dataclass(frozen=True, slots=True)
class CatchmentWeights:
    """Grid-aligned intersection areas and independently calculated total area."""

    spec: GridSpec
    areas_m2: npt.NDArray[np.float64]
    catchment_area_m2: float
    covered_area_m2: float

    def mean_depth(self, grid: SpatialIFDGrid) -> float:
        if grid.spec != self.spec:
            msg = "Catchment weights and rainfall grid are not aligned"
            raise ValueError(msg)
        return weighted_depth(grid, self.areas_m2)

    def multipliers(self, grid: SpatialIFDGrid) -> npt.NDArray[np.float64]:
        """Normalize to the physical catchment mean (not the grid-cell mean)."""
        return grid.multipliers(self.mean_depth(grid))


def _ellipsoidal_area(geometry: BaseGeometry, geod: Geod) -> float:
    if isinstance(geometry, Polygon):
        if geometry.is_empty:
            return 0.0
        dense = geometry.segmentize(_EDGE_SEGMENT_DEGREES)
        if not isinstance(dense, Polygon):
            msg = "Unexpected densified geometry type"
            raise ValueError(msg)
        # A counter-clockwise exterior and clockwise holes ensure subtraction.
        area, _ = geod.geometry_area_perimeter(orient(dense, sign=1.0))
        return abs(float(area))
    if isinstance(geometry, (MultiPolygon, GeometryCollection)):
        return fsum(_ellipsoidal_area(part, geod) for part in geometry.geoms)
    # Intersection boundaries may include lines and points, with zero area.
    return 0.0


def _grid_extent(spec: GridSpec) -> Polygon:
    return box(
        float(spec.xllcorner),
        float(spec.yllcorner),
        float(spec.xllcorner + spec.cols * spec.spacing),
        float(spec.yllcorner + spec.rows * spec.spacing),
    )


def _validate_catchment(spec: GridSpec, catchment: Polygon | MultiPolygon) -> None:
    if not isinstance(catchment, (Polygon, MultiPolygon)) or catchment.is_empty or not catchment.is_valid:
        msg = "Catchment must be a valid nonempty Polygon or MultiPolygon"
        raise ValueError(msg)
    west, south, east, north = catchment.bounds
    if not all(isfinite(value) for value in (west, south, east, north)):
        msg = "Catchment bounds must be finite"
        raise ValueError(msg)
    if west < -180 or east > 180 or south <= -85 or north >= 85 or east - west >= 180:
        msg = "Catchment must use non-dateline-crossing WGS84 lon/lat coordinates away from the poles"
        raise ValueError(msg)
    if not _grid_extent(spec).covers(catchment):
        msg = "Grid does not cover the entire catchment; extend it before calculating the mean"
        raise ValueError(msg)


def catchment_weights(spec: GridSpec, catchment: Polygon | MultiPolygon) -> CatchmentWeights:
    """Calculate ellipsoidal areas of every cell/catchment intersection in m².

    Input coordinates must already be EPSG:4326, longitude then latitude, with
    no antimeridian crossing. The grid must cover the whole catchment. Holes,
    multipart geometries and fractional cells are supported. The adopted
    segment length and independent area-closure check are approximation
    controls; engineering acceptance of genuine polygons remains separate.
    """
    _validate_catchment(spec, catchment)
    geod = Geod(ellps="WGS84")
    total = _ellipsoidal_area(catchment, geod)
    areas = np.zeros((spec.rows, spec.cols), dtype=np.float64)
    for row in range(spec.rows):
        south = spec.yllcorner + (spec.rows - row - 1) * spec.spacing
        for col in range(spec.cols):
            west = spec.xllcorner + col * spec.spacing
            cell = box(float(west), float(south), float(west + spec.spacing), float(south + spec.spacing))
            areas[row, col] = _ellipsoidal_area(catchment.intersection(cell), geod)
    covered = float(np.sum(areas))
    if not isfinite(total) or total <= 0 or not isclose(covered, total, rel_tol=_AREA_CLOSURE_REL_TOL):
        msg = f"Catchment intersection area closure failed: total={total}, covered={covered} m²"
        raise ValueError(msg)
    return CatchmentWeights(spec, areas, total, covered)


def read_catchment_polygon(path: Path, *, layer: str | None = None) -> Polygon | MultiPolygon:
    """Read and explicitly union all polygon features of one WGS84 vector layer.

    Reject missing CRS, nonpolygon or invalid features and ambiguous layer
    selection. Reproject other datums/projected inputs explicitly before use.
    Use separate calls/inputs to retain distinct subcatchment identities.
    """
    if layer is None:
        layers = fiona.listlayers(path)
        if len(layers) != 1:
            msg = "Select one vector layer explicitly"
            raise ValueError(msg)
        layer = layers[0]
    polygons: list[Polygon | MultiPolygon] = []
    with fiona.open(path, layer=layer) as source:
        if not source.crs_wkt or CRS.from_wkt(source.crs_wkt) != CRS.from_epsg(4326):
            msg = "Catchment layer must have an explicit WGS84 (EPSG:4326) CRS"
            raise ValueError(msg)
        for feature in source:
            if feature.geometry is None:
                msg = "Catchment layer contains a missing geometry"
                raise ValueError(msg)
            geometry = shape(feature.geometry)
            if not isinstance(geometry, (Polygon, MultiPolygon)) or geometry.is_empty or not geometry.is_valid:
                msg = "Catchment layer must contain valid nonempty polygons only"
                raise ValueError(msg)
            polygons.append(geometry)
    result = unary_union(polygons)
    if not isinstance(result, (Polygon, MultiPolygon)) or result.is_empty or not result.is_valid:
        msg = "Catchment union must be a valid nonempty polygon"
        raise ValueError(msg)
    return result
