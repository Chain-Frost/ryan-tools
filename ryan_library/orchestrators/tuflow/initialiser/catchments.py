"""D8 AOI-entry catchments and genuinely longest connected flow paths.

One catchment is retained for each distinct exterior-to-AOI D8 crossing.
These are *incremental exterior* partitions for that AOI, not independent
hydrological basins or per-stream-reach subcatchments. AOIs may overlap in
their cumulative upstream coverage (e.g. nested AOIs).
"""

import math
from collections import deque
from dataclasses import dataclass
from itertools import pairwise

import numpy as np
import rasterio
from numpy.typing import NDArray
from rasterio.features import rasterize, shapes
from rasterio.transform import rowcol
from rasterio.windows import Window
from shapely.geometry import Point, box, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from .d8 import OFFSETS, contributing_cells, next_cell
from .profiles import equal_area_slope

type Cell = tuple[int, int]


@dataclass(frozen=True, slots=True)
class InflowCatchment:
    """A D8 entry crossing and the exterior cells attributed to that crossing."""

    aoi_id: int
    catchment_id: int
    inlet: Cell
    outlet: Cell
    polygon: BaseGeometry
    local_area_m2: float
    aoi_cumulative_area_m2: float
    min_elevation_m: float
    max_elevation_m: float
    mean_elevation_m: float
    path: tuple[Cell, ...]
    length_m: float | None
    slope_ea: float | None
    source_elevation_m: float | None
    outlet_elevation_m: float | None
    status: str


@dataclass(frozen=True, slots=True)
class OutletCatchment:
    """One complete or deliberately clipped catchment draining to a point."""

    interest_id: int
    outlet: Cell
    polygon: BaseGeometry
    area_m2: float
    min_elevation_m: float
    max_elevation_m: float
    mean_elevation_m: float
    status: str
    snap_distance_m: float


def _snapped_cell(
    point: Point,
    valid_cells: NDArray[np.bool_],
    accumulation: NDArray[np.floating],
    *,
    transform: rasterio.Affine,
    snap_distance_m: float,
) -> tuple[Cell, float]:
    row_value, col_value = rowcol(transform, point.x, point.y)
    row, col = int(row_value), int(col_value)
    height, width = valid_cells.shape
    if not (0 <= row < height and 0 <= col < width):
        msg = "Interest point falls outside the analysis bounds"
        raise ValueError(msg)
    radius = math.ceil(snap_distance_m / min(abs(transform.a), abs(transform.e)))
    candidates: list[tuple[float, float, int, int]] = []
    for candidate_row in range(max(0, row - radius), min(height, row + radius + 1)):
        for candidate_col in range(max(0, col - radius), min(width, col + radius + 1)):
            if not valid_cells[candidate_row, candidate_col]:
                continue
            x = transform.c + (candidate_col + 0.5) * transform.a
            y = transform.f + (candidate_row + 0.5) * transform.e
            distance = math.hypot(x - point.x, y - point.y)
            if distance <= snap_distance_m or (radius == 0 and candidate_row == row and candidate_col == col):
                score = abs(float(accumulation[candidate_row, candidate_col]))
                if math.isfinite(score):
                    candidates.append((-score, distance, candidate_row, candidate_col))
    if not candidates:
        msg = f"No valid D8 cell within {snap_distance_m:g} m of interest point"
        raise ValueError(msg)
    _, distance, snapped_row, snapped_col = min(candidates)
    return (snapped_row, snapped_col), distance


def _partial_status(cells: NDArray[np.bool_], valid_cells: NDArray[np.bool_]) -> str:
    reasons: list[str] = []
    if bool(np.any(cells[0, :]) or np.any(cells[-1, :]) or np.any(cells[:, 0]) or np.any(cells[:, -1])):
        reasons.append("analysis_edge")
    height, width = cells.shape
    for dr, dc in OFFSETS.values():
        rows = slice(max(0, -dr), min(height, height - dr))
        cols = slice(max(0, -dc), min(width, width - dc))
        neighbour_rows = slice(max(0, dr), min(height, height + dr))
        neighbour_cols = slice(max(0, dc), min(width, width + dc))
        if np.any(cells[rows, cols] & ~valid_cells[neighbour_rows, neighbour_cols]):
            reasons.append("nodata_edge")
            break
    return "complete" if not reasons else "partial_" + "_and_".join(reasons)


def delineate_outlet_catchments(
    directions: NDArray[np.integer],
    elevations: NDArray[np.floating],
    accumulation: NDArray[np.floating],
    outlets: list[BaseGeometry],
    *,
    transform: rasterio.Affine,
    valid_cells: NDArray[np.bool_],
    snap_distance_m: float = 0,
    allow_partial: bool = False,
    clip_bounds: tuple[float, float, float, float] | None = None,
) -> list[OutletCatchment]:
    """Delineate one upstream D8 catchment for each point interest feature."""
    if directions.shape != elevations.shape or directions.shape != accumulation.shape:
        msg = "D8, elevation and accumulation grids must match"
        raise ValueError(msg)
    if valid_cells.shape != directions.shape:
        msg = "Validity mask must match D8 grid"
        raise ValueError(msg)
    if snap_distance_m < 0:
        msg = "Outlet snap distance cannot be negative"
        raise ValueError(msg)
    result: list[OutletCatchment] = []
    cell_area = abs(transform.a * transform.e)
    for interest_id, geometry in enumerate(outlets, start=1):
        if not isinstance(geometry, Point):
            msg = "Outlet catchments require point interest geometries"
            raise TypeError(msg)
        outlet, snapped_distance = _snapped_cell(
            geometry,
            valid_cells,
            accumulation,
            transform=transform,
            snap_distance_m=snap_distance_m,
        )
        seed = np.zeros(directions.shape, dtype=bool)
        seed[outlet] = True
        cells = contributing_cells(
            directions,
            seed,
            allow_unknown_boundary=allow_partial,
            require_complete=not allow_partial,
            valid_cells=valid_cells,
        )
        rows, cols = np.nonzero(cells)
        row_min, row_max = int(rows.min()), int(rows.max()) + 1
        col_min, col_max = int(cols.min()), int(cols.max()) + 1
        subset = cells[row_min:row_max, col_min:col_max]
        subset_transform = rasterio.windows.transform(
            Window(col_min, row_min, col_max - col_min, row_max - row_min), transform
        )
        parts = [
            shape(geometry)
            for geometry, value in shapes(subset.astype(np.uint8), mask=subset, transform=subset_transform)
            if value
        ]
        polygon = unary_union(parts)
        if clip_bounds is not None:
            polygon = polygon.intersection(box(*clip_bounds))
        if polygon.is_empty or not polygon.is_valid:
            msg = "Invalid point-outlet catchment polygon"
            raise ValueError(msg)
        values = np.asarray(elevations[cells & valid_cells], dtype=np.float64)
        result.append(
            OutletCatchment(
                interest_id=interest_id,
                outlet=outlet,
                polygon=polygon,
                area_m2=(float(polygon.area) if clip_bounds is not None else int(np.count_nonzero(cells)) * cell_area),
                min_elevation_m=float(np.min(values)),
                max_elevation_m=float(np.max(values)),
                mean_elevation_m=float(np.mean(values)),
                status=_partial_status(cells, valid_cells),
                snap_distance_m=snapped_distance,
            )
        )
    return result


def _partition(
    directions: NDArray[np.integer],
    seeds: NDArray[np.bool_],
    contributing: NDArray[np.bool_],
) -> tuple[NDArray[np.int32], list[tuple[Cell, Cell]]]:
    """Assign each upstream exterior cell to its first D8 entry into an AOI."""
    crossings: list[tuple[Cell, Cell]] = []
    for r, c in zip(*np.nonzero(contributing & ~seeds), strict=True):
        cell = int(r), int(c)
        downstream = next_cell(directions, *cell)
        if downstream is not None and seeds[downstream]:
            crossings.append((cell, downstream))
    crossings.sort()
    if not crossings:
        return np.zeros(directions.shape, dtype=np.int32), []
    if len(crossings) > 10000:
        msg = "More than 10000 AOI D8 entry edges; refine AOI geometry before detailed analysis"
        raise ValueError(msg)
    entries = {cell: index for index, (cell, _) in enumerate(crossings, 1)}
    owner = np.zeros(directions.shape, dtype=np.int32)
    for r, c in zip(*np.nonzero(contributing & ~seeds), strict=True):
        start: Cell = int(r), int(c)
        if owner[start]:
            continue
        chain: list[Cell] = []
        current = start
        while not owner[current]:
            if current in entries:
                owner[current] = entries[current]
                break
            chain.append(current)
            downstream = next_cell(directions, *current)
            if downstream is None or not contributing[downstream] or seeds[downstream]:
                msg = "Outside contributing cell has no attributable AOI entry"
                raise ValueError(msg)
            current = downstream
            if len(chain) > directions.size:
                msg = "D8 cycle in entry attribution"
                raise ValueError(msg)
        label = owner[current]
        for item in chain:
            owner[item] = label
    if np.any((owner > 0) != (contributing & ~seeds)):
        msg = "AOI entry partition contains gaps or overlaps"
        raise ValueError(msg)
    return owner, crossings


def _farthest_routes(
    directions: NDArray[np.integer],
    owner: NDArray[np.int32],
    transform: rasterio.Affine,
) -> tuple[NDArray[np.int32], NDArray[np.float64]]:
    """Topological longest-distance DP, not maximum contributing accumulation."""
    indegree = np.zeros(directions.shape, dtype=np.uint8)
    for r, c in zip(*np.nonzero(owner), strict=True):
        row, col = int(r), int(c)
        down = next_cell(directions, row, col)
        if down is not None and owner[down] == owner[row, col]:
            indegree[down] += 1
    queue: deque[Cell] = deque(
        (int(r), int(c)) for r, c in zip(*np.nonzero((owner > 0) & (indegree == 0)), strict=True)
    )
    best = np.zeros(directions.shape, dtype=np.float64)
    upstream_ptr = np.full(directions.shape, -1, dtype=np.int32)
    processed = 0
    width = directions.shape[1]
    while queue:
        row, col = queue.popleft()
        processed += 1
        down = next_cell(directions, row, col)
        if down is None or owner[down] != owner[row, col]:
            continue
        dr, dc = down[0] - row, down[1] - col
        candidate = best[row, col] + math.hypot(dc * transform.a, dr * transform.e)
        previous = int(upstream_ptr[down])
        candidate_index = row * width + col
        if candidate > best[down] + 1e-10 or (
            math.isclose(candidate, best[down], rel_tol=0, abs_tol=1e-10)
            and (previous < 0 or candidate_index < previous)
        ):
            best[down] = candidate
            upstream_ptr[down] = candidate_index
        indegree[down] -= 1
        if indegree[down] == 0:
            queue.append(down)
    if processed != int(np.count_nonzero(owner)):
        msg = "Cycle or incomplete D8 topological processing"
        raise ValueError(msg)
    return upstream_ptr, best


def _longest_path(
    root: Cell,
    outlet: Cell,
    upstream_ptr: NDArray[np.int32],
    best: NDArray[np.float64],
    elevations: NDArray[np.floating],
    transform: rasterio.Affine,
) -> tuple[tuple[Cell, ...], float, float]:
    width = upstream_ptr.shape[1]
    reverse = [root]
    at = root
    while int(upstream_ptr[at]) >= 0:
        flat = int(upstream_ptr[at])
        at = flat // width, flat % width
        reverse.append(at)
        if len(reverse) > upstream_ptr.size:
            msg = "Invalid longest-path upstream pointers"
            raise ValueError(msg)
    route = (*reversed(reverse), outlet)
    first = math.hypot((root[1] - outlet[1]) * transform.a, (root[0] - outlet[0]) * transform.e)
    length = float(best[root]) + first
    downstream_first = tuple(reversed(route))
    distances = [0.0]
    for left, right in pairwise(downstream_first):
        distances.append(
            distances[-1] + math.hypot((left[1] - right[1]) * transform.a, (left[0] - right[0]) * transform.e)
        )
    heights = np.asarray([float(elevations[cell]) for cell in downstream_first], dtype=np.float64)
    if not np.all(np.isfinite(heights)):
        msg = "Nonfinite DEM values on longest flow path"
        raise ValueError(msg)
    slope = equal_area_slope(np.asarray(distances, dtype=np.float64), heights)
    if not math.isclose(distances[-1], length, rel_tol=1e-9, abs_tol=1e-7):
        msg = "Longest flow path length failed independent reconstruction"
        raise ValueError(msg)
    return route, length, slope


def delineate_inflows(
    directions: NDArray[np.integer],
    elevations: NDArray[np.floating],
    aois: list[BaseGeometry],
    *,
    transform: rasterio.Affine,
    analyse_cells: NDArray[np.bool_] | None = None,
    valid_cells: NDArray[np.bool_] | None = None,
) -> list[InflowCatchment]:
    """Derive distinct AOI D8 crossings, exterior areas and each longest path.

    Inputs are AOI features in file order (stable 1-based aoi_id). Every
    exterior cell contributing to an AOI is assigned exactly once *per AOI*.
    Catchment IDs are stable in AOI, row and column ordering. Exclusions
    retain catchment geometry but suppress a potentially incomplete LFP.
    """
    if directions.shape != elevations.shape or directions.ndim != 2:
        msg = "D8 and elevation grids must match"
        raise ValueError(msg)
    if analyse_cells is not None and analyse_cells.shape != directions.shape:
        msg = "Detailed-analysis mask must match D8 grid"
        raise ValueError(msg)
    if valid_cells is not None and valid_cells.shape != directions.shape:
        msg = "Validity mask must match D8 grid"
        raise ValueError(msg)
    if not aois:
        msg = "At least one AOI geometry is required"
        raise ValueError(msg)
    area = abs(transform.a * transform.e)
    result: list[InflowCatchment] = []
    for aoi_id, aoi in enumerate(aois, start=1):
        seed = rasterize(
            [(aoi, 1)],
            out_shape=directions.shape,
            transform=transform,
            all_touched=True,
        ).astype(bool)
        cumulative = contributing_cells(directions, seed, valid_cells=valid_cells)
        owner, crossings = _partition(directions, seed, cumulative)
        if not crossings:
            continue
        parts: dict[int, list[BaseGeometry]] = {}
        for geom, label in shapes(owner, mask=owner > 0, transform=transform):
            parts.setdefault(int(label), []).append(shape(geom))
        pointers, distances = _farthest_routes(directions, owner, transform)
        for label, (inlet, outlet) in enumerate(crossings, start=1):
            cells = owner == label
            if label not in parts or not np.any(cells):
                msg = "Missing polygon for D8 inlet partition"
                raise ValueError(msg)
            polygon = unary_union(parts[label])
            if polygon.is_empty or not polygon.is_valid:
                msg = "Invalid AOI inlet subcatchment polygon"
                raise ValueError(msg)
            cell_count = int(np.count_nonzero(cells))
            if not math.isclose(polygon.area, cell_count * area, rel_tol=1e-8, abs_tol=1e-6):
                msg = "D8 inlet polygon does not conserve raster area"
                raise ValueError(msg)
            values = np.asarray(elevations[cells], dtype=np.float64)
            if not np.all(np.isfinite(values)):
                msg = "Nonfinite contributing-catchment elevations"
                raise ValueError(msg)
            status = "complete"
            route: tuple[Cell, ...] = ()
            length: float | None = None
            slope: float | None = None
            if analyse_cells is not None and not bool(np.all(analyse_cells[cells])):
                status = "excluded_from_detailed_analysis"
            else:
                route, length, slope = _longest_path(inlet, outlet, pointers, distances, elevations, transform)
            result.append(
                InflowCatchment(
                    aoi_id=aoi_id,
                    catchment_id=len(result) + 1,
                    inlet=inlet,
                    outlet=outlet,
                    polygon=polygon,
                    local_area_m2=cell_count * area,
                    aoi_cumulative_area_m2=int(np.count_nonzero(cumulative)) * area,
                    min_elevation_m=float(np.min(values)),
                    max_elevation_m=float(np.max(values)),
                    mean_elevation_m=float(np.mean(values)),
                    path=route,
                    length_m=length,
                    slope_ea=slope,
                    source_elevation_m=float(elevations[route[0]]) if route else None,
                    outlet_elevation_m=float(elevations[route[-1]]) if route else None,
                    status=status,
                )
            )
    return result
