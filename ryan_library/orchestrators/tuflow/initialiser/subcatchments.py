"""D8-conserving local subcatchments for every extracted channel reach.

Reach-local areas are disjoint; downstream cumulative D8 areas overlap by
definition and must never be summed as though they were local areas. Direct
AOI drainage without a threshold stream is retained as an explicit residual.
"""

import math
from collections import deque
from dataclasses import dataclass
from itertools import pairwise

import numpy as np
import rasterio
from numpy.typing import NDArray
from rasterio.features import rasterize, shapes
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from .d8 import incoming, next_cell
from .profiles import equal_area_slope, stream_reaches

type Cell = tuple[int, int]


@dataclass(frozen=True, slots=True)
class ReachSubcatchment:
    """Disjoint local D8 cells and attributes for a reach or AOI direct runoff."""

    subcatchment_id: int
    kind: str
    reach_id: int | None
    aoi_id: int | None
    downstream_subcatchment_id: int | None
    downstream_reach_id: int | None
    outlet: Cell | None
    polygon: BaseGeometry
    local_area_m2: float
    full_local_area_m2: float
    cumulative_area_m2: float | None
    min_elevation_m: float | None
    max_elevation_m: float | None
    mean_elevation_m: float | None
    mean_terrain_slope: float | None
    stream_length_m: float | None
    stream_slope_ea: float | None
    profile_status: str
    reach_cells: tuple[Cell, ...]


@dataclass(frozen=True, slots=True)
class SubcatchmentResult:
    """QA-ready per-reach polygons and conserved analysis-cell coverage."""

    subcatchments: list[ReachSubcatchment]
    analysed_cell_count: int
    excluded_cell_count: int
    full_contributing_cell_count: int
    channel_reach_count: int


def _terrain_slope(terrain: NDArray[np.floating], transform: rasterio.Affine) -> NDArray[np.float64]:
    """Finite-difference slope magnitude; finite one-sided gradients at NoData."""
    if transform.b != 0 or transform.d != 0 or transform.a == 0 or transform.e == 0:
        msg = "Terrain slope requires a north-up, non-degenerate raster"
        raise ValueError(msg)
    z = np.asarray(terrain, dtype=np.float64)
    valid = np.isfinite(z)
    left = np.full_like(z, np.nan)
    right = np.full_like(z, np.nan)
    above = np.full_like(z, np.nan)
    below = np.full_like(z, np.nan)
    left[:, 1:] = z[:, :-1]
    right[:, :-1] = z[:, 1:]
    above[1:, :] = z[:-1, :]
    below[:-1, :] = z[1:, :]

    def gradient(before: NDArray[np.float64], after: NDArray[np.float64], spacing: float) -> NDArray[np.float64]:
        both = np.isfinite(before) & np.isfinite(after)
        return np.where(
            both,
            (after - before) / (2 * spacing),
            np.where(
                np.isfinite(after), (after - z) / spacing, np.where(np.isfinite(before), (z - before) / spacing, 0)
            ),
        )

    gx = gradient(left, right, abs(transform.a))
    gy = gradient(above, below, abs(transform.e))
    values = np.hypot(gx, gy)
    values[~valid] = np.nan
    return values


def _upstream_sizes(directions: NDArray[np.integer], contributing: NDArray[np.bool_]) -> NDArray[np.int64]:
    """Count cumulative D8 cells independently of stream and AOI thresholds."""
    degree = np.zeros(directions.shape, dtype=np.uint8)
    sizes = contributing.astype(np.int64)
    for row, col in zip(*np.nonzero(contributing), strict=True):
        down = next_cell(directions, int(row), int(col))
        if down is not None and contributing[down]:
            degree[down] += 1
    queue: deque[Cell] = deque(
        (int(row), int(col)) for row, col in zip(*np.nonzero(contributing & (degree == 0)), strict=True)
    )
    visited = 0
    while queue:
        cell = queue.popleft()
        visited += 1
        down = next_cell(directions, *cell)
        if down is not None and contributing[down]:
            sizes[down] += sizes[cell]
            degree[down] -= 1
            if degree[down] == 0:
                queue.append(down)
    if visited != int(np.count_nonzero(contributing)):
        msg = "D8 contributing catchment has a cycle or unresolved connectivity"
        raise ValueError(msg)
    return sizes


def _stream_reach_owners(
    directions: NDArray[np.integer], stream: NDArray[np.bool_]
) -> tuple[NDArray[np.int32], list[tuple[Cell, ...]]]:
    """Preserve existing stream_reaches IDs; add any isolated/terminal cells."""
    paths = [tuple(p) for p in stream_reaches(directions, stream)]
    owners = np.zeros(directions.shape, dtype=np.int32)
    for reach_id, path in enumerate(paths, start=1):
        for cell in path[:-1]:
            if owners[cell]:
                msg = "Stream cell belongs to multiple channel reaches"
                raise ValueError(msg)
            owners[cell] = reach_id

    # Keep the last (degree-one) terminal stream cell with its incoming reach.
    for reach_id, path in enumerate(paths, start=1):
        terminal = path[-1]
        if owners[terminal]:
            continue
        following = next_cell(directions, *terminal)
        if (following is None or not stream[following]) and sum(
            bool(stream[c]) for c in incoming(directions, *terminal)
        ) == 1:
            owners[terminal] = reach_id

    # A confluence with no downstream reach, or a one-cell stream, needs an
    # explicit singleton to conserve area without inventing a line segment.
    for row, col in zip(*np.nonzero(stream & (owners == 0)), strict=True):
        cell = int(row), int(col)
        paths.append((cell,))
        owners[cell] = len(paths)
    if np.any(stream & (owners == 0)) or np.any((owners > 0) & ~stream):
        msg = "Incomplete stream-to-reach attribution"
        raise ValueError(msg)
    return owners, paths


def _assign_owners(
    directions: NDArray[np.integer],
    contributing: NDArray[np.bool_],
    channels: NDArray[np.int32],
    aois: list[BaseGeometry],
    transform: rasterio.Affine,
    reach_count: int,
) -> NDArray[np.int32]:
    """Trace upstream to the first stream reach or otherwise direct AOI runoff."""
    owner = channels.copy()
    # AOI-direct IDs follow all reach IDs. A cell shared by multiple AOI
    # polygons is assigned to the first feature in file order.
    for aoi_id, polygon in enumerate(aois, start=1):
        seeds = rasterize(
            [(polygon, 1)], out_shape=directions.shape, transform=transform, all_touched=True, fill=0
        ).astype(bool)
        owner[seeds & contributing & (owner == 0)] = reach_count + aoi_id

    queue: deque[Cell] = deque((int(row), int(col)) for row, col in zip(*np.nonzero(owner > 0), strict=True))
    while queue:
        cell = queue.popleft()
        for upstream in incoming(directions, *cell):
            if contributing[upstream] and owner[upstream] == 0:
                owner[upstream] = owner[cell]
                queue.append(upstream)

    if np.any(contributing & (owner == 0)) or np.any((owner > 0) & ~contributing):
        msg = "Subcatchment attribution missed contributing raster cells"
        raise ValueError(msg)
    return owner


def _path_profile(
    path: tuple[Cell, ...], terrain: NDArray[np.floating], transform: rasterio.Affine
) -> tuple[float | None, float | None]:
    if len(path) < 2:
        return 0.0, None
    downstream_first = tuple(reversed(path))
    cumulative = [0.0]
    for left, right in pairwise(downstream_first):
        cumulative.append(
            cumulative[-1] + math.hypot((left[1] - right[1]) * transform.a, (left[0] - right[0]) * transform.e)
        )
    heights = np.asarray([float(terrain[cell]) for cell in downstream_first], dtype=np.float64)
    if not np.all(np.isfinite(heights)):
        msg = "Nonfinite DEM on channel reach profile"
        raise ValueError(msg)
    return cumulative[-1], equal_area_slope(np.asarray(cumulative, dtype=np.float64), heights)


def _verify_inputs(
    directions: NDArray[np.integer],
    accumulation: NDArray[np.floating],
    contributing: NDArray[np.bool_],
    detailed: NDArray[np.bool_],
    stream: NDArray[np.bool_],
    terrain: NDArray[np.floating],
    aois: list[BaseGeometry],
    transform: rasterio.Affine,
) -> None:
    """Reject incompatible D8 rasters or invalid subcatchment coverage."""
    if not aois or any(
        array.shape != directions.shape for array in (accumulation, contributing, detailed, stream, terrain)
    ):
        msg = "Subcatchment rasters must match and at least one AOI is required"
        raise ValueError(msg)
    if directions.ndim != 2 or not np.all(np.isfinite(accumulation[contributing])):
        msg = "Invalid D8 or upstream accumulation for subcatchments"
        raise ValueError(msg)
    if np.any(detailed & ~contributing) or np.any(stream & ~detailed):
        msg = "Stream and detailed masks must be within full contributing area"
        raise ValueError(msg)
    if transform.b != 0 or transform.d != 0 or transform.a <= 0 or transform.e >= 0:
        msg = "Subcatchments require a north-up, metre-based D8 grid"
        raise ValueError(msg)


def _reach_attributes(
    path: tuple[Cell, ...],
    reach_id: int,
    owners: NDArray[np.int32],
    cumulative: NDArray[np.int64],
    directions: NDArray[np.integer],
    terrain: NDArray[np.floating],
    transform: rasterio.Affine,
    cell_area: float,
) -> tuple[Cell, int | None, float, float | None, float | None]:
    """Resolve a reach's upstream area at its final owned cell and its successor."""
    if not path:
        msg = "Channel reach has no D8 cells"
        raise ValueError(msg)
    owned_path = [cell for cell in path if owners[cell] == reach_id]
    if not owned_path:
        msg = "Channel reach has no uniquely attributed D8 cells"
        raise ValueError(msg)
    outlet = owned_path[-1]
    upstream_area = float(cumulative[outlet]) * cell_area
    terminal = path[-1]
    downstream: int | None = None
    if owners[terminal] != reach_id:
        downstream = int(owners[terminal])
    else:
        following = next_cell(directions, *terminal)
        if following is not None and owners[following] not in (0, reach_id):
            downstream = int(owners[following])
    length, slope = _path_profile(path, terrain, transform)
    return outlet, downstream, upstream_area, length, slope


def _make_record(
    subcatchment_id: int,
    *,
    full_cells: NDArray[np.bool_],
    detailed_cells: NDArray[np.bool_],
    polygon: BaseGeometry,
    number: int,
    reach_count: int,
    paths: list[tuple[Cell, ...]],
    owners: NDArray[np.int32],
    cumulative: NDArray[np.int64],
    directions: NDArray[np.integer],
    terrain: NDArray[np.floating],
    slope_grid: NDArray[np.float64],
    transform: rasterio.Affine,
    cell_area: float,
) -> ReachSubcatchment:
    """Build one local basin's tested attributes and reach connectivity."""
    values = np.asarray(terrain[detailed_cells], dtype=np.float64)
    gradients = slope_grid[detailed_cells]
    if not np.all(np.isfinite(values)) or not np.all(np.isfinite(gradients)):
        msg = "Detailed subcatchment has invalid DEM elevation or slope"
        raise ValueError(msg)
    reach_id = subcatchment_id if subcatchment_id <= reach_count else None
    aoi_id = subcatchment_id - reach_count if reach_id is None else None
    path: tuple[Cell, ...] = paths[reach_id - 1] if reach_id is not None else ()
    downstream: int | None = None
    outlet: Cell | None = None
    upstream_area: float | None = None
    length: float | None = None
    slope: float | None = None
    if reach_id is not None:
        outlet, downstream, upstream_area, length, slope = _reach_attributes(
            path, reach_id, owners, cumulative, directions, terrain, transform, cell_area
        )
    full_count = int(np.count_nonzero(full_cells))
    if full_count != number:
        status = "partial_exclusion"
    elif reach_id is None:
        status = "aoi_direct_without_threshold_stream"
    elif length == 0:
        status = "singleton_stream_cell"
    else:
        status = "complete"
    return ReachSubcatchment(
        subcatchment_id=subcatchment_id,
        kind="channel_reach" if reach_id is not None else "aoi_direct",
        reach_id=reach_id,
        aoi_id=aoi_id,
        downstream_subcatchment_id=downstream,
        downstream_reach_id=downstream if downstream is not None and downstream <= reach_count else None,
        outlet=outlet,
        polygon=polygon,
        local_area_m2=number * cell_area,
        full_local_area_m2=full_count * cell_area,
        cumulative_area_m2=upstream_area,
        min_elevation_m=float(np.min(values)),
        max_elevation_m=float(np.max(values)),
        mean_elevation_m=float(np.mean(values)),
        mean_terrain_slope=float(np.mean(gradients)),
        stream_length_m=length,
        stream_slope_ea=slope,
        profile_status=status,
        reach_cells=path,
    )


def _validate_downstream_links(items: list[ReachSubcatchment], reach_count: int) -> None:
    """Fail on cycles, self references or silently missing channel receivers."""
    lookup = {item.subcatchment_id: item for item in items}
    for item in items:
        target = item.downstream_subcatchment_id
        if target is not None and target == item.subcatchment_id:
            msg = "Reach drains to itself"
            raise ValueError(msg)
        if target is not None and target not in lookup and target <= reach_count:
            msg = "Detailed channel reach has missing downstream topology"
            raise ValueError(msg)
    for item in items:
        visited: set[int] = set()
        current = item
        while current.downstream_subcatchment_id is not None:
            if current.subcatchment_id in visited:
                msg = "Cycle in downstream subcatchment links"
                raise ValueError(msg)
            visited.add(current.subcatchment_id)
            next_item = lookup.get(current.downstream_subcatchment_id)
            if next_item is None:
                break
            current = next_item


def delineate_reach_subcatchments(
    directions: NDArray[np.integer],
    accumulation: NDArray[np.floating],
    contributing: NDArray[np.bool_],
    detailed: NDArray[np.bool_],
    stream: NDArray[np.bool_],
    terrain: NDArray[np.floating],
    aois: list[BaseGeometry],
    *,
    transform: rasterio.Affine,
) -> SubcatchmentResult:
    """Partition D8 contributing cells into disjoint channel and direct-AOI basins.

    Exclusions remain in the full catchment's routing graph and cumulative
    areas, but detailed GIS polygons and terrain statistics exclude them.
    """
    _verify_inputs(directions, accumulation, contributing, detailed, stream, terrain, aois, transform)
    stream_owners, paths = _stream_reach_owners(directions, stream)
    reach_count = len(paths)
    owners = _assign_owners(directions, contributing, stream_owners, aois, transform, reach_count)
    cumulative = _upstream_sizes(directions, contributing)
    slope_grid = _terrain_slope(terrain, transform)
    labels = np.where(detailed, owners, 0).astype(np.int32)
    polygons: dict[int, list[BaseGeometry]] = {}
    for geom, label in shapes(labels, mask=labels > 0, transform=transform):
        polygons.setdefault(int(label), []).append(shape(geom))
    cell_area = abs(transform.a * transform.e)
    outputs: list[ReachSubcatchment] = []
    assigned = 0
    for subcatchment_id in range(1, reach_count + len(aois) + 1):
        full_cells = contributing & (owners == subcatchment_id)
        detailed_cells = detailed & full_cells
        number = int(np.count_nonzero(detailed_cells))
        if not number:
            continue
        if subcatchment_id not in polygons:
            msg = "Missing subcatchment polygon for D8-labelled raster cells"
            raise ValueError(msg)
        polygon = unary_union(polygons[subcatchment_id])
        if (
            polygon.is_empty
            or not polygon.is_valid
            or not math.isclose(polygon.area, number * cell_area, rel_tol=1e-8, abs_tol=1e-6)
        ):
            msg = "Invalid subcatchment polygon or raster area loss"
            raise ValueError(msg)
        outputs.append(
            _make_record(
                subcatchment_id,
                full_cells=full_cells,
                detailed_cells=detailed_cells,
                polygon=polygon,
                number=number,
                reach_count=reach_count,
                paths=paths,
                owners=owners,
                cumulative=cumulative,
                directions=directions,
                terrain=terrain,
                slope_grid=slope_grid,
                transform=transform,
                cell_area=cell_area,
            )
        )
        assigned += number
    total_detailed = int(np.count_nonzero(detailed))
    if assigned != total_detailed or len({item.subcatchment_id for item in outputs}) != len(outputs):
        msg = "Subcatchment local areas are not disjoint or do not cover detailed AOI contribution"
        raise ValueError(msg)
    _validate_downstream_links(outputs, reach_count)
    return SubcatchmentResult(
        subcatchments=outputs,
        analysed_cell_count=total_detailed,
        excluded_cell_count=int(np.count_nonzero(contributing & ~detailed)),
        full_contributing_cell_count=int(np.count_nonzero(contributing)),
        channel_reach_count=reach_count,
    )
