"""D8 routing utilities, using the GRASS r.watershed direction convention.

Codes run counter-clockwise from NE (1) to E (8). Negative codes
identify outgoing raster-boundary drainage; 0 is a sink/depression.
"""

from collections import deque

import numpy as np
from numpy.typing import NDArray

# (row step, column step), from a cell to its downstream neighbour.
OFFSETS: dict[int, tuple[int, int]] = {
    1: (-1, 1),
    2: (-1, 0),
    3: (-1, -1),
    4: (0, -1),
    5: (1, -1),
    6: (1, 0),
    7: (1, 1),
    8: (0, 1),
}


def _check(directions: NDArray[np.integer], seeds: NDArray[np.bool_]) -> None:
    if directions.ndim != 2 or seeds.ndim != 2 or directions.shape != seeds.shape:
        msg = "D8 raster and AOI raster must be same-shape 2D arrays"
        raise ValueError(msg)
    if not np.issubdtype(directions.dtype, np.integer):
        msg = "D8 flow directions must be integers"
        raise TypeError(msg)
    if not np.all(np.abs(directions.astype(np.int16)) <= 8):
        msg = "Invalid GRASS D8 flow direction, expected -8..8"
        raise ValueError(msg)
    if not bool(np.any(seeds)):
        msg = "AOI raster has no selected cells"
        raise ValueError(msg)


def _check_valid_cells(
    directions: NDArray[np.integer],
    aoi: NDArray[np.bool_],
    valid_cells: NDArray[np.bool_] | None,
) -> None:
    """Distinguish unknown D8 data from genuine flow sinks."""
    if valid_cells is None:
        return
    if valid_cells.shape != directions.shape:
        msg = "D8 validity mask must match the direction grid"
        raise ValueError(msg)
    if np.any(aoi & ~valid_cells):
        msg = "AOI intersects NoData/unknown flow directions"
        raise ValueError(msg)


def _check_unknown_upstream_boundary(visited: NDArray[np.bool_], valid_cells: NDArray[np.bool_] | None) -> None:
    """Fail where a NoData pixel could be an omitted upstream contributor."""
    if valid_cells is None:
        return
    height, width = valid_cells.shape
    for row, col in zip(*np.nonzero(visited), strict=True):
        for dr, dc in OFFSETS.values():
            nr, nc = int(row) + dr, int(col) + dc
            if 0 <= nr < height and 0 <= nc < width and not valid_cells[nr, nc]:
                msg = "Contributing catchment borders NoData/unknown D8 terrain; supply complete DEM"
                raise ValueError(msg)


def next_cell(directions: NDArray[np.integer], row: int, col: int) -> tuple[int, int] | None:
    """Return downstream cell or None for sink/edge/outgoing direction."""
    direction = int(directions[row, col])
    if direction <= 0:
        return None
    dr, dc = OFFSETS[direction]
    nr, nc = row + dr, col + dc
    if not (0 <= nr < directions.shape[0] and 0 <= nc < directions.shape[1]):
        return None
    return nr, nc


def incoming(directions: NDArray[np.integer], row: int, col: int) -> list[tuple[int, int]]:
    """Direct upstream neighbours of a D8 cell."""
    height, width = directions.shape
    result: list[tuple[int, int]] = []
    for direction, (dr, dc) in OFFSETS.items():
        ur, uc = row - dr, col - dc
        if 0 <= ur < height and 0 <= uc < width and int(directions[ur, uc]) == direction:
            result.append((ur, uc))
    return result


def contributing_cells(
    directions: NDArray[np.integer],
    aoi: NDArray[np.bool_],
    *,
    allow_unknown_boundary: bool = False,
    require_complete: bool = True,
    valid_cells: NDArray[np.bool_] | None = None,
) -> NDArray[np.bool_]:
    """Delineate all cells draining into ANY AOI cell, not a single outlet.

    Each cell is visited once. Neighbour checks are constant-time, so the
    algorithm scales linearly with contributing cell count.
    """
    _check(directions, aoi)
    _check_valid_cells(directions, aoi, valid_cells)
    visited = aoi.copy()
    queue: deque[tuple[int, int]] = deque((int(r), int(c)) for r, c in zip(*np.nonzero(aoi), strict=True))
    while queue:
        row, col = queue.popleft()
        for ur, uc in incoming(directions, row, col):
            if not visited[ur, uc]:
                visited[ur, uc] = True
                queue.append((ur, uc))

    if not allow_unknown_boundary:
        _check_unknown_upstream_boundary(visited, valid_cells)

    # A directed cycle cannot represent terrain that drains to the AOI.
    # Detect any such cycle within the complete selected drainage subgraph.
    states = np.zeros(directions.shape, dtype=np.uint8)
    for rr, cc in zip(*np.nonzero(visited), strict=True):
        row, col = int(rr), int(cc)
        trail: list[tuple[int, int]] = []
        while visited[row, col] and states[row, col] == 0:
            states[row, col] = 1
            trail.append((row, col))
            dest = next_cell(directions, row, col)
            if dest is None:
                break
            row, col = dest
        else:
            if visited[row, col] and states[row, col] == 1:
                msg = "D8 flow-direction cycle within AOI contributing area"
                raise ValueError(msg)
        for r, c in trail:
            states[r, c] = 2

    if require_complete and (
        bool(np.any(visited[0, :]))
        or bool(np.any(visited[-1, :]))
        or bool(np.any(visited[:, 0]))
        or bool(np.any(visited[:, -1]))
    ):
        msg = "Contributing catchment touches DEM edge; supply wider terrain"
        raise ValueError(msg)
    return visited


def stream_mask(accumulation: NDArray[np.floating], *, cell_area_m2: float, min_area_m2: float) -> NDArray[np.bool_]:
    """Area-thresholded stream cells; unrelated to full AOI catchment extent."""
    if cell_area_m2 <= 0 or min_area_m2 <= 0:
        msg = "Cell area and stream threshold must be positive"
        raise ValueError(msg)
    if not np.all(np.isfinite(accumulation)):
        msg = "Accumulation must be finite and free of NoData"
        raise ValueError(msg)
    return accumulation * cell_area_m2 >= min_area_m2


def headwater_extensions(
    directions: NDArray[np.integer],
    stream: NDArray[np.bool_],
    accumulation: NDArray[np.floating],
    *,
    allowed_cells: NDArray[np.bool_] | None = None,
) -> list[list[tuple[int, int]]]:
    """Trace synthetic upstream paths from stream initiation cells to divides.

    Select the highest-accumulation upstream branch at each junction.
    These are *inferred overland flow paths*, not mapped natural channels.
    """
    if directions.shape != stream.shape or stream.shape != accumulation.shape:
        msg = "Mismatched raster shapes"
        raise ValueError(msg)
    if allowed_cells is not None:
        if allowed_cells.shape != stream.shape:
            msg = "Allowed stream-analysis raster shape must match"
            raise ValueError(msg)
        if np.any(stream & ~allowed_cells):
            msg = "Threshold streams must lie inside stream-analysis cells"
            raise ValueError(msg)
    paths: list[list[tuple[int, int]]] = []
    for r, c in zip(*np.nonzero(stream), strict=True):
        start = (int(r), int(c))
        if any(stream[ur, uc] for ur, uc in incoming(directions, *start)):
            continue
        route = [start]
        current = start
        seen = {start}
        while True:
            choices = [
                p
                for p in incoming(directions, *current)
                if not stream[p] and (allowed_cells is None or allowed_cells[p])
            ]
            if not choices:
                break
            nxt = max(choices, key=lambda p: (float(accumulation[p]), -p[0], -p[1]))
            if nxt in seen:
                msg = "Cycle in synthetic headwater trace"
                raise ValueError(msg)
            seen.add(nxt)
            route.append(nxt)
            current = nxt
        if len(route) >= 2:
            paths.append(route[::-1])
    return paths
