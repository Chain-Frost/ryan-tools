"""Stream-profile calculations and simple D8 reach extraction."""

import numpy as np
from numpy.typing import NDArray

from .d8 import incoming, next_cell


def equal_area_slope(distances_m: NDArray[np.floating], elevations_m: NDArray[np.floating]) -> float:
    """Equal-area profile slope, with distance measured UPSTREAM from outlet.

    Equal-area slope = 2 * area between stream profile and outlet datum / L².
    Returns dimensionless rise/run; values are signed to flag reversed slopes.
    """
    x = np.asarray(distances_m, dtype=np.float64)
    z = np.asarray(elevations_m, dtype=np.float64)
    if x.ndim != 1 or z.ndim != 1 or len(x) < 2 or x.shape != z.shape:
        msg = "Profile requires at least two paired distances/elevations"
        raise ValueError(msg)
    if not (np.all(np.isfinite(x)) and np.all(np.isfinite(z))):
        msg = "Non-finite stream profile data"
        raise ValueError(msg)
    if not np.isclose(x[0], 0.0) or not np.all(np.diff(x) > 0):
        msg = "Profile distances must start at zero and increase upstream"
        raise ValueError(msg)
    length = float(x[-1])
    area = float(np.trapezoid(z - z[0], x))
    return 2.0 * area / (length * length)


def stream_reaches(directions: NDArray[np.integer], stream: NDArray[np.bool_]) -> list[list[tuple[int, int]]]:
    """Split threshold-derived D8 stream network at confluences and ends.

    Each reach is an ordered list of (row, col) cell centres, upstream first.
    """
    if directions.shape != stream.shape:
        msg = "Direction and stream arrays must match"
        raise ValueError(msg)
    degree: dict[tuple[int, int], int] = {}
    for r, c in zip(*np.nonzero(stream), strict=True):
        key = int(r), int(c)
        degree[key] = sum(bool(stream[n]) for n in incoming(directions, *key))
    reaches: list[list[tuple[int, int]]] = []
    for start, count in degree.items():
        if count == 1:
            continue
        at = start
        path = [at]
        seen = {at}
        while True:
            downstream = next_cell(directions, *at)
            if downstream is None or not stream[downstream]:
                break
            if downstream in seen:
                msg = "D8 stream cycle detected"
                raise ValueError(msg)
            path.append(downstream)
            seen.add(downstream)
            at = downstream
            if degree.get(at, 0) != 1:
                break
        if len(path) >= 2:
            reaches.append(path)
    return reaches
