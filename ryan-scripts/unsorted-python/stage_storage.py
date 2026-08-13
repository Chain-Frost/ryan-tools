"""Experimental block-streamed stage-storage calculations for a single-band DEM."""

from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import Iterable, Protocol, Sequence, cast

import numpy as np
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger


class _AffineTransform(Protocol):
    a: float
    b: float
    d: float
    e: float


class _RasterDataset(Protocol):
    count: int
    width: int
    height: int
    nodata: float | int | None
    transform: _AffineTransform

    def block_windows(self, band_index: int) -> Iterable[tuple[tuple[int, int], object]]: ...

    def read(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.generic]: ...

    def close(self) -> None: ...


def _open_raster(path: Path) -> _RasterDataset:
    return cast(
        _RasterDataset,
        rasterio.open(path),  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    )


def _valid_values(data: npt.NDArray[np.generic], nodata_value: float | int | None) -> npt.NDArray[np.float64]:
    values = np.asarray(data, dtype=np.float64)
    valid = np.isfinite(values)
    if nodata_value is not None and np.isfinite(nodata_value):
        valid &= values != float(nodata_value)
    return values[valid]


def find_elevation_bounds(dem_path: Path, nodata_value: float | None = None) -> tuple[float, float]:
    """Return exact finite minimum and maximum elevations using block streaming."""
    if not dem_path.is_file():
        raise FileNotFoundError(f"DEM does not exist: {dem_path}")
    minimum = np.inf
    maximum = -np.inf
    with closing(_open_raster(dem_path)) as source:
        if source.count != 1:
            raise ValueError(f"Stage-storage requires a single-band DEM: {dem_path}")
        effective_nodata = source.nodata if nodata_value is None else nodata_value
        for _, window in source.block_windows(1):
            values = _valid_values(source.read(1, window=window), effective_nodata)
            if values.size:
                minimum = min(minimum, float(values.min()))
                maximum = max(maximum, float(values.max()))
    if not np.isfinite(minimum) or not np.isfinite(maximum):
        raise ValueError(f"DEM contains no finite data cells: {dem_path}")
    return float(minimum), float(maximum)


def compute_stage_storage(
    dem_path: Path, levels: Sequence[float], nodata_value: float | None = None
) -> dict[float, float]:
    """Return cumulative volume below each strictly increasing water level."""
    path = dem_path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"DEM does not exist: {path}")
    level_values = np.asarray(list(levels), dtype=np.float64)
    if level_values.ndim != 1 or level_values.size == 0:
        raise ValueError("At least one stage level is required")
    if not np.all(np.isfinite(level_values)):
        raise ValueError("Stage levels must be finite")
    if not np.all(np.diff(level_values) > 0):
        raise ValueError("Stage levels must be unique and strictly increasing")

    bin_edges = np.concatenate(([-np.inf], level_values, [np.inf]))
    global_counts = np.zeros(len(bin_edges) - 1, dtype=np.int64)
    global_sums = np.zeros(len(bin_edges) - 1, dtype=np.float64)

    with closing(_open_raster(path)) as source:
        if source.count != 1:
            raise ValueError(f"Stage-storage requires a single-band DEM: {path}")
        determinant = source.transform.a * source.transform.e - source.transform.b * source.transform.d
        cell_area = abs(float(determinant))
        if not np.isfinite(cell_area) or cell_area <= 0:
            raise ValueError(f"DEM has an invalid cell transform: {path}")
        effective_nodata = source.nodata if nodata_value is None else nodata_value
        logger.info(
            "Computing stage-storage for {} ({}x{}) using block streaming.",
            path.name,
            source.width,
            source.height,
        )
        for _, window in source.block_windows(1):
            values = _valid_values(source.read(1, window=window), effective_nodata)
            if not values.size:
                continue
            counts, _ = np.histogram(values, bins=bin_edges)
            sums, _ = np.histogram(values, bins=bin_edges, weights=values)
            global_counts += counts
            global_sums += sums

    cumulative_counts = np.cumsum(global_counts)
    cumulative_sums = np.cumsum(global_sums)
    return {
        float(level): float(cell_area * (level * cumulative_counts[index] - cumulative_sums[index]))
        for index, level in enumerate(level_values)
    }
