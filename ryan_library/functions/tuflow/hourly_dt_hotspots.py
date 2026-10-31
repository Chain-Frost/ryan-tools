"""Reusable time-aligned TUFLOW HPC dt/Nu/Nc/Nd hotspot algorithms.

Four hourly rasters are compared only at matching output timestamps.
The whole-run dt_Min raster is intentionally not used.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from affine import Affine
from rasterio.crs import CRS
from scipy import ndimage

PARAMETERS: tuple[str, ...] = ("dt", "Nu", "Nc", "Nd")
CONTROL_PARAMETERS: tuple[str, ...] = ("Nu", "Nc", "Nd")
DEFAULT_CONTROL_LIMITS: dict[str, float] = {"Nu": 1.0, "Nc": 1.0, "Nd": 0.3}


def near_limit_threshold(limit: float, fraction: float) -> np.float32:
    """Use source raster precision consistently when testing near-limit cells."""
    return np.float32(limit * fraction)


HOURLY_PATTERN = re.compile(
    r"^(?P<prefix>.+)_(?P<parameter>dt|Nu|Nc|Nd)_(?P<hour>\d{3})_00_00\.tif$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class HourlyRasterSet:
    """One time-aligned set of four parameter rasters."""

    hour: int
    paths: Mapping[str, Path]


@dataclass(frozen=True, slots=True)
class GridDefinition:
    """Shared dimensions and georeferencing of validated rasters."""

    shape: tuple[int, int]
    transform: Affine
    crs: CRS
    cell_area_ha: float


def _validate_hour_bounds(min_hour: int | None, max_hour: int | None) -> None:
    if min_hour is not None and min_hour < 0:
        message = "min_hour must be nonnegative"
        raise ValueError(message)
    if max_hour is not None and max_hour < 0:
        message = "max_hour must be nonnegative"
        raise ValueError(message)
    if min_hour is not None and max_hour is not None and min_hour > max_hour:
        message = "min_hour must not exceed max_hour"
        raise ValueError(message)


def _selected_hourly_file(
    path: Path,
    *,
    prefix: str | None,
    min_hour: int | None,
    max_hour: int | None,
) -> tuple[str, int, str] | None:
    """Parse a selected hourly filename, returning prefix, hour and parameter."""
    if not path.is_file():
        return None
    match = HOURLY_PATTERN.fullmatch(path.name)
    if match is None:
        return None
    file_prefix = match.group("prefix")
    if prefix is not None and file_prefix.casefold() != prefix.casefold():
        return None
    hour = int(match.group("hour"))
    if (min_hour is not None and hour < min_hour) or (max_hour is not None and hour > max_hour):
        return None
    parameter = next(name for name in PARAMETERS if name.casefold() == match.group("parameter").casefold())
    return file_prefix, hour, parameter


def discover_hourly_rasters(
    grid_dir: Path,
    *,
    prefix: str | None = None,
    min_hour: int | None = None,
    max_hour: int | None = None,
) -> tuple[str, tuple[HourlyRasterSet, ...]]:
    """Find complete sets; reject mixed runs, duplicates and missing parameter hours."""
    if not grid_dir.is_dir():
        message = f"Grid directory does not exist: {grid_dir}"
        raise FileNotFoundError(message)
    _validate_hour_bounds(min_hour, max_hour)

    discovered: dict[str, dict[int, dict[str, Path]]] = {}
    actual_prefixes: dict[str, str] = {}
    for path in sorted(grid_dir.iterdir()):
        parsed = _selected_hourly_file(path, prefix=prefix, min_hour=min_hour, max_hour=max_hour)
        if parsed is None:
            continue
        file_prefix, hour, parameter = parsed
        prefix_key = file_prefix.casefold()
        actual_prefixes.setdefault(prefix_key, file_prefix)
        file_set = discovered.setdefault(prefix_key, {}).setdefault(hour, {})
        if parameter in file_set:
            message = f"Duplicate {parameter} raster for hour {hour}: {file_set[parameter]} and {path}"
            raise ValueError(message)
        file_set[parameter] = path

    if not discovered:
        message = f"No matching hourly dt/Nu/Nc/Nd rasters found in {grid_dir}"
        raise FileNotFoundError(message)
    if len(discovered) != 1:
        names = ", ".join(sorted(actual_prefixes.values()))
        message = f"Multiple simulation prefixes found ({names}); select one with --prefix"
        raise ValueError(message)
    prefix_key, hourly = next(iter(discovered.items()))
    for hour, paths in sorted(hourly.items()):
        missing = [name for name in PARAMETERS if name not in paths]
        if missing:
            message = f"Incomplete hourly raster set at hour {hour}: missing {', '.join(missing)}"
            raise ValueError(message)
    return actual_prefixes[prefix_key], tuple(HourlyRasterSet(hour=hour, paths=hourly[hour]) for hour in sorted(hourly))


def _require_metre_crs(crs: CRS | None, path: Path) -> CRS:
    if crs is None or not crs.is_projected:
        message = f"Raster requires a projected CRS: {path}"
        raise ValueError(message)
    _, units_to_metres = crs.linear_units_factor
    if not np.isclose(units_to_metres, 1.0, rtol=0.0, atol=1e-9):
        message = f"Raster requires metre-based projected coordinates: {path}"
        raise ValueError(message)
    return crs


def validate_raster_grids(raster_sets: Sequence[HourlyRasterSet]) -> GridDefinition:
    """Preflight all headers; mismatched georeferencing is never silently resampled."""
    if not raster_sets:
        message = "No hourly raster sets were selected"
        raise ValueError(message)
    reference: GridDefinition | None = None
    for raster_set in raster_sets:
        for parameter in PARAMETERS:
            path = raster_set.paths[parameter]
            with rasterio.open(path) as source:
                if source.count != 1:
                    message = f"Expected a single-band raster: {path}"
                    raise ValueError(message)
                crs = _require_metre_crs(source.crs, path)
                transform = source.transform
                determinant = transform.a * transform.e - transform.b * transform.d
                if not np.isfinite(determinant) or abs(determinant) <= 1e-12:
                    message = f"Raster transform is degenerate: {path}"
                    raise ValueError(message)
                if reference is None:
                    reference = GridDefinition(
                        shape=(source.height, source.width),
                        transform=transform,
                        crs=crs,
                        cell_area_ha=abs(determinant) / 10_000.0,
                    )
                elif (
                    (source.height, source.width) != reference.shape
                    or crs != reference.crs
                    or not transform.almost_equals(reference.transform, precision=1e-8)
                ):
                    message = f"Raster grid/CRS differs from reference raster: {path}"
                    raise ValueError(message)
    if reference is None:
        message = "No raster headers were successfully inspected"
        raise ValueError(message)
    return reference


def read_hour(raster_set: HourlyRasterSet) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """Read only one timestamp into memory; respect raster masks and nonfinite pixels."""
    result: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for parameter in PARAMETERS:
        with rasterio.open(raster_set.paths[parameter]) as source:
            values = source.read(1).astype(np.float32, copy=False)
            valid = (source.read_masks(1) != 0) & np.isfinite(values)
        result[parameter] = values, valid
    return result


def cell_coordinates(transform: Affine, row: int, col: int) -> tuple[float, float]:
    """Return a cell centre, including rotated and sheared grids."""
    x, y = transform * (col + 0.5, row + 0.5)
    return float(x), float(y)


def component_cells(
    mask: np.ndarray, *, connectivity: int, min_cells: int
) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
    """Find component cells using bounding slices, not a full-grid scan per component."""
    if connectivity not in (4, 8):
        message = "connectivity must be 4 or 8"
        raise ValueError(message)
    if min_cells < 1:
        message = "min_cells must be at least 1"
        raise ValueError(message)
    structure = ndimage.generate_binary_structure(2, 2 if connectivity == 8 else 1)
    labels, _ = ndimage.label(mask, structure=structure)
    for component_id, region in enumerate(ndimage.find_objects(labels), start=1):
        if region is None:
            continue
        local_rows, local_cols = np.nonzero(labels[region] == component_id)
        if len(local_rows) < min_cells:
            continue
        yield component_id, local_rows + (region[0].start or 0), local_cols + (region[1].start or 0)


def leading_control(values: Mapping[str, float | None], limits: Mapping[str, float]) -> str | None:
    """Name the largest normalised control value (not a causal assertion)."""
    ratios = {name: value / limits[name] for name, value in values.items() if value is not None}
    return max(ratios, key=lambda name: ratios[name]) if ratios else None


def _safe_parameter_values(
    values: np.ndarray,
    valid: np.ndarray,
    rows: np.ndarray,
    cols: np.ndarray,
) -> np.ndarray:
    selected = values[rows, cols].astype(np.float64, copy=True)
    selected[~valid[rows, cols]] = np.nan
    return selected


def hourly_dt_records(
    *,
    hour: int,
    arrays: Mapping[str, tuple[np.ndarray, np.ndarray]],
    grid: GridDefinition,
    low_dt: float,
    critical_dt: float,
    near_limit_fraction: float,
    limits: Mapping[str, float],
    connectivity: int,
    min_cells: int,
) -> Iterator[dict[str, object]]:
    """Emit a point per contiguous low-dt patch for one output hour."""
    dt, dt_valid = arrays["dt"]
    low = dt_valid & (dt > 0) & (dt <= low_dt)
    for component_id, rows, cols in component_cells(low, connectivity=connectivity, min_cells=min_cells):
        component_dt = dt[rows, cols]
        min_index = int(np.argmin(component_dt))
        row, col = int(rows[min_index]), int(cols[min_index])
        at_min: dict[str, float | None] = {}
        peak: dict[str, float | None] = {}
        for parameter in CONTROL_PARAMETERS:
            values, valid = arrays[parameter]
            at_min[parameter] = float(values[row, col]) if valid[row, col] else None
            selected = _safe_parameter_values(values, valid, rows, cols)
            peak[parameter] = float(np.nanmax(selected)) if np.any(np.isfinite(selected)) else None
        record: dict[str, object] = {
            "hour": hour,
            "component": component_id,
            "severity": "critical" if float(component_dt[min_index]) <= critical_dt else "low",
            "min_dt_s": float(component_dt[min_index]),
            "cells_low": len(rows),
            "cells_critical": np.count_nonzero(component_dt <= critical_dt),
            "area_ha": float(len(rows) * grid.cell_area_ha),
            "highest_ratio_at_min": leading_control(at_min, limits),
            "geometry": {"type": "Point", "coordinates": cell_coordinates(grid.transform, row, col)},
        }
        for parameter in CONTROL_PARAMETERS:
            key = parameter.lower()
            record[f"{key}_at_min"] = at_min[parameter]
            record[f"{key}_peak"] = peak[parameter]
            peak_value = peak[parameter]
            is_near = peak_value is not None and (
                np.float32(peak_value) >= near_limit_threshold(limits[parameter], near_limit_fraction)
            )
            record[f"{key}_near"] = int(is_near)
        yield record


def hourly_parameter_records(
    *,
    hour: int,
    arrays: Mapping[str, tuple[np.ndarray, np.ndarray]],
    grid: GridDefinition,
    low_dt: float,
    near_limit_fraction: float,
    limits: Mapping[str, float],
    connectivity: int,
    min_cells: int,
) -> Iterator[dict[str, object]]:
    """Emit same-hour near-limit Nu/Nc/Nd patches independently of low-dt patches."""
    dt, dt_valid = arrays["dt"]
    for parameter in CONTROL_PARAMETERS:
        values, valid = arrays[parameter]
        limit = limits[parameter]
        mask = valid & (values >= near_limit_threshold(limit, near_limit_fraction))
        for component_id, rows, cols in component_cells(mask, connectivity=connectivity, min_cells=min_cells):
            subset = values[rows, cols]
            max_index = int(np.argmax(subset))
            row, col = int(rows[max_index]), int(cols[max_index])
            overlapping = dt_valid[rows, cols] & (dt[rows, cols] > 0) & (dt[rows, cols] <= low_dt)
            yield {
                "hour": hour,
                "parameter": parameter,
                "component": component_id,
                "value": float(subset[max_index]),
                "limit": float(limit),
                "ratio": float(subset[max_index] / limit),
                "dt_at_peak_s": float(dt[row, col]) if dt_valid[row, col] else None,
                "cells_near": len(rows),
                "cells_low_dt": np.count_nonzero(overlapping),
                "area_ha": float(len(rows) * grid.cell_area_ha),
                "geometry": {"type": "Point", "coordinates": cell_coordinates(grid.transform, row, col)},
            }


def overall_dt_records(
    *,
    counts: np.ndarray,
    minimum: np.ndarray,
    hour_of_minimum: np.ndarray,
    grid: GridDefinition,
    connectivity: int,
    min_cells: int,
) -> Iterator[dict[str, object]]:
    """Cluster the union of low-dt cells across all analysed hours."""
    for component_id, rows, cols in component_cells(counts > 0, connectivity=connectivity, min_cells=min_cells):
        selected_dt = minimum[rows, cols]
        idx = int(np.argmin(selected_dt))
        row, col = int(rows[idx]), int(cols[idx])
        observed_counts = counts[rows, cols]
        yield {
            "component": component_id,
            "worst_hour": int(hour_of_minimum[row, col]),  # Fiona requires a built-in int.
            "min_dt_s": float(selected_dt[idx]),
            "cells_ever": len(rows),
            "area_ha": float(len(rows) * grid.cell_area_ha),
            "max_hours_at_cell": int(np.max(observed_counts)),
            "hours_at_worst_cell": int(counts[row, col]),
            "sum_cell_hours": int(np.sum(observed_counts, dtype=np.uint64)),
            "worst_row": row,
            "worst_col": col,
            "geometry": {"type": "Point", "coordinates": cell_coordinates(grid.transform, row, col)},
        }
