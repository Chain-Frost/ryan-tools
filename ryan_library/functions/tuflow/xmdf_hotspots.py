"""Time-aligned XMDF vertex diagnostics without raster-cell assumptions.

XMDF results are mesh-vertex surfaces; no raster footprint or solver-event time
is inferred from the coordinates and output timestamps.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .hourly_dt_hotspots import near_limit_threshold

REQUIRED_DATASETS: tuple[str, ...] = ("Nu", "Nc", "Nd")
_OPTIONAL_DATASETS: tuple[str, ...] = ("dt", "depth", "velocity", "water_level")
_DATASET_ALIASES: Mapping[str, tuple[str, ...]] = {
    "Nu": ("nu", "courant", "courant number", "velocity number"),
    "Nc": ("nc", "celerity", "celerity number", "shallow wave celerity number"),
    "Nd": ("nd", "diffusion", "diffusion number"),
    "dt": ("dt", "timestep", "time step", "minimum timestep"),
    "depth": ("depth", "d"),
    "velocity": ("velocity", "v", "scalar velocity"),
    "water_level": ("water level", "water surface elevation", "h"),
}


class XmdfReader(Protocol):
    """Stable portion of the public PyTUFLOW XMDF API used by this workflow."""

    def data_types(self, filter_by: str | None = None) -> list[str]: ...
    def times(self, filter_by: str | None = None) -> list[float]: ...
    def surface(self, data_type: str, time: float) -> pd.DataFrame: ...


@dataclass(frozen=True, slots=True)
class XmdfVertexSurface:
    """Values, coordinates and active flags for one output type and time."""

    x: NDArray[np.float64]
    y: NDArray[np.float64]
    value: NDArray[np.float32]
    valid: NDArray[np.bool_]

    @property
    def size(self) -> int:
        return int(self.value.size)


def _canonical_name(label: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", label.casefold()))


def resolve_xmdf_types(
    available: Sequence[str], overrides: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Resolve exact type names, rejecting missing required or ambiguous matches."""
    result: dict[str, str] = {}
    normalized: dict[str, list[str]] = {}
    for name in available:
        normalized.setdefault(_canonical_name(name), []).append(name)
    for parameter in (*REQUIRED_DATASETS, *_OPTIONAL_DATASETS):
        explicit = overrides.get(parameter) if overrides else None
        if explicit is not None:
            matches = normalized.get(_canonical_name(explicit), [])
        else:
            matches = [
                value
                for alias in _DATASET_ALIASES[parameter]
                for value in normalized.get(_canonical_name(alias), [])
            ]
        distinct = list(dict.fromkeys(matches))
        if len(distinct) > 1:
            message = f"Ambiguous XMDF dataset for {parameter}: {distinct}; supply an explicit mapping"
            raise ValueError(message)
        if not distinct:
            if parameter in REQUIRED_DATASETS:
                message = f"Required XMDF dataset {parameter} is missing; available: {list(available)}"
                raise ValueError(message)
            if explicit is not None:
                message = f"Requested XMDF dataset {explicit!r} for {parameter} is missing"
                raise ValueError(message)
            continue
        result[parameter] = distinct[0]
    if len(set(result.values())) != len(result):
        message = "XMDF parameter types must map to distinct source datasets"
        raise ValueError(message)
    return result


def _validate_time_options(
    start_time_hr: float | None, end_time_hr: float | None, every: int
) -> None:
    if every < 1:
        message = "time_stride must be at least 1"
        raise ValueError(message)
    if start_time_hr is not None and not np.isfinite(start_time_hr):
        message = "start_time_hr must be finite"
        raise ValueError(message)
    if end_time_hr is not None and not np.isfinite(end_time_hr):
        message = "end_time_hr must be finite"
        raise ValueError(message)
    if start_time_hr is not None and end_time_hr is not None and start_time_hr > end_time_hr:
        message = "start_time_hr must not exceed end_time_hr"
        raise ValueError(message)


def selected_xmdf_times(
    reader: XmdfReader,
    names: Mapping[str, str],
    *,
    start_time_hr: float | None,
    end_time_hr: float | None,
    every: int,
) -> tuple[float, ...]:
    """Select actual stored output timestamps, not an assumed hourly calendar."""
    _validate_time_options(start_time_hr, end_time_hr, every)
    times = sorted({float(value) for value in reader.times()})
    if not times or not all(np.isfinite(t) for t in times):
        message = "XMDF has no valid temporal output timestamps"
        raise ValueError(message)
    filtered = tuple(
        t for t in times
        if (start_time_hr is None or t >= start_time_hr - 1e-8)
        and (end_time_hr is None or t <= end_time_hr + 1e-8)
    )[::every]
    if not filtered:
        message = "No XMDF times are within the selected range"
        raise ValueError(message)
    for parameter, data_type in names.items():
        available = np.asarray(reader.times(data_type), dtype=np.float64)
        if not available.size or not np.all(np.isfinite(available)):
            message = f"XMDF {parameter} has no valid temporal data"
            raise ValueError(message)
        for time_hr in filtered:
            if not np.any(np.isclose(available, time_hr, atol=1e-7, rtol=0.0)):
                message = f"XMDF {parameter} has no sample at time {time_hr:g} h"
                raise ValueError(message)
    return filtered


def validate_xmdf_surface(
    frame: pd.DataFrame,
    *,
    parameter: str,
    reference: XmdfVertexSurface | None,
) -> XmdfVertexSurface:
    """Validate vertex alignment and active flags for a PyTUFLOW surface."""
    required = {"x", "y", "value", "active"}
    if not required.issubset(frame.columns):
        message = f"XMDF {parameter} surface is missing {sorted(required - set(frame.columns))}"
        raise ValueError(message)
    x = np.asarray(pd.to_numeric(frame["x"], errors="coerce"), dtype=np.float64)
    y = np.asarray(pd.to_numeric(frame["y"], errors="coerce"), dtype=np.float64)
    raw = np.asarray(pd.to_numeric(frame["value"], errors="coerce"), dtype=np.float64)
    active = frame["active"]
    if active.isna().any() or not active.isin([True, False]).all():
        message = f"XMDF {parameter} active flags are not valid booleans"
        raise ValueError(message)
    active_mask = np.asarray(active, dtype=np.bool_)
    if x.ndim != 1 or not x.size or not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        message = f"XMDF {parameter} surface has invalid node coordinates"
        raise ValueError(message)
    if len(y) != len(x) or len(raw) != len(x) or len(active_mask) != len(x):
        message = f"XMDF {parameter} has inconsistent vertex array lengths"
        raise ValueError(message)
    if reference is not None and (
        len(x) != reference.size
        or not np.allclose(x, reference.x, atol=1e-6, rtol=0.0)
        or not np.allclose(y, reference.y, atol=1e-6, rtol=0.0)
    ):
        message = f"XMDF {parameter} node coordinates/order do not match the reference surface"
        raise ValueError(message)
    values = raw.astype(np.float32)
    valid = active_mask & np.isfinite(raw) & np.isfinite(values)
    return XmdfVertexSurface(x=x, y=y, value=values, valid=valid)


def selected_nodes(
    surface: XmdfVertexSurface, mask: NDArray[np.bool_], *, limit: int, smallest: bool = False
) -> NDArray[np.int64]:
    """Bound output size; prefer the largest control ratio or lowest timestep."""
    indices = np.flatnonzero(mask).astype(np.int64)
    if len(indices) <= limit:
        return indices
    values = surface.value[indices]
    order = np.lexsort((indices, values if smallest else -values))
    return indices[order[:limit]]


def control_snapshot_records(
    *,
    mesh_name: str,
    time_hr: float,
    parameter: str,
    surface: XmdfVertexSurface,
    limit: float,
    near_fraction: float,
    maximum_points: int,
    optional: Mapping[str, XmdfVertexSurface],
) -> tuple[list[dict[str, object]], int]:
    """Report near-limit active vertices; do not imply connected cell areas."""
    near = surface.valid & (surface.value >= near_limit_threshold(limit, near_fraction))
    total = int(np.count_nonzero(near))
    rows: list[dict[str, object]] = []
    for index in selected_nodes(surface, near, limit=maximum_points):
        node = int(index)
        rec: dict[str, object] = {
            "mesh_file": mesh_name,
            "time_hr": time_hr,
            "parameter": parameter,
            "node_index": node,
            "value": float(surface.value[node]),
            "limit": limit,
            "ratio": float(surface.value[node] / limit),
            "near_nodes_total": total,
            "geometry": {"type": "Point", "coordinates": (float(surface.x[node]), float(surface.y[node]))},
        }
        for name, arr in optional.items():
            rec[f"{name}_at_node"] = float(arr.value[node]) if arr.valid[node] else None
        rows.append(rec)
    return rows, total


def dt_snapshot_records(
    *,
    mesh_name: str,
    time_hr: float,
    dt: XmdfVertexSurface,
    controls: Mapping[str, XmdfVertexSurface],
    low_dt: float,
    maximum_points: int,
) -> tuple[list[dict[str, object]], int]:
    """Return actual dt surfaces only when the XMDF contains a dt dataset."""
    low = dt.valid & (dt.value > 0) & (dt.value <= low_dt)
    total = int(np.count_nonzero(low))
    rows: list[dict[str, object]] = []
    for index in selected_nodes(dt, low, limit=maximum_points, smallest=True):
        node = int(index)
        rec: dict[str, object] = {
            "mesh_file": mesh_name,
            "time_hr": time_hr,
            "node_index": node,
            "dt_snapshot_s": float(dt.value[node]),
            "low_nodes_total": total,
            "geometry": {"type": "Point", "coordinates": (float(dt.x[node]), float(dt.y[node]))},
        }
        for parameter, surface in controls.items():
            rec[f"{parameter.lower()}_at_node"] = (
                float(surface.value[node]) if surface.valid[node] else None
            )
        rows.append(rec)
    return rows, total
