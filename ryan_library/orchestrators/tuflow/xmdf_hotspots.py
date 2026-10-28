"""Managed mesh-vertex hotspot workflow for PyTUFLOW XMDF outputs.

This is not a raster connected-component workflow. It reports active mesh
vertices at stored output snapshots and does not infer solver-event times.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import fiona
import numpy as np
import rasterio
from loguru import logger
from numpy.typing import NDArray
from rasterio.errors import CRSError

from ...functions.tuflow.hourly_dt_hotspots import DEFAULT_CONTROL_LIMITS, near_limit_threshold
from ...functions.tuflow.xmdf_hotspots import (
    REQUIRED_DATASETS,
    XmdfReader,
    XmdfVertexSurface,
    control_snapshot_records,
    dt_snapshot_records,
    resolve_xmdf_types,
    selected_nodes,
    selected_xmdf_times,
    validate_xmdf_surface,
)

_CONTROL_SCHEMA: dict[str, str] = {
    "mesh_file": "str",
    "time_hr": "float",
    "parameter": "str:4",
    "node_index": "int64",
    "value": "float",
    "limit": "float",
    "ratio": "float",
    "near_nodes_total": "int64",
    "dt_at_node": "float",
    "depth_at_node": "float",
    "velocity_at_node": "float",
    "water_level_at_node": "float",
}
_DT_SCHEMA: dict[str, str] = {
    "mesh_file": "str",
    "time_hr": "float",
    "node_index": "int64",
    "dt_snapshot_s": "float",
    "low_nodes_total": "int64",
    "nu_at_node": "float",
    "nc_at_node": "float",
    "nd_at_node": "float",
}
_SUMMARY_SCHEMA: dict[str, str] = {
    "mesh_file": "str",
    "parameter": "str:4",
    "node_index": "int64",
    "peak_value": "float",
    "limit": "float",
    "ratio": "float",
    "peak_time_hr": "float",
    "near_snapshots": "int64",
}
_METADATA_SCHEMA: dict[str, str] = {
    "mesh_file": "str",
    "crs": "str",
    "source_kind": "str",
    "spatial_support": "str",
    "time_count": "int",
    "first_time_hr": "float",
    "last_time_hr": "float",
    "node_count": "int64",
    "nu_dataset": "str",
    "nc_dataset": "str",
    "nd_dataset": "str",
    "dt_dataset": "str",
    "dt_available": "int",
    "low_dt_s": "float",
    "near_fraction": "float",
    "nu_limit": "float",
    "nc_limit": "float",
    "nd_limit": "float",
    "max_points_per_type": "int",
    "truncated_records": "int64",
}


def _empty_dataset_names() -> dict[str, str]:
    """Supply a strictly typed independent dataset-name map."""
    return {}


@dataclass(frozen=True, slots=True)
class XmdfHotspotConfig:
    """Separate mesh-specific input and reporting configuration."""

    xmdf_files: tuple[Path, ...]
    output: Path
    crs: str
    mesh_file: Path | None = None
    dataset_names: Mapping[str, str] = field(default_factory=_empty_dataset_names)
    start_time_hr: float | None = None
    end_time_hr: float | None = None
    time_stride: int = 1
    low_dt: float = 5.0
    near_limit_fraction: float = 0.9
    control_limits: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULT_CONTROL_LIMITS))
    max_points_per_type: int = 1000
    overwrite: bool = False
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class XmdfHotspotResult:
    output: Path
    mesh_files: int
    snapshots: int
    control_features: int
    dt_features: int
    summary_features: int
    dt_sources: int
    dry_run: bool


@dataclass(slots=True)
class _Peak:
    """Peak values, sample times and count of qualifying snapshots per vertex."""

    values: NDArray[np.float32]
    times: NDArray[np.float64]
    counts: NDArray[np.uint32]


def _reader(path: Path, twodm: Path | None) -> XmdfReader:
    """Defer the heavy PyTUFLOW/mesh import until XMDF mode is requested."""
    from pytuflow import XMDF

    return cast("XmdfReader", XMDF(path, twodm=twodm, driver="h5py"))


def _validate_inputs(config: XmdfHotspotConfig) -> None:
    if not config.xmdf_files or len(set(config.xmdf_files)) != len(config.xmdf_files):
        message = "Select at least one distinct XMDF file"
        raise ValueError(message)
    for source in config.xmdf_files:
        if source.suffix.casefold() != ".xmdf" or not source.is_file():
            message = f"XMDF input does not exist or has the wrong extension: {source}"
            raise FileNotFoundError(message)
    if config.mesh_file is not None and (len(config.xmdf_files) != 1 or not config.mesh_file.is_file()):
        message = "Explicit --mesh requires exactly one XMDF source and an existing 2DM file"
        raise ValueError(message)


def _validate_numeric_options(config: XmdfHotspotConfig) -> None:
    if config.time_stride < 1 or config.max_points_per_type < 1:
        message = "time_stride and max_points_per_type must be positive"
        raise ValueError(message)
    if not np.isfinite(config.low_dt) or config.low_dt <= 0:
        message = "low_dt must be finite and positive"
        raise ValueError(message)
    if not np.isfinite(config.near_limit_fraction) or not 0 < config.near_limit_fraction <= 1:
        message = "near_limit_fraction must be within (0, 1]"
        raise ValueError(message)
    if set(config.control_limits) != set(REQUIRED_DATASETS):
        message = "control_limits must provide exactly Nu, Nc and Nd"
        raise ValueError(message)
    if any(not np.isfinite(v) or v <= 0 for v in config.control_limits.values()):
        message = "Control-number limits must be finite and positive"
        raise ValueError(message)


def _validate_destination(config: XmdfHotspotConfig) -> None:
    if config.output.suffix.casefold() != ".gpkg":
        message = "Output must be a .gpkg file"
        raise ValueError(message)
    if not config.dry_run and (config.output.is_symlink() or (config.output.exists() and not config.overwrite)):
        message = f"Output already exists or is a symbolic link: {config.output}"
        raise FileExistsError(message)


def _validate_crs(value: str) -> str:
    try:
        crs = rasterio.crs.CRS.from_user_input(value)
    except (ValueError, CRSError) as error:
        message = f"Invalid XMDF CRS {value!r}"
        raise ValueError(message) from error
    if not crs.is_projected:
        message = "XMDF CRS must be projected; coordinates are not transformed"
        raise ValueError(message)
    _, scale = crs.linear_units_factor
    if not np.isclose(scale, 1.0, rtol=0.0, atol=1e-9):
        message = "XMDF projected CRS must use metre units"
        raise ValueError(message)
    return crs.to_wkt()


def _validate_config(config: XmdfHotspotConfig) -> str:
    _validate_inputs(config)
    _validate_numeric_options(config)
    _validate_destination(config)
    return _validate_crs(config.crs)


def _new_layer(path: Path, name: str, schema: Mapping[str, str], *, crs_wkt: str | None) -> None:
    geometry = "None" if crs_wkt is None else "Point"
    properties: dict[str, object] = {"geometry": geometry, "properties": dict(schema)}
    with fiona.open(path, mode="w", layer=name, driver="GPKG", schema=properties, crs_wkt=crs_wkt):
        pass


def _write_points(path: Path, layer: str, records: Sequence[dict[str, object]], fields: Mapping[str, str]) -> int:
    if not records:
        return 0
    with fiona.open(path, "a", layer=layer) as output:
        output.writerecords(
            {"geometry": record["geometry"], "properties": {key: record.get(key) for key in fields}}
            for record in records
        )
    return len(records)


def _write_metadata(path: Path, records: Sequence[dict[str, object]]) -> None:
    with fiona.open(path, "a", layer="xmdf_metadata") as output:
        output.writerecords(
            {"geometry": None, "properties": {key: record.get(key) for key in _METADATA_SCHEMA}} for record in records
        )


def _stamp(path: Path) -> tuple[int, int, int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _publish(temp: Path, output: Path, *, overwrite: bool) -> None:
    if overwrite:
        temp.replace(output)
    elif os.name == "nt":
        temp.rename(output)
    else:
        output.hardlink_to(temp)
        temp.unlink()


def _fetch_surfaces(reader: XmdfReader, names: Mapping[str, str], time_hr: float) -> dict[str, XmdfVertexSurface]:
    """Load a single output time and enforce shared ordered vertex support."""
    surfaces: dict[str, XmdfVertexSurface] = {}
    reference: XmdfVertexSurface | None = None
    for parameter in (*REQUIRED_DATASETS, "dt", "depth", "velocity", "water_level"):
        data_type = names.get(parameter)
        if data_type is None:
            continue
        surface = validate_xmdf_surface(reader.surface(data_type, time_hr), parameter=parameter, reference=reference)
        if reference is None:
            reference = surface
        surfaces[parameter] = surface
    return surfaces


def _matching_optional_types(reader: XmdfReader, names: Mapping[str, str], times: tuple[float, ...]) -> dict[str, str]:
    """Skip optional datasets that do not cover every selected time."""
    selected: dict[str, str] = {parameter: names[parameter] for parameter in REQUIRED_DATASETS}
    for parameter, label in names.items():
        if parameter in REQUIRED_DATASETS:
            continue
        available = np.asarray(reader.times(label), dtype=np.float64)
        if available.size and all(np.any(np.isclose(available, t, rtol=0.0, atol=1e-7)) for t in times):
            selected[parameter] = label
        else:
            logger.warning("Skipping optional {} XMDF dataset: incomplete temporal coverage", parameter)
    return selected


def _summary_records(
    *,
    mesh_name: str,
    surfaces: Mapping[str, XmdfVertexSurface],
    peaks: Mapping[str, _Peak],
    config: XmdfHotspotConfig,
) -> tuple[list[dict[str, object]], int]:
    out: list[dict[str, object]] = []
    skipped = 0
    for parameter in REQUIRED_DATASETS:
        maximum = peaks[parameter]
        surface = surfaces[parameter]
        finite = maximum.counts > 0
        count = int(np.count_nonzero(finite))
        selected_surface = XmdfVertexSurface(surface.x, surface.y, maximum.values, finite)
        for index in selected_nodes(selected_surface, finite, limit=config.max_points_per_type):
            node = int(index)
            out.append(
                {
                    "mesh_file": mesh_name,
                    "parameter": parameter,
                    "node_index": node,
                    "peak_value": float(maximum.values[node]),
                    "limit": config.control_limits[parameter],
                    "ratio": float(maximum.values[node] / config.control_limits[parameter]),
                    "peak_time_hr": float(maximum.times[node]),
                    "near_snapshots": int(maximum.counts[node]),
                    "geometry": {"type": "Point", "coordinates": (float(surface.x[node]), float(surface.y[node]))},
                }
            )
        skipped += max(0, count - config.max_points_per_type)
    return out, skipped


def _process_source(
    *,
    xmdf: Path,
    config: XmdfHotspotConfig,
    output: Path | None,
) -> tuple[int, int, int, int, int, dict[str, object]]:
    reader = _reader(xmdf, config.mesh_file)
    names = resolve_xmdf_types(reader.data_types(), config.dataset_names)
    times = selected_xmdf_times(
        reader,
        {parameter: names[parameter] for parameter in REQUIRED_DATASETS},
        start_time_hr=config.start_time_hr,
        end_time_hr=config.end_time_hr,
        every=config.time_stride,
    )
    active_names = _matching_optional_types(reader, names, times)
    has_dt = "dt" in active_names
    if not has_dt:
        logger.warning("{}: no temporal dt dataset; exporting control-number hotspots only", xmdf.name)
    if output is None:
        meta: dict[str, object] = {"mesh_file": xmdf.name, "time_count": len(times), "dt_available": int(has_dt)}
        return len(times), 0, 0, 0, int(has_dt), meta

    peaks: dict[str, _Peak] = {}
    count_control = 0
    count_dt = 0
    clipped = 0
    last_surfaces: dict[str, XmdfVertexSurface] = {}
    for time_hr in times:
        surfaces = _fetch_surfaces(reader, active_names, time_hr)
        last_surfaces = surfaces
        dt = surfaces.get("dt")
        optional = {
            name: surface for name, surface in surfaces.items() if name in ("dt", "depth", "velocity", "water_level")
        }
        for parameter in REQUIRED_DATASETS:
            surface = surfaces[parameter]
            if parameter not in peaks:
                peaks[parameter] = _Peak(
                    np.full(surface.size, -np.inf, dtype=np.float32),
                    np.full(surface.size, np.nan, dtype=np.float64),
                    np.zeros(surface.size, dtype=np.uint32),
                )
            state = peaks[parameter]
            if state.values.size != surface.size:
                message = f"XMDF {xmdf.name} vertex count changed during processing"
                raise ValueError(message)
            threshold = near_limit_threshold(config.control_limits[parameter], config.near_limit_fraction)
            near = surface.valid & (surface.value >= threshold)
            state.counts[near] += 1
            replace = near & (surface.value > state.values)
            state.values[replace] = surface.value[replace]
            state.times[replace] = time_hr
            rows, total = control_snapshot_records(
                mesh_name=xmdf.name,
                time_hr=time_hr,
                parameter=parameter,
                surface=surface,
                limit=config.control_limits[parameter],
                near_fraction=config.near_limit_fraction,
                maximum_points=config.max_points_per_type,
                optional=optional,
            )
            count_control += _write_points(output, "xmdf_control_hotspots", rows, _CONTROL_SCHEMA)
            clipped += total - len(rows)
        if dt is not None:
            controls = {name: surfaces[name] for name in REQUIRED_DATASETS}
            rows, total = dt_snapshot_records(
                mesh_name=xmdf.name,
                time_hr=time_hr,
                dt=dt,
                controls=controls,
                low_dt=config.low_dt,
                maximum_points=config.max_points_per_type,
            )
            count_dt += _write_points(output, "xmdf_dt_hotspots", rows, _DT_SCHEMA)
            clipped += total - len(rows)

    rows, summary_clipped = _summary_records(mesh_name=xmdf.name, surfaces=last_surfaces, peaks=peaks, config=config)
    count_summary = _write_points(output, "xmdf_control_summary", rows, _SUMMARY_SCHEMA)
    clipped += summary_clipped
    if clipped:
        logger.warning("{} XMDF hotspot records omitted by max_points_per_type cap", clipped)

    metadata: dict[str, object] = {
        "mesh_file": xmdf.name,
        "crs": config.crs,
        "source_kind": "XMDF",
        "spatial_support": "mesh_vertices",
        "time_count": len(times),
        "first_time_hr": times[0],
        "last_time_hr": times[-1],
        "node_count": last_surfaces["Nu"].size,
        "nu_dataset": names["Nu"],
        "nc_dataset": names["Nc"],
        "nd_dataset": names["Nd"],
        "dt_dataset": active_names.get("dt"),
        "dt_available": int(has_dt),
        "low_dt_s": config.low_dt,
        "near_fraction": config.near_limit_fraction,
        "nu_limit": config.control_limits["Nu"],
        "nc_limit": config.control_limits["Nc"],
        "nd_limit": config.control_limits["Nd"],
        "max_points_per_type": config.max_points_per_type,
        "truncated_records": clipped,
    }
    return len(times), count_control, count_dt, count_summary, int(has_dt), metadata


def run_xmdf_hotspot_workflow(config: XmdfHotspotConfig) -> XmdfHotspotResult:
    """Read existing XMDF surfaces without changing input results."""
    wkt = _validate_config(config)
    stamp = _stamp(config.output)
    if config.dry_run:
        samples = 0
        matched_dt = 0
        for source in config.xmdf_files:
            count, _, _, _, dt_present, _ = _process_source(xmdf=source, config=config, output=None)
            samples += count
            matched_dt += dt_present
        logger.success("XMDF dry run: {} files, {} stored output snapshots", len(config.xmdf_files), samples)
        return XmdfHotspotResult(config.output, len(config.xmdf_files), samples, 0, 0, 0, matched_dt, True)

    config.output.parent.mkdir(parents=True, exist_ok=True)
    handle, filename = tempfile.mkstemp(prefix=f".{config.output.stem}_", suffix=".gpkg", dir=config.output.parent)
    os.close(handle)
    staged = Path(filename)
    staged.unlink()
    snapshots = controls = dts = summaries = dt_sources = 0
    metadata: list[dict[str, object]] = []
    try:
        _new_layer(staged, "xmdf_control_hotspots", _CONTROL_SCHEMA, crs_wkt=wkt)
        _new_layer(staged, "xmdf_dt_hotspots", _DT_SCHEMA, crs_wkt=wkt)
        _new_layer(staged, "xmdf_control_summary", _SUMMARY_SCHEMA, crs_wkt=wkt)
        _new_layer(staged, "xmdf_metadata", _METADATA_SCHEMA, crs_wkt=None)
        for source in config.xmdf_files:
            nt, nc, ndt, ns, has_dt, details = _process_source(xmdf=source, config=config, output=staged)
            snapshots += nt
            controls += nc
            dts += ndt
            summaries += ns
            dt_sources += has_dt
            metadata.append(details)
        _write_metadata(staged, metadata)
        if config.output.is_symlink() or _stamp(config.output) != stamp:
            message = f"XMDF output changed while processing: {config.output}"
            raise FileExistsError(message)
        _publish(staged, config.output, overwrite=config.overwrite)
    finally:
        staged.unlink(missing_ok=True)
    logger.success(
        "XMDF hotspots: {} control samples, {} dt samples, {} control summary points in {}",
        controls,
        dts,
        summaries,
        config.output,
    )
    return XmdfHotspotResult(
        config.output, len(config.xmdf_files), snapshots, controls, dts, summaries, dt_sources, False
    )
