"""Managed, bounded-by-grid-size TUFLOW hourly timestep-hotspot workflow."""

from __future__ import annotations

import os
import tempfile
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import fiona
import numpy as np
import rasterio
from loguru import logger

from ...functions.tuflow.hourly_dt_hotspots import (
    CONTROL_PARAMETERS,
    DEFAULT_CONTROL_LIMITS,
    GridDefinition,
    HourlyRasterSet,
    cell_coordinates,
    discover_hourly_rasters,
    hourly_dt_records,
    hourly_parameter_records,
    leading_control,
    overall_dt_records,
    read_hour,
    validate_raster_grids,
)

_DT_SCHEMA: dict[str, str] = {
    "hour": "int",
    "component": "int",
    "severity": "str:16",
    "min_dt_s": "float",
    "cells_low": "int",
    "cells_critical": "int",
    "area_ha": "float",
    "nu_at_min": "float",
    "nc_at_min": "float",
    "nd_at_min": "float",
    "nu_peak": "float",
    "nc_peak": "float",
    "nd_peak": "float",
    "nu_near": "int",
    "nc_near": "int",
    "nd_near": "int",
    "highest_ratio_at_min": "str:8",
}
_PARAMETER_SCHEMA: dict[str, str] = {
    "hour": "int",
    "parameter": "str:4",
    "component": "int",
    "value": "float",
    "limit": "float",
    "ratio": "float",
    "dt_at_peak_s": "float",
    "cells_near": "int",
    "cells_low_dt": "int",
    "area_ha": "float",
}
_METADATA_SCHEMA: dict[str, str] = {
    "run_prefix": "str",
    "input_dir": "str",
    "first_hour": "int",
    "last_hour": "int",
    "hour_count": "int",
    "low_dt_s": "float",
    "critical_dt_s": "float",
    "near_fraction": "float",
    "nu_limit": "float",
    "nc_limit": "float",
    "nd_limit": "float",
    "connectivity": "int",
    "min_component_cells": "int",
}
_OVERALL_SCHEMA: dict[str, str] = {
    "component": "int",
    "worst_hour": "int",
    "min_dt_s": "float",
    "cells_ever": "int",
    "area_ha": "float",
    "max_hours_at_cell": "int",
    "hours_at_worst_cell": "int",
    "sum_cell_hours": "int64",
    "nu_at_worst": "float",
    "nc_at_worst": "float",
    "nd_at_worst": "float",
    "highest_ratio_at_worst": "str:8",
}


@dataclass(slots=True, frozen=True)
class HourlyHotspotConfig:
    """Noninteractive input, threshold and output configuration."""

    grid_dir: Path
    output: Path
    prefix: str | None = None
    min_hour: int | None = None
    max_hour: int | None = None
    low_dt: float = 5.0
    critical_dt: float = 3.0
    near_limit_fraction: float = 0.9
    control_limits: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULT_CONTROL_LIMITS))
    connectivity: int = 8
    min_component_cells: int = 1
    overwrite: bool = False
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class HourlyHotspotResult:
    """Small, stable workflow result suitable for logs and automation."""

    output: Path
    prefix: str
    hours_processed: int
    hourly_dt_hotspots: int
    hourly_parameter_hotspots: int
    overall_dt_hotspots: int
    dry_run: bool


def _validate_settings(config: HourlyHotspotConfig) -> None:
    if not np.isfinite(config.low_dt) or config.low_dt <= 0:
        message = "low_dt must be finite and greater than zero"
        raise ValueError(message)
    if not np.isfinite(config.critical_dt) or config.critical_dt <= 0 or config.critical_dt > config.low_dt:
        message = "critical_dt must be finite, positive and no greater than low_dt"
        raise ValueError(message)
    if not np.isfinite(config.near_limit_fraction) or not 0 < config.near_limit_fraction <= 1:
        message = "near_limit_fraction must be within (0, 1]"
        raise ValueError(message)
    if config.connectivity not in (4, 8) or config.min_component_cells < 1:
        message = "connectivity must be 4 or 8 and min_component_cells must be positive"
        raise ValueError(message)
    if set(config.control_limits) != set(CONTROL_PARAMETERS):
        message = "control_limits must contain exactly Nu, Nc and Nd"
        raise ValueError(message)
    for name, value in config.control_limits.items():
        if not np.isfinite(value) or value <= 0:
            message = f"{name} control limit must be finite and positive"
            raise ValueError(message)


def _validate_output(config: HourlyHotspotConfig) -> None:
    if config.output.suffix.casefold() != ".gpkg":
        message = "Output path must be a .gpkg file"
        raise ValueError(message)
    if not config.dry_run and config.output.is_symlink():
        message = f"Output path must not be a symbolic link: {config.output}"
        raise ValueError(message)
    if not config.dry_run and config.output.exists() and (not config.overwrite or not config.output.is_file()):
        message = f"Output already exists: {config.output}; use --overwrite to replace it"
        raise FileExistsError(message)


def _create_empty_layer(path: Path, layer: str, properties: Mapping[str, str], grid: GridDefinition) -> None:
    schema: dict[str, object] = {"geometry": "Point", "properties": dict(properties)}
    with fiona.open(path, "w", driver="GPKG", layer=layer, schema=schema, crs_wkt=grid.crs.to_wkt()):
        pass


def _write_records(path: Path, layer: str, records: Iterator[dict[str, object]], schema: Mapping[str, str]) -> int:
    written = 0
    with fiona.open(path, "a", layer=layer) as target:
        batch: list[dict[str, object]] = []
        for record in records:
            batch.append(
                {
                    "geometry": record["geometry"],
                    "properties": {field_name: record.get(field_name) for field_name in schema},
                }
            )
            if len(batch) == 1000:
                target.writerecords(batch)
                written += len(batch)
                batch.clear()
        if batch:
            target.writerecords(batch)
            written += len(batch)
    return written


def _write_metadata(
    path: Path,
    *,
    prefix: str,
    hourly: Sequence[HourlyRasterSet],
    config: HourlyHotspotConfig,
) -> None:
    schema: dict[str, object] = {"geometry": "None", "properties": _METADATA_SCHEMA}
    with fiona.open(path, "w", driver="GPKG", layer="analysis_metadata", schema=schema) as target:
        target.write(
            {
                "geometry": None,
                "properties": {
                    "run_prefix": prefix,
                    "input_dir": str(config.grid_dir.absolute()),
                    "first_hour": hourly[0].hour,
                    "last_hour": hourly[-1].hour,
                    "hour_count": len(hourly),
                    "low_dt_s": config.low_dt,
                    "critical_dt_s": config.critical_dt,
                    "near_fraction": config.near_limit_fraction,
                    "nu_limit": config.control_limits["Nu"],
                    "nc_limit": config.control_limits["Nc"],
                    "nd_limit": config.control_limits["Nd"],
                    "connectivity": config.connectivity,
                    "min_component_cells": config.min_component_cells,
                },
            }
        )


def _update_overall(
    *,
    hour: int,
    dt: np.ndarray,
    dt_valid: np.ndarray,
    counts: np.ndarray,
    minimum: np.ndarray,
    hour_of_minimum: np.ndarray,
    low_dt: float,
) -> None:
    mask = dt_valid & (dt > 0) & (dt <= low_dt)
    counts[mask] += 1  # Three-digit hour token means at most 1000 distinct hours.
    newly_worst = mask & (dt < minimum)
    minimum[newly_worst] = dt[newly_worst]
    hour_of_minimum[newly_worst] = hour


def _attach_worst_hour_controls(
    records: Sequence[dict[str, object]],
    raster_sets: Sequence[HourlyRasterSet],
    config: HourlyHotspotConfig,
    grid: GridDefinition,
) -> None:
    """Sample controls at each overall hotspot's worst-dt cell AND worst-dt hour."""
    by_hour: dict[int, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        by_hour[int(record["worst_hour"])].append(record)
    lookup = {raster_set.hour: raster_set for raster_set in raster_sets}
    for hour, grouped in by_hour.items():
        raster_set = lookup[hour]
        coordinates = [
            cell_coordinates(grid.transform, int(record["worst_row"]), int(record["worst_col"])) for record in grouped
        ]
        for parameter in CONTROL_PARAMETERS:
            with rasterio.open(raster_set.paths[parameter]) as source:
                samples = source.sample(coordinates, indexes=1, masked=True)
                for record, sample in zip(grouped, samples, strict=True):
                    value = sample[0]
                    record[f"{parameter.lower()}_at_worst"] = (
                        None if np.ma.is_masked(value) or not np.isfinite(value) else float(value)
                    )
        for record in grouped:
            at_worst = {name: cast("float | None", record[f"{name.lower()}_at_worst"]) for name in CONTROL_PARAMETERS}
            record["highest_ratio_at_worst"] = leading_control(at_worst, config.control_limits)
            record.pop("worst_row")
            record.pop("worst_col")


def _output_stamp(path: Path) -> tuple[int, int, int, int] | None:
    """Detect unexpected replacement/changes while a new GeoPackage is prepared."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def run_hourly_dt_hotspot_workflow(config: HourlyHotspotConfig) -> HourlyHotspotResult:
    """Validate quartets, stream hourly features and safely publish a GPKG.

    Parameter peaks within an hourly component use the same timestamp, but
    may be at different cells. The overall union represents recurrence by cell.
    """
    _validate_settings(config)
    _validate_output(config)
    expected_output_stamp = _output_stamp(config.output)
    prefix, hourly = discover_hourly_rasters(
        config.grid_dir, prefix=config.prefix, min_hour=config.min_hour, max_hour=config.max_hour
    )
    grid = validate_raster_grids(hourly)
    logger.info(
        "Found {} complete hourly raster sets for {} ({} cells per raster)",
        len(hourly),
        prefix,
        np.prod(grid.shape),
    )
    if config.dry_run:
        logger.success("Dry run: validated {} hourly sets; output would be {}", len(hourly), config.output)
        return HourlyHotspotResult(config.output, prefix, len(hourly), 0, 0, 0, True)

    config.output.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{config.output.stem}_", suffix=".gpkg", dir=config.output.parent)
    os.close(fd)
    temp_path = Path(tmp_name)
    temp_path.unlink()
    hourly_dt_count = 0
    parameter_count = 0
    overall_count = 0
    try:
        _create_empty_layer(temp_path, "hourly_dt_hotspots", _DT_SCHEMA, grid)
        _create_empty_layer(temp_path, "hourly_parameter_hotspots", _PARAMETER_SCHEMA, grid)
        counts = np.zeros(grid.shape, dtype=np.uint16)
        minimum = np.full(grid.shape, np.inf, dtype=np.float32)
        hour_of_minimum = np.full(grid.shape, -1, dtype=np.int16)
        for processed, raster_set in enumerate(hourly, start=1):
            arrays = read_hour(raster_set)
            dt, dt_valid = arrays["dt"]
            _update_overall(
                hour=raster_set.hour,
                dt=dt,
                dt_valid=dt_valid,
                counts=counts,
                minimum=minimum,
                hour_of_minimum=hour_of_minimum,
                low_dt=config.low_dt,
            )
            hourly_dt_count += _write_records(
                temp_path,
                "hourly_dt_hotspots",
                hourly_dt_records(
                    hour=raster_set.hour,
                    arrays=arrays,
                    grid=grid,
                    low_dt=config.low_dt,
                    critical_dt=config.critical_dt,
                    near_limit_fraction=config.near_limit_fraction,
                    limits=config.control_limits,
                    connectivity=config.connectivity,
                    min_cells=config.min_component_cells,
                ),
                _DT_SCHEMA,
            )
            parameter_count += _write_records(
                temp_path,
                "hourly_parameter_hotspots",
                hourly_parameter_records(
                    hour=raster_set.hour,
                    arrays=arrays,
                    grid=grid,
                    low_dt=config.low_dt,
                    near_limit_fraction=config.near_limit_fraction,
                    limits=config.control_limits,
                    connectivity=config.connectivity,
                    min_cells=config.min_component_cells,
                ),
                _PARAMETER_SCHEMA,
            )
            logger.info("Processed timestep output {} of {}: hour {}", processed, len(hourly), raster_set.hour)
            del arrays

        overall_records = list(
            overall_dt_records(
                counts=counts,
                minimum=minimum,
                hour_of_minimum=hour_of_minimum,
                grid=grid,
                connectivity=config.connectivity,
                min_cells=config.min_component_cells,
            )
        )
        _attach_worst_hour_controls(overall_records, hourly, config, grid)
        _create_empty_layer(temp_path, "overall_dt_hotspots", _OVERALL_SCHEMA, grid)
        overall_count = _write_records(temp_path, "overall_dt_hotspots", iter(overall_records), _OVERALL_SCHEMA)
        _write_metadata(temp_path, prefix=prefix, hourly=hourly, config=config)
        if config.output.is_symlink() or _output_stamp(config.output) != expected_output_stamp:
            message = f"Output was created or changed during processing: {config.output}"
            raise FileExistsError(message)
        temp_path.replace(config.output)
    finally:
        temp_path.unlink(missing_ok=True)
    logger.success(
        "Created {}: {} hourly dt hotspots, {} hourly parameter hotspots, {} overall hotspots",
        config.output,
        hourly_dt_count,
        parameter_count,
        overall_count,
    )
    return HourlyHotspotResult(
        config.output, prefix, len(hourly), hourly_dt_count, parameter_count, overall_count, False
    )
