r"""Crop a scenario's ordered TUFLOW terrain stack while retaining buffered seams.

PyTUFLOW evaluates ``If Scenario`` scopes for every ``Read Grid Zpts`` command.
The selected rasters are clipped to one analysis box and lower-priority data is
removed where later terrain provides coverage.  Each layer retains a configurable
buffer around its uniquely required area so adjacent rasters still overlap.

The shipped settings are plan-only.  ``--write`` creates new rasters, a new TGC
beside the source TGC, and a JSON manifest.  It never edits or overwrites inputs.

Example::

    python crop_tgc_terrains.py --tgc model\GEOM_01.tgc --scenario EXG \
        --bounds 700000 7400000 750000 7450000 --crs EPSG:28350 \
        --buffer 50 --output-dir terrain\cropped_EXG --write --no-pause
"""

from __future__ import annotations

# Rasterio, GeoPandas and PyTUFLOW do not currently expose complete type information.
# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false
# pyright: reportCallIssue=false, reportGeneralTypeIssues=false
from pathlib import Path

WRAPPER_VERSION = "2026-11-08.2"

TGC = Path("model/GEOM_01.tgc")
SCENARIO = "EXG"
BOUNDS: tuple[float, float, float, float] | None = None
CRS = "EPSG:28350"
SEAM_BUFFER_METRES = 50.0
PLANNING_RESOLUTION_METRES = 10.0
OUTPUT_DIRECTORY = Path("terrain/cropped_EXG")
OUTPUT_PROFILE = "tuflow"
WRITE = False
PAUSE_ON_FINISH = True

import argparse
import json
import math
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any, cast

import fiona
import geopandas as gpd
import numpy as np
import rasterio
import rasterio.warp
from pytuflow import TGC as PyTuflowTGC
from pytuflow import Context
from rasterio.enums import Resampling
from rasterio.features import geometry_mask, shapes
from rasterio.transform import array_bounds, from_origin
from rasterio.windows import Window, from_bounds
from scipy.ndimage import binary_dilation
from shapely import box as vector_box
from shapely import covers as covers_geometry
from shapely import union_all
from shapely.geometry import box, mapping, shape

from ryan_library.functions.gdal.raster_processing import geotiff_creation_options


@dataclass(frozen=True, slots=True)
class AnalysisBounds:
    """Axis-aligned analysis limits in the requested projected CRS."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float

    def validate(self) -> None:
        values = (self.xmin, self.ymin, self.xmax, self.ymax)
        if not all(math.isfinite(value) for value in values):
            message = f"BOUNDS must contain finite numbers, received {values!r}"
            raise ValueError(message)
        if self.xmin >= self.xmax or self.ymin >= self.ymax:
            message = f"BOUNDS must be xmin ymin xmax ymax, received {values!r}"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class TerrainLayer:
    """One PyTUFLOW-selected terrain read in original command order."""

    order: int
    line_number: int
    source: Path
    mask: Path | None
    source_expression: str
    command_text: str


@dataclass(slots=True)
class PlannedLayer:
    """Coverage allocation and output metadata for one terrain layer."""

    layer: TerrainLayer
    output: Path
    active_cells: int
    responsibility_cells: int
    retained_cells: int
    omitted: bool
    retained_mask: np.ndarray[Any, np.dtype[np.bool_]]


@dataclass(frozen=True, slots=True)
class PlanningGrid:
    """Common coarse grid used only for coverage allocation."""

    transform: Any
    width: int
    height: int
    resolution: float
    crs: str

    @property
    def shape(self) -> tuple[int, int]:
        return self.height, self.width


def _absolute(path: Path, base: Path | None = None) -> Path:
    candidate = path if path.is_absolute() else (base or Path.cwd()) / path
    return candidate.absolute()


def _normalise_scenario(value: str) -> str:
    scenario = value.strip()
    if not scenario or re.search(r"\s", scenario):
        message = f"SCENARIO must be one non-blank TUFLOW scenario value, received {value!r}"
        raise ValueError(message)
    return scenario


def _planning_grid(bounds: AnalysisBounds, resolution: float, crs: str) -> PlanningGrid:
    if not math.isfinite(resolution) or resolution <= 0:
        message = f"PLANNING_RESOLUTION_METRES must be positive, received {resolution!r}"
        raise ValueError(message)
    width = math.ceil((bounds.xmax - bounds.xmin) / resolution)
    height = math.ceil((bounds.ymax - bounds.ymin) / resolution)
    return PlanningGrid(
        from_origin(bounds.xmin, bounds.ymax, resolution, resolution),
        width,
        height,
        resolution,
        crs,
    )


def _load_active_variables(model: Any, context: Any) -> None:
    variables: dict[str, str] = {}
    for variable_input in model.find_input(lhs="set variable", recursive=True):
        if context.in_context_by_scope(variable_input.scope):
            name, value = variable_input.command().parse_variable()
            variables[str(name)] = str(value)
    context.load_variables(variables)


def resolve_terrain_layers(tgc_path: Path, scenario: str) -> list[TerrainLayer]:
    """Use PyTUFLOW to select ordered terrain reads for one scenario value."""
    absolute_tgc = _absolute(tgc_path)
    if not absolute_tgc.is_file():
        message = f"TGC does not exist: {absolute_tgc}"
        raise FileNotFoundError(message)

    model = PyTuflowTGC(absolute_tgc)
    if not model.loaded:
        message = f"PyTUFLOW could not load TGC: {absolute_tgc}"
        raise ValueError(message)
    context = Context(f"-s1 {_normalise_scenario(scenario)}", config=model.config)
    context.file_resolution = "deferred"
    _load_active_variables(model, context)

    selected: list[TerrainLayer] = []
    for build_input in model.find_input(lhs="Read Grid Zpts", recursive=True):  # pyright: ignore[reportArgumentType]
        if not context.in_context_by_scope(build_input.scope):
            continue
        if build_input.trd is not None:
            message = (
                f"Terrain commands in Read File includes are not supported: {build_input.trd}. "
                "Move active Read Grid Zpts commands into the root TGC before cropping."
            )
            raise ValueError(message)
        run_input = build_input.context(context=context)
        references = cast(tuple[Any, ...], tuple(run_input.file_references))
        if not references:
            message = f"Line {run_input.line_number} has no resolved raster reference: {run_input.rhs}"
            raise ValueError(message)
        if len(references) > 2:
            message = f"Line {run_input.line_number} has unsupported extra references: {run_input.rhs}"
            raise ValueError(message)

        source = _absolute(Path(str(references[0].path_pattern)))
        mask_path = _absolute(Path(str(references[1].path_pattern))) if len(references) == 2 else None
        source_expression = str(run_input.rhs).split("|", maxsplit=1)[0].strip()
        selected.append(
            TerrainLayer(
                order=len(selected) + 1,
                line_number=int(run_input.line_number),
                source=source,
                mask=mask_path,
                source_expression=source_expression,
                command_text=str(run_input.command()).strip(),
            )
        )

    if not selected:
        message = f"No active Read Grid Zpts commands found for scenario {scenario!r}"
        raise ValueError(message)
    return selected


def _read_mask_geometries(mask_path: Path, destination_crs: str) -> list[dict[str, Any]]:
    if not mask_path.is_file():
        message = f"Terrain clip mask does not exist: {mask_path}"
        raise FileNotFoundError(message)
    frame = gpd.read_file(mask_path)
    if frame.crs is None:
        message = f"Terrain clip mask has no CRS: {mask_path}"
        raise ValueError(message)
    frame = frame.to_crs(destination_crs)
    geometries = [mapping(geometry) for geometry in frame.geometry.dropna() if not geometry.is_empty]
    if not geometries:
        message = f"Terrain clip mask contains no usable geometry: {mask_path}"
        raise ValueError(message)
    return geometries


def _native_mask_on_grid(
    layer: TerrainLayer,
    grid: PlanningGrid,
    resampling: Resampling,
) -> np.ndarray[Any, np.dtype[np.bool_]]:
    """Aggregate real native validity masks in bounded windows, without lossy overviews.

    MAX reports any source elevations for the inventory. MIN certifies every
    contributing source pixel is valid; uncertain cells cannot erase lower terrain.
    """
    output = np.zeros(grid.shape, dtype=bool)
    with rasterio.open(layer.source) as source:
        scale = max(1.0, grid.resolution / source.res[0]) * max(1.0, grid.resolution / source.res[1])
        block = max(1, min(256, int(math.sqrt(4_000_000 / scale))))
        for row in range(0, grid.height, block):
            for col in range(0, grid.width, block):
                destination = Window(col, row, min(block, grid.width - col), min(block, grid.height - row))
                tile_transform = rasterio.windows.transform(destination, grid.transform)
                west, south, east, north = array_bounds(int(destination.height), int(destination.width), tile_transform)
                if (
                    east <= source.bounds.left
                    or west >= source.bounds.right
                    or north <= source.bounds.bottom
                    or south >= source.bounds.top
                ):
                    continue
                requested = from_bounds(west, south, east, north, source.transform)
                c0 = max(0, math.floor(requested.col_off) - 1)
                c1 = min(source.width, math.ceil(requested.col_off + requested.width) + 1)
                r0 = max(0, math.floor(requested.row_off) - 1)
                r1 = min(source.height, math.ceil(requested.row_off + requested.height) + 1)
                if c0 >= c1 or r0 >= r1:
                    continue
                if (c1 - c0) * (r1 - r0) > 16_000_000:
                    if resampling == Resampling.min:
                        continue  # Cannot certify full native coverage; preserve lower-priority raster.
                    message = f"Source mask window too large for native QA: {layer.source}; refine the planning grid"
                    raise ValueError(message)
                source_window = Window(c0, r0, c1 - c0, r1 - r0)
                source_mask = source.read_masks(1, window=source_window)
                sampled = np.zeros((int(destination.height), int(destination.width)), dtype=np.uint8)
                rasterio.warp.reproject(
                    source=source_mask,
                    destination=sampled,
                    src_transform=rasterio.windows.transform(source_window, source.transform),
                    src_crs=source.crs,
                    dst_transform=tile_transform,
                    dst_crs=grid.crs,
                    resampling=resampling,
                )
                valid = sampled > 0 if resampling == Resampling.max else sampled == 255
                if resampling == Resampling.min:
                    xs = west + np.arange(int(destination.width)) * grid.resolution
                    ys = north - np.arange(int(destination.height)) * grid.resolution
                    valid &= (
                        (xs[None, :] >= source.bounds.left)
                        & (xs[None, :] + grid.resolution <= source.bounds.right)
                        & (ys[:, None] <= source.bounds.top)
                        & (ys[:, None] - grid.resolution >= source.bounds.bottom)
                    )
                output[row : row + int(destination.height), col : col + int(destination.width)] = valid
    return output


def _confirmed_replacement(
    layer: TerrainLayer,
    grid: PlanningGrid,
    bounds: AnalysisBounds,
) -> np.ndarray[Any, np.dtype[np.bool_]]:
    """Certify valid native pixels and complete polygon-clip coverage per cell."""
    confirmed = _native_mask_on_grid(layer, grid, Resampling.min)
    if not confirmed.any():
        return confirmed
    xs = grid.transform.c + np.arange(grid.width) * grid.resolution
    ys = grid.transform.f - np.arange(grid.height) * grid.resolution
    confirmed &= (
        (xs[None, :] >= bounds.xmin)
        & (xs[None, :] + grid.resolution <= bounds.xmax)
        & (ys[:, None] <= bounds.ymax)
        & (ys[:, None] - grid.resolution >= bounds.ymin)
    )
    if layer.mask is None or not confirmed.any():
        return confirmed
    polygon = union_all([shape(item) for item in _read_mask_geometries(layer.mask, grid.crs)])
    if not polygon.is_valid:
        message = f"Invalid TGC polygon clip: {layer.mask}"
        raise ValueError(message)
    for row in range(0, grid.height, 256):
        for col in range(0, grid.width, 256):
            tile = confirmed[row : row + 256, col : col + 256]
            rows, cols = np.nonzero(tile)
            if not rows.size:
                continue
            west = grid.transform.c + (col + cols) * grid.resolution
            north = grid.transform.f - (row + rows) * grid.resolution
            rectangles = vector_box(west, north - grid.resolution, west + grid.resolution, north)
            tile[rows, cols] &= np.asarray(covers_geometry(polygon, rectangles), dtype=bool)
    return confirmed


def _coverage_mask(
    layer: TerrainLayer,
    grid: PlanningGrid,
    bounds: AnalysisBounds,
) -> np.ndarray[Any, np.dtype[np.bool_]]:
    if not layer.source.is_file():
        message = f"Terrain raster does not exist: {layer.source}"
        raise FileNotFoundError(message)
    with rasterio.Env(GDAL_CACHEMAX=256), rasterio.open(layer.source) as source:
        if source.count != 1:
            message = f"Terrain must contain exactly one band: {layer.source} has {source.count}"
            raise ValueError(message)
        if source.crs is None:
            message = f"Terrain has no CRS: {layer.source}"
            raise ValueError(message)
        if source.crs != rasterio.crs.CRS.from_string(grid.crs):
            message = f"Terrain CRS must be {grid.crs}: {layer.source} is {source.crs}"
            raise ValueError(message)
        if source.nodata is None:
            message = f"Terrain must declare NoData before safe overlap trimming: {layer.source}"
            raise ValueError(message)
        if not source.transform.is_rectilinear or source.transform.a <= 0 or source.transform.e >= 0:
            message = f"Terrain must use a north-up rectilinear grid: {layer.source}"
            raise ValueError(message)
    coverage = _native_mask_on_grid(layer, grid, Resampling.max)

    analysis_geometry = mapping(box(bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax))
    coverage &= geometry_mask(
        [analysis_geometry],
        out_shape=grid.shape,
        transform=grid.transform,
        invert=True,
        all_touched=True,
    )
    if layer.mask is not None:
        coverage &= geometry_mask(
            _read_mask_geometries(layer.mask, grid.crs),
            out_shape=grid.shape,
            transform=grid.transform,
            invert=True,
            all_touched=True,
        )
    return coverage


def _buffer_binary(mask: np.ndarray[Any, np.dtype[np.bool_]], cells: int) -> np.ndarray[Any, np.dtype[np.bool_]]:
    if cells <= 0 or not mask.any():
        return mask.copy()
    axis = np.arange(-cells, cells + 1)
    yy, xx = np.meshgrid(axis, axis, indexing="ij")
    structure = (xx * xx + yy * yy) <= cells * cells
    return np.asarray(binary_dilation(mask, structure=structure), dtype=bool)


def allocate_retained_coverage(
    coverages: Sequence[np.ndarray[Any, np.dtype[np.bool_]]],
    buffer_cells: int,
    replacements: Sequence[np.ndarray[Any, np.dtype[np.bool_]]] | None = None,
) -> list[tuple[np.ndarray[Any, np.dtype[np.bool_]], int]]:
    """Remove lower-priority data only under conservatively certified coverage."""
    if not coverages:
        return []
    shape = coverages[0].shape
    if any(coverage.shape != shape for coverage in coverages):
        message = "All planning coverage masks must share one shape"
        raise ValueError(message)
    confirmed = coverages if replacements is None else replacements
    if len(confirmed) != len(coverages) or any(item.shape != shape for item in confirmed):
        message = "Confirmed replacement masks must match source layers and grid"
        raise ValueError(message)

    later_certified = np.zeros(shape, dtype=bool)
    retained_reversed: list[tuple[np.ndarray[Any, np.dtype[np.bool_]], int]] = []
    for coverage, replacement in reversed(tuple(zip(coverages, confirmed, strict=True))):
        responsibility = coverage & ~later_certified
        retained = coverage & _buffer_binary(responsibility, buffer_cells)
        retained_reversed.append((retained, int(np.count_nonzero(responsibility))))
        later_certified |= replacement
    retained_reversed.reverse()
    return retained_reversed


def _safe_output_name(layer: TerrainLayer) -> str:
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", layer.source.stem).strip("._") or "terrain"
    return f"{layer.order:03d}_{safe_stem}_cropped.tif"


def build_plan(
    layers: Sequence[TerrainLayer],
    output_directory: Path,
    bounds: AnalysisBounds,
    crs: str,
    planning_resolution: float,
    seam_buffer: float,
) -> tuple[PlanningGrid, list[PlannedLayer]]:
    """Inspect source coverage and calculate buffered retained areas."""
    if not math.isfinite(seam_buffer) or seam_buffer < 0:
        message = f"SEAM_BUFFER_METRES must be non-negative, received {seam_buffer!r}"
        raise ValueError(message)
    grid = _planning_grid(bounds, planning_resolution, crs)
    coverages: list[np.ndarray[Any, np.dtype[np.bool_]]] = []
    replacements: list[np.ndarray[Any, np.dtype[np.bool_]]] = []
    for index, layer in enumerate(layers, start=1):
        print(f"Inspecting terrain coverage {index}/{len(layers)}: {layer.source}")
        coverages.append(_coverage_mask(layer, grid, bounds))
        replacements.append(_confirmed_replacement(layer, grid, bounds))
    buffer_cells = math.ceil(seam_buffer / planning_resolution)
    allocations = allocate_retained_coverage(coverages, buffer_cells, replacements)
    destination = _absolute(output_directory)
    plan = [
        PlannedLayer(
            layer=layer,
            output=destination / _safe_output_name(layer),
            active_cells=int(np.count_nonzero(coverage)),
            responsibility_cells=responsibility_cells,
            retained_cells=int(np.count_nonzero(retained)),
            omitted=not retained.any(),
            retained_mask=retained,
        )
        for layer, coverage, (retained, responsibility_cells) in zip(layers, coverages, allocations, strict=True)
    ]
    return grid, plan


def _source_window(source: Any, bounds: tuple[float, float, float, float]) -> Window:
    left, bottom, right, top = bounds
    col_start = math.floor((left - source.transform.c) / source.transform.a)
    col_stop = math.ceil((right - source.transform.c) / source.transform.a)
    row_start = math.floor((source.transform.f - top) / -source.transform.e)
    row_stop = math.ceil((source.transform.f - bottom) / -source.transform.e)
    col_start = max(0, min(source.width, col_start))
    col_stop = max(0, min(source.width, col_stop))
    row_start = max(0, min(source.height, row_start))
    row_stop = max(0, min(source.height, row_stop))
    if col_start >= col_stop or row_start >= row_stop:
        message = "Retained coverage does not intersect the source raster"
        raise ValueError(message)
    width, height = col_stop - col_start, row_stop - row_start
    return Window(col_start, row_start, width, height)


def _retained_bounds(
    mask: np.ndarray[Any, np.dtype[np.bool_]], grid: PlanningGrid
) -> tuple[float, float, float, float]:
    rows, cols = np.nonzero(mask)
    if rows.size == 0:
        message = "Cannot calculate bounds for empty retained coverage"
        raise ValueError(message)
    row_start, row_stop = int(rows.min()), int(rows.max()) + 1
    col_start, col_stop = int(cols.min()), int(cols.max()) + 1
    transform = rasterio.windows.transform(
        Window(col_start, row_start, col_stop - col_start, row_stop - row_start),
        grid.transform,
    )
    west, south, east, north = array_bounds(row_stop - row_start, col_stop - col_start, transform)
    return west, south, east, north


def _output_profile(source: Any, window: Window) -> dict[str, Any]:
    profile = dict(source.profile)
    width, height = int(window.width), int(window.height)
    options = dict(option.split("=", maxsplit=1) for option in geotiff_creation_options(OUTPUT_PROFILE, "ALL_CPUS"))
    profile.update(
        driver="GTiff",
        width=width,
        height=height,
        transform=rasterio.windows.transform(window, source.transform),
        compress=options["COMPRESS"],
        predictor=int(options["PREDICTOR"]),
        BIGTIFF=options["BIGTIFF"],
        NUM_THREADS=options["NUM_THREADS"],
        tiled=False,
    )
    profile.pop("blockxsize", None)
    profile.pop("blockysize", None)
    return profile


def _write_layer(planned: PlannedLayer, grid: PlanningGrid, bounds: AnalysisBounds) -> None:
    retained_bounds = _retained_bounds(planned.retained_mask, grid)
    requested = (bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax)
    intersected = (
        max(retained_bounds[0], requested[0]),
        max(retained_bounds[1], requested[1]),
        min(retained_bounds[2], requested[2]),
        min(retained_bounds[3], requested[3]),
    )
    with rasterio.open(planned.layer.source) as source:
        source_bounds = (
            source.bounds.left,
            source.bounds.bottom,
            source.bounds.right,
            source.bounds.top,
        )
        clipped = (
            max(intersected[0], source_bounds[0]),
            max(intersected[1], source_bounds[1]),
            min(intersected[2], source_bounds[2]),
            min(intersected[3], source_bounds[3]),
        )
        source_window = _source_window(source, clipped)
        profile = _output_profile(source, source_window)
        planned.output.parent.mkdir(parents=True, exist_ok=True)
        retained_source = planned.retained_mask.view(np.uint8)
        descriptor = os.open(planned.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        os.close(descriptor)
        complete = False
        try:
            with rasterio.open(planned.output, "w", **profile) as destination:
                destination.update_tags(**source.tags())
                destination.update_tags(1, **source.tags(1))
                for _, destination_window in destination.block_windows(1):
                    read_window = Window(
                        source_window.col_off + destination_window.col_off,
                        source_window.row_off + destination_window.row_off,
                        destination_window.width,
                        destination_window.height,
                    )
                    data = source.read(1, window=read_window)
                    source_valid = source.read_masks(1, window=read_window) > 0
                    destination_transform = rasterio.windows.transform(read_window, source.transform)
                    retained = np.zeros(data.shape, dtype=np.uint8)
                    rasterio.warp.reproject(
                        source=retained_source,
                        destination=retained,
                        src_transform=grid.transform,
                        src_crs=grid.crs,
                        dst_transform=destination_transform,
                        dst_crs=source.crs,
                        resampling=Resampling.nearest,
                    )
                    inside = geometry_mask(
                        [mapping(box(bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax))],
                        out_shape=data.shape,
                        transform=destination_transform,
                        invert=True,
                        all_touched=False,
                    )
                    valid = source_valid & (retained > 0) & inside
                    # Discovery rejects sources without a nodata value.
                    data[~valid] = cast("float", source.nodata)
                    destination.write(data, 1, window=destination_window)
            complete = True
        finally:
            if not complete:
                planned.output.unlink(missing_ok=True)


def _patched_tgc_path(tgc_path: Path, scenario: str) -> Path:
    return tgc_path.with_name(f"{tgc_path.stem}_{scenario}_cropped{tgc_path.suffix}")


def _write_patched_tgc(tgc_path: Path, scenario: str, plan: Sequence[PlannedLayer]) -> Path:
    destination = _patched_tgc_path(tgc_path, scenario)
    if destination.exists():
        message = f"Patched TGC already exists: {destination}"
        raise FileExistsError(message)
    lines = tgc_path.read_text(encoding="utf-8-sig").splitlines(keepends=True)
    for planned in plan:
        index = planned.layer.line_number - 1
        if index < 0 or index >= len(lines):
            message = f"PyTUFLOW line number is outside the TGC: {planned.layer.line_number}"
            raise ValueError(message)
        if planned.omitted:
            lines[index] = f"! Omitted by terrain crop: {lines[index].lstrip()}"
            continue
        relative = os.path.relpath(planned.output, start=tgc_path.parent).replace("/", "\\")
        if planned.layer.source_expression not in lines[index]:
            message = (
                f"Could not safely replace terrain path on TGC line {planned.layer.line_number}: "
                f"{planned.layer.source_expression!r}"
            )
            raise ValueError(message)
        lines[index] = lines[index].replace(planned.layer.source_expression, relative, 1)
    with destination.open("x", encoding="utf-8", newline="") as output:
        output.write("".join(lines))
    return destination


def _manifest_payload(
    tgc_path: Path,
    patched_tgc: Path | None,
    scenario: str,
    bounds: AnalysisBounds,
    grid: PlanningGrid,
    seam_buffer: float,
    plan: Sequence[PlannedLayer],
) -> dict[str, Any]:
    return {
        "tool": "crop_tgc_terrains.py",
        "wrapper_version": WRAPPER_VERSION,
        "source_tgc": str(tgc_path),
        "patched_tgc": str(patched_tgc) if patched_tgc else None,
        "scenario_flag": f"-s1 {scenario}",
        "bounds": asdict(bounds),
        "crs": grid.crs,
        "planning_resolution_metres": grid.resolution,
        "seam_buffer_metres": seam_buffer,
        "output_raster_profile": {
            "name": OUTPUT_PROFILE,
            "creation_options": geotiff_creation_options(OUTPUT_PROFILE, "ALL_CPUS"),
        },
        "layers": [
            {
                "order": item.layer.order,
                "line_number": item.layer.line_number,
                "source": str(item.layer.source),
                "mask": str(item.layer.mask) if item.layer.mask else None,
                "output": None if item.omitted else str(item.output),
                "active_planning_cells": item.active_cells,
                "unique_responsibility_cells": item.responsibility_cells,
                "retained_planning_cells": item.retained_cells,
                "omitted": item.omitted,
            }
            for item in plan
        ],
    }


def _qa_properties(planned: PlannedLayer | None, area_m2: float, method: str) -> dict[str, Any]:
    """Common attributes preserve source identity and TGC precedence in QGIS."""
    return {
        "layer_order": planned.layer.order if planned is not None else 0,
        "tgc_line": planned.layer.line_number if planned is not None else 0,
        "source": str(planned.layer.source) if planned is not None else "",
        "clip": str(planned.layer.mask) if planned is not None and planned.layer.mask else "",
        "output": str(planned.output) if planned is not None and not planned.omitted else "",
        "area_m2": area_m2,
        "method": method,
    }


def _qa_add_geometry(
    path: Path,
    layer: str,
    geometry: dict[str, Any],
    planned: PlannedLayer | None,
    method: str,
) -> None:
    with fiona.open(path, mode="a", layer=layer) as output:
        output.write(
            {
                "geometry": geometry,
                "properties": _qa_properties(planned, float(shape(geometry).area), method),
            }
        )


def _qa_add_mask(
    path: Path,
    layer: str,
    mask: np.ndarray[Any, np.dtype[np.bool_]],
    grid: PlanningGrid,
    planned: PlannedLayer | None,
    method: str,
) -> None:
    if not mask.any():
        return
    with fiona.open(path, mode="a", layer=layer) as output:
        for geometry, value in shapes(mask.astype("uint8"), mask=mask, transform=grid.transform):
            if value == 1:
                output.write(
                    {
                        "geometry": geometry,
                        "properties": _qa_properties(planned, float(shape(geometry).area), method),
                    }
                )


def _write_coverage_qa(
    output_directory: Path,
    grid: PlanningGrid,
    bounds: AnalysisBounds,
    plan: Sequence[PlannedLayer],
) -> dict[str, Any]:
    """Publish an auditable QGIS GPKG of source and final coverage at planning resolution.

    Source footprints are full rectangular raster bounds. All other per-layer
    polygonised coverage uses valid-data masks after any supplied TGC polygon
    clip, restricted to the requested bounding box. This is not native-cell
    coverage verification and must not replace TUFLOW equivalence testing.
    """
    path = output_directory / "terrain_coverage_qa.gpkg"
    if path.exists():
        message = f"Refusing to overwrite existing terrain coverage QA: {path}"
        raise FileExistsError(message)
    schema = {
        "geometry": "Polygon",
        "properties": {
            "layer_order": "int",
            "tgc_line": "int",
            "source": "str",
            "clip": "str",
            "output": "str",
            "area_m2": "float",
            "method": "str",
        },
    }
    names = (
        "original_raster_footprints",
        "original_valid_coverage",
        "clipped_out",
        "minimum_coverage_extent",
        "final_clipped_extent",
        "coverage_gaps",
        "lost_valid_coverage",
    )
    for name in names:
        with fiona.open(path, mode="w", driver="GPKG", layer=name, crs=grid.crs, schema=schema):
            pass

    requested_geometry = mapping(box(bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax))
    _qa_add_geometry(path, "minimum_coverage_extent", requested_geometry, None, "requested_exact_bounds")
    original_union = np.zeros(grid.shape, dtype=bool)
    final_union = np.zeros(grid.shape, dtype=bool)
    for planned in plan:
        with rasterio.open(planned.layer.source) as source:
            _qa_add_geometry(
                path,
                "original_raster_footprints",
                mapping(box(*source.bounds)),
                planned,
                "full_raster_bounds_including_nodata",
            )
        original = _coverage_mask(planned.layer, grid, bounds)
        final = (
            _coverage_mask(replace(planned.layer, source=planned.output), grid, bounds)
            if not planned.omitted
            else np.zeros(grid.shape, dtype=bool)
        )
        _qa_add_mask(path, "original_valid_coverage", original, grid, planned, "original_planning_valid")
        _qa_add_mask(path, "clipped_out", original & ~final, grid, planned, "removed_planning_valid")
        _qa_add_mask(path, "final_clipped_extent", final, grid, planned, "written_raster_planning_valid")
        original_union |= original
        final_union |= final

    required = geometry_mask(
        [requested_geometry],
        out_shape=grid.shape,
        transform=grid.transform,
        invert=True,
        all_touched=True,
    )
    gap = required & ~final_union
    lost = original_union & ~final_union
    _qa_add_mask(path, "coverage_gaps", gap, grid, None, "uncovered_required_planning_cells")
    _qa_add_mask(path, "lost_valid_coverage", lost, grid, None, "valid_original_missing_after_crop")
    return {
        "gpkg": str(path),
        "planning_resolution_m": grid.resolution,
        "original_uncovered_planning_cells": int(np.count_nonzero(required & ~original_union)),
        "final_uncovered_planning_cells": int(np.count_nonzero(gap)),
        "lost_original_planning_cells": int(np.count_nonzero(lost)),
        "method": "overview_aware_planning_masks_not_native_resolution_verification",
    }


def _check_write_targets(tgc_path: Path, scenario: str, output_directory: Path, plan: Sequence[PlannedLayer]) -> None:
    targets = [item.output for item in plan if not item.omitted]
    targets.extend(
        (
            _patched_tgc_path(tgc_path, scenario),
            output_directory / "terrain_crop_manifest.json",
            output_directory / "terrain_coverage_qa.gpkg",
        )
    )
    existing = [path for path in targets if path.exists()]
    if existing:
        listing = "\n".join(f"  {path}" for path in existing)
        message = f"Refusing to overwrite existing outputs:\n{listing}"
        raise FileExistsError(message)


def run(
    *,
    tgc_path: Path,
    scenario: str,
    bounds: AnalysisBounds,
    crs: str,
    seam_buffer: float,
    planning_resolution: float,
    output_directory: Path,
    write: bool,
) -> dict[str, Any]:
    """Plan or write a single-scenario terrain crop workflow."""
    bounds.validate()
    absolute_tgc = _absolute(tgc_path)
    absolute_output = _absolute(output_directory)
    layers = resolve_terrain_layers(absolute_tgc, scenario)
    grid, plan = build_plan(layers, absolute_output, bounds, crs, planning_resolution, seam_buffer)

    patched_tgc: Path | None = None
    if write:
        _check_write_targets(absolute_tgc, scenario, absolute_output, plan)
        absolute_output.mkdir(parents=True, exist_ok=True)
        for item in plan:
            if not item.omitted:
                _write_layer(item, grid, bounds)
        qa = _write_coverage_qa(absolute_output, grid, bounds, plan)
        if qa["lost_original_planning_cells"] or qa["final_uncovered_planning_cells"]:
            message = (
                "Unsafe terrain crop: valid original coverage was lost or required coverage is missing "
                f"(lost={qa['lost_original_planning_cells']}, "
                f"uncovered={qa['final_uncovered_planning_cells']}). "
                f"QA preserved at {qa['gpkg']}; patched TGC was not published."
            )
            raise ValueError(message)
        patched_tgc = _write_patched_tgc(absolute_tgc, scenario, plan)
        payload = _manifest_payload(absolute_tgc, patched_tgc, scenario, bounds, grid, seam_buffer, plan)
        payload["coverage_qa"] = qa
        manifest = absolute_output / "terrain_crop_manifest.json"
        with manifest.open("x", encoding="utf-8") as output:
            output.write(json.dumps(payload, indent=2))
        return payload
    payload = _manifest_payload(absolute_tgc, None, scenario, bounds, grid, seam_buffer, plan)
    payload["coverage_qa"] = None
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crop a PyTUFLOW-selected terrain stack with buffered overlap. Defaults to plan-only."
    )
    parser.add_argument("--tgc", type=Path, help=f"Geometry control file (default: {TGC})")
    parser.add_argument("--scenario", help=f"Single s1 scenario value (default: {SCENARIO})")
    parser.add_argument("--bounds", nargs=4, type=float, metavar=("XMIN", "YMIN", "XMAX", "YMAX"))
    parser.add_argument("--crs", help=f"Projected CRS for bounds and outputs (default: {CRS})")
    parser.add_argument(
        "--buffer",
        type=float,
        help=f"Seam allowance in metres (default: {SEAM_BUFFER_METRES})",
    )
    parser.add_argument(
        "--planning-resolution",
        type=float,
        help=f"Coverage-allocation grid size in metres (default: {PLANNING_RESOLUTION_METRES})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=f"New raster directory (default: {OUTPUT_DIRECTORY})",
    )
    parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--pause", action=argparse.BooleanOptionalAction, default=None)
    return parser


def _pause(enabled: bool) -> None:
    if enabled and sys.stdin.isatty():
        input("Press Enter to close...")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    tgc_path = args.tgc if args.tgc is not None else TGC
    scenario = args.scenario if args.scenario is not None else SCENARIO
    raw_bounds = tuple(args.bounds) if args.bounds is not None else BOUNDS
    crs = args.crs if args.crs is not None else CRS
    seam_buffer = args.buffer if args.buffer is not None else SEAM_BUFFER_METRES
    planning_resolution = (
        args.planning_resolution if args.planning_resolution is not None else PLANNING_RESOLUTION_METRES
    )
    output_directory = args.output_dir if args.output_dir is not None else OUTPUT_DIRECTORY
    write = args.write if args.write is not None else WRITE
    pause = args.pause if args.pause is not None else PAUSE_ON_FINISH

    print(f"crop_tgc_terrains.py {WRAPPER_VERSION}")
    try:
        if raw_bounds is None:
            message = "BOUNDS is required; set it near the top of the script or pass --bounds XMIN YMIN XMAX YMAX"
            raise ValueError(message)
        payload = run(
            tgc_path=tgc_path,
            scenario=scenario,
            bounds=AnalysisBounds(*raw_bounds),
            crs=crs,
            seam_buffer=seam_buffer,
            planning_resolution=planning_resolution,
            output_directory=output_directory,
            write=write,
        )
    # The CLI boundary reports failures from PyTUFLOW, GDAL, and file-system dependencies.
    except Exception as error:  # noqa: BLE001
        print(f"ERROR: {error}", file=sys.stderr)
        _pause(pause)
        return 1

    mode = "WROTE" if write else "PLAN ONLY"
    print(f"{mode}: {len(payload['layers'])} active terrain layer(s) for -s1 {scenario}")
    for layer in payload["layers"]:
        print(
            f"  {layer['order']:03d} line {layer['line_number']}: retained "
            f"{layer['retained_planning_cells']}/{layer['active_planning_cells']} planning cells -> "
            f"{layer['output'] or 'omitted'}"
        )
    if payload["patched_tgc"]:
        print(f"Patched TGC: {payload['patched_tgc']}")
    if payload["coverage_qa"] is not None:
        print(f"Terrain coverage QA: {payload['coverage_qa']['gpkg']}")
    _pause(pause)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
