"""Catchment-only workflow, independent of TUFLOW project generation."""

import shutil
import sqlite3
import tempfile
import warnings
from dataclasses import dataclass
from pathlib import Path

import fiona
import numpy as np
import rasterio
from rasterio.crs import CRS
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from .catchment_outputs import write_catchment_polygons, write_outlet_catchment_polygons
from .catchments import delineate_inflows, delineate_outlet_catchments
from .geometry import check_dem
from .grass_runner import process_terrain
from .terrain import TerrainLayer, compose_terrain_layers
from .tgc_terrain import read_tgc_terrain_stack


@dataclass(frozen=True, slots=True)
class CatchmentConfig:
    """Inputs for one catchment-only output."""

    terrains: tuple[Path, ...]
    interest: Path
    output: Path
    target_crs: CRS
    tgc: Path | None = None
    tgc_scenarios: tuple[str, ...] = ()
    tgc_global_only: bool = False
    analysis_bounds: tuple[float, float, float, float] | None = None
    analysis_resolution_m: float | None = None
    max_analysis_cells: int = 100_000_000
    grass_executable: str = "grass"
    fill_depressions: bool = False
    snap_distance_m: float = 0
    allow_partial: bool = False


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def read_interest_features(path: Path) -> tuple[CRS, list[BaseGeometry]]:
    """Read point/line/polygon features, including stale-count GeoPackages."""
    with fiona.open(path) as source:
        if not source.crs_wkt:
            msg = "Interest vector has no CRS"
            raise ValueError(msg)
        crs = CRS.from_wkt(source.crs_wkt)
        records = list(source)
        if not records and path.suffix.lower() == ".gpkg":
            # QGIS/GDAL can leave gpkg_ogr_contents.feature_count stale. Fiona
            # then reports zero and skips iteration although keyed features exist.
            with sqlite3.connect(path) as database:
                table = _quoted_identifier(source.name)
                columns = database.execute(f"PRAGMA table_info({table})").fetchall()
                primary = next((str(row[1]) for row in columns if int(row[5]) == 1), None)
                if primary is not None:
                    key = _quoted_identifier(primary)
                    ids = database.execute(f"SELECT {key} FROM {table} ORDER BY {key}").fetchall()  # noqa: S608
                    records = [source.get(int(row[0])) for row in ids]
        geometries: list[BaseGeometry] = []
        for record in records:
            if record is None or record.geometry is None:
                continue
            geometry = shape(record.geometry)
            if geometry.geom_type not in {
                "Point",
                "LineString",
                "MultiLineString",
                "Polygon",
                "MultiPolygon",
            }:
                msg = "Interest vector must contain point, line or polygon geometries"
                raise ValueError(msg)
            if not geometry.is_valid:
                msg = "Interest vector contains an invalid geometry"
                raise ValueError(msg)
            if not geometry.is_empty:
                geometries.append(geometry)
    if not geometries:
        msg = "Interest vector has no usable point, line or polygon features"
        raise ValueError(msg)
    return crs, geometries


def build_catchments(config: CatchmentConfig) -> Path:
    """Generate only a catchment GeoPackage; never create TUFLOW files."""
    if config.output.exists():
        msg = f"Existing catchment output is protected: {config.output}"
        raise FileExistsError(msg)
    if config.output.suffix.lower() != ".gpkg":
        msg = "Catchment output must be a .gpkg file"
        raise ValueError(msg)
    if bool(config.terrains) == bool(config.tgc):
        msg = "Supply either explicit terrain rasters or one TGC, but not both"
        raise ValueError(msg)
    if config.terrains and any(not path.is_file() for path in config.terrains):
        msg = "Every terrain raster must exist"
        raise FileNotFoundError(msg)
    if config.terrains and len(config.terrains) != len({path.absolute() for path in config.terrains}):
        msg = "Explicit terrain rasters must be unique, in priority order"
        raise ValueError(msg)
    if not config.interest.is_file():
        raise FileNotFoundError(config.interest)
    interest_crs, interests = read_interest_features(config.interest)
    if interest_crs != config.target_crs:
        msg = "Interest CRS must match the requested analysis CRS; reproject explicitly"
        raise ValueError(msg)
    config.output.parent.mkdir(parents=True, exist_ok=True)
    if config.tgc is not None:
        stack = read_tgc_terrain_stack(
            config.tgc,
            scenarios=config.tgc_scenarios,
            global_only=config.tgc_global_only,
        )
        layers = stack.layers
        if stack.skipped_commands:
            warnings.warn(
                "TGC terrain commands not applied to the raster mosaic: " + "; ".join(stack.skipped_commands),
                RuntimeWarning,
                stacklevel=2,
            )
    else:
        layers = tuple(TerrainLayer(path) for path in config.terrains)
    with tempfile.TemporaryDirectory(prefix="catchment-init-") as temporary:
        scratch = Path(temporary)
        mosaic = compose_terrain_layers(
            layers,
            target_crs=config.target_crs,
            directory=scratch / "mosaic",
            resolution_m=config.analysis_resolution_m,
            max_cells=config.max_analysis_cells,
            assume_missing_crs=config.target_crs,
            bounds=config.analysis_bounds,
        )
        with rasterio.open(mosaic.dem) as terrain:
            check_dem(terrain)
            grass = process_terrain(
                mosaic.dem,
                work_dir=scratch / "grass",
                grass_executable=config.grass_executable,
                fill_depressions=config.fill_depressions,
            )
            with (
                rasterio.open(grass.flow_direction) as direction_source,
                rasterio.open(grass.accumulation) as accumulation_source,
                rasterio.open(grass.conditioned_dem) as elevation_source,
            ):
                valid = (
                    (direction_source.read_masks(1) > 0)
                    & (elevation_source.read_masks(1) > 0)
                    & (terrain.read_masks(1) > 0)
                )
                directions = direction_source.read(1, masked=True).filled(0).astype(np.int16)
                elevations = elevation_source.read(1, masked=True).filled(np.nan).astype(np.float64)
                valid &= np.isfinite(elevations)
                staged = scratch / config.output.name
                geometry_types = {geometry.geom_type for geometry in interests}
                if geometry_types == {"Point"}:
                    upstream = accumulation_source.read(1, masked=True).filled(np.nan).astype(np.float64)
                    catchments = delineate_outlet_catchments(
                        directions,
                        elevations,
                        upstream,
                        interests,
                        transform=terrain.transform,
                        valid_cells=valid,
                        snap_distance_m=config.snap_distance_m,
                        allow_partial=config.allow_partial,
                        clip_bounds=config.analysis_bounds,
                    )
                    write_outlet_catchment_polygons(staged, catchments, crs_wkt=config.target_crs.to_wkt())
                elif "Point" in geometry_types:
                    msg = "Interest vector cannot mix point and line/polygon geometries"
                    raise ValueError(msg)
                else:
                    inflows = delineate_inflows(
                        directions,
                        elevations,
                        interests,
                        transform=terrain.transform,
                        valid_cells=valid,
                    )
                    write_catchment_polygons(staged, inflows, crs_wkt=config.target_crs.to_wkt())
                publishing = config.output.with_suffix(config.output.suffix + ".tmp")
                if publishing.exists():
                    msg = f"Temporary catchment output already exists: {publishing}"
                    raise FileExistsError(msg)
                shutil.copy2(staged, publishing)
                publishing.replace(config.output)
    return config.output
