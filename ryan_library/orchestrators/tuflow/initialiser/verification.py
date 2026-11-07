"""Reopen real-system outputs and retain machine-readable QA evidence."""

import math
from pathlib import Path
from typing import TypedDict

import fiona
import numpy as np
import rasterio
from rasterio.features import rasterize
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ryan_library.orchestrators.tuflow.initialiser.gis_types import LayerSchema
from ryan_library.orchestrators.tuflow.initialiser.gis_writer import feature_geometry
from ryan_library.orchestrators.tuflow.initialiser.project_setup import read_empty_schema


class LayerQA(TypedDict):
    """Reopened layer size and canonical schema."""

    features: int
    area_km2: float
    schema: LayerSchema


class GridQA(TypedDict):
    """Analysis raster geometry and active elevation range."""

    shape: list[int]
    resolution_m: list[float]
    active_elevation_range_m: list[float]


class ReachQA(TypedDict):
    """Signed slope evidence linked to a persisted stream path."""

    path_id: int
    path_type: str
    length_m: float
    slope_ea: float


class StreamsQA(TypedDict):
    """Stream counts and reach-level negative-slope evidence."""

    features: int
    channels: int
    synthetic_headwaters: int
    negative_slopes: int
    negative_slope_reaches: list[ReachQA]


class SubcatchmentsQA(TypedDict):
    """Hydrology GeoPackage local-area and reach-link integrity."""

    features: int
    channel_subcatchments: int
    direct_aoi_subcatchments: int
    total_local_area_m2: float
    total_polygon_area_m2: float
    negative_channel_slopes: int


class InflowQA(TypedDict):
    """Reopened AOI entry and inferred longest-path attribution."""

    aois: int
    entry_catchments: int
    longest_flow_paths: int
    excluded_from_path_analysis: int
    signed_negative_paths: int


class ProjectQA(TypedDict):
    """Reopened GIS and terrain evidence for a fixed EXG/RF01 project."""

    layers: dict[str, LayerQA]
    contributing_area_km2: float
    analysis_grid: GridQA
    streams: StreamsQA
    subcatchments: SubcatchmentsQA
    inflows: InflowQA


def _require(message: str, *, condition: bool) -> None:
    if not condition:
        raise ValueError(message)


def _number(feature: fiona.model.Feature, field: str) -> float:
    value = feature.properties[field]
    if not isinstance(value, (float, int)) or not math.isfinite(value):
        msg = f"Stream field {field!r} must be a finite number"
        raise ValueError(msg)
    return float(value)


def verify_streams(project: Path) -> StreamsQA:
    """Validate stream records and preserve signed slopes as reach-level QA."""
    with fiona.open(project / "model/gis/streams.gpkg") as src:
        rows = list(src)
        _require("Stream layer is empty", condition=bool(rows))
        negative: list[ReachQA] = []
        for feature in rows:
            _require("Invalid stream geometry", condition=feature_geometry(feature).is_valid)
            length = _number(feature, "length_m")
            slope = _number(feature, "slope_ea")
            _require("Stream length must be positive", condition=length > 0)
            if slope < 0:
                path_id = _number(feature, "path_id")
                path_type = feature.properties["path_type"]
                if not isinstance(path_type, str) or not path_id.is_integer():
                    msg = "Stream path ID/type is invalid"
                    raise ValueError(msg)
                negative.append(ReachQA(path_id=int(path_id), path_type=path_type, length_m=length, slope_ea=slope))
        return StreamsQA(
            features=len(rows),
            channels=sum(f.properties["path_type"] == "channel" for f in rows),
            synthetic_headwaters=sum(f.properties["path_type"] == "synthetic_headwater" for f in rows),
            negative_slopes=len(negative),
            negative_slope_reaches=negative,
        )


def verify_subcatchments(project: Path, contributing: BaseGeometry) -> SubcatchmentsQA:
    """Reopen subcatchment GIS and verify conservation and downstream IDs."""
    path = project / "model/gis/hydrology.gpkg"
    with fiona.open(path, layer="subcatchments") as source:
        rows = list(source)
        _require("Subcatchments layer is empty", condition=bool(rows))
        _require("Invalid subcatchment GIS CRS", condition=source.crs.to_epsg() == 28350)

    by_id: dict[int, fiona.model.Feature] = {}
    geometries: list[BaseGeometry] = []
    local_area = 0.0
    channels = 0
    direct = 0
    negative = 0
    for feature in rows:
        geom = feature_geometry(feature)
        _require("Invalid subcatchment geometry", condition=geom.is_valid and geom.area > 0)
        identifier = _number(feature, "subcatchment_id")
        _require("Invalid subcatchment ID", condition=identifier.is_integer() and identifier > 0)
        number = int(identifier)
        _require("Duplicate subcatchment ID", condition=number not in by_id)
        by_id[number] = feature
        area = _number(feature, "local_area_m2")
        _require("Subcatchment GIS area mismatch", condition=math.isclose(area, geom.area, abs_tol=0.01))
        geometries.append(geom)
        local_area += area
        kind = feature.properties["type"]
        channels += kind == "channel_reach"
        direct += kind == "aoi_direct"
        slope = feature.properties["stream_slope_ea"]
        if isinstance(slope, (int, float)) and slope < 0:
            negative += 1

    _require("Unknown subcatchment classification", condition=channels + direct == len(rows))
    full = unary_union(geometries)
    _require(
        "Local subcatchments overlap",
        condition=math.isclose(full.area, local_area, rel_tol=1e-9, abs_tol=0.01),
    )
    for feature in rows:
        downstream = feature.properties["downstream_id"]
        if downstream is not None:
            _require(
                "Subcatchment points to missing downstream ID",
                condition=isinstance(downstream, int) and downstream in by_id,
            )

    layers = fiona.listlayers(path)
    if "exclusions_applied" not in layers:
        _require("Local subcatchments do not cover full contributing area", condition=full.equals(contributing))
    else:
        _require(
            "Excluded subcatchment output exceeds full contributing area",
            condition=contributing.covers(full),
        )
    return SubcatchmentsQA(
        features=len(rows),
        channel_subcatchments=channels,
        direct_aoi_subcatchments=direct,
        total_local_area_m2=local_area,
        total_polygon_area_m2=full.area,
        negative_channel_slopes=negative,
    )


def verify_inflows(project: Path) -> InflowQA:
    """Reopen AOI-entry polygons and full inferred D8 flow paths for QA."""
    path = project / "model/gis/inflow_catchments.gpkg"
    required = {"aoi_inputs", "aoi_inflow_catchments", "longest_flow_paths"}
    _require("Missing AOI inflow GIS layers", condition=required <= set(fiona.listlayers(path)))
    with fiona.open(path, layer="aoi_inputs") as src:
        aois = list(src)
    _require("No AOI input geometries", condition=bool(aois))
    aoi_ids = {int(_number(row, "aoi_id")) for row in aois}
    _require("AOI identifiers missing or duplicated", condition=len(aoi_ids) == len(aois))

    with fiona.open(path, layer="aoi_inflow_catchments") as src:
        entries = list(src)
    catch_ids: set[int] = set()
    completed: set[int] = set()
    excluded = 0
    for entry in entries:
        _require("Invalid AOI entry polygon", condition=feature_geometry(entry).is_valid)
        catch_id = int(_number(entry, "catch_id"))
        _require("Duplicate AOI entry catchment ID", condition=catch_id not in catch_ids)
        catch_ids.add(catch_id)
        _require(
            "AOI entry belongs to missing AOI identifier",
            condition=int(_number(entry, "aoi_id")) in aoi_ids,
        )
        if entry.properties["status"] == "complete":
            completed.add(catch_id)
        elif entry.properties["status"] == "excluded_from_detailed_analysis":
            excluded += 1
        else:
            msg = "Unrecognised AOI entry analysis status"
            raise ValueError(msg)
    with fiona.open(path, layer="longest_flow_paths") as src:
        paths = list(src)
    ids: set[int] = set()
    negative = 0
    for feature in paths:
        _require("Invalid longest flow path geometry", condition=feature_geometry(feature).is_valid)
        path_id = int(_number(feature, "catch_id"))
        _require("Duplicate longest-path catchment ID", condition=path_id not in ids)
        ids.add(path_id)
        length = _number(feature, "length_m")
        _require(
            "Inferred LFP length differs from recorded geometry",
            condition=length > 0 and math.isclose(feature_geometry(feature).length, length, rel_tol=1e-8, abs_tol=0.01),
        )
        negative += _number(feature, "slope_ea") < 0
    _require("Missing or extra AOI longest paths", condition=ids == completed)
    return InflowQA(
        aois=len(aois),
        entry_catchments=len(entries),
        longest_flow_paths=len(paths),
        excluded_from_path_analysis=excluded,
        signed_negative_paths=negative,
    )


def verify_project(project: Path) -> ProjectQA:
    """Verify a fixed EXG/RF01 project with checks that survive python -O."""
    layers: dict[str, LayerQA] = {}
    geometries: dict[str, BaseGeometry] = {}
    for name in ("2d_code", "2d_rf", "2d_loc"):
        path = project / "model/gis/EXG" / f"{name}_EXG_01_R.gpkg"
        with fiona.open(path) as src:
            features = list(src)
            _require(f"Wrong CRS: {path}", condition=src.crs.to_epsg() == 28350)
            _require(f"Empty layer: {path}", condition=bool(features))
            _require(f"Invalid geometry: {path}", condition=all(feature_geometry(f).is_valid for f in features))
            geometries[name] = unary_union([feature_geometry(f) for f in features])
            canonical = read_empty_schema(
                source_path=project / "empty" / f"{name}_empty.gpkg", source_layer=f"{name}_empty_R"
            )
            _require(f"Noncanonical schema: {path}", condition=src.schema == canonical)
            layers[name] = LayerQA(features=len(features), area_km2=geometries[name].area / 1e6, schema=src.schema)
            if name == "2d_code":
                _require("Code must be 1", condition=all(f.properties["Code"] == 1 for f in features))
            if name == "2d_rf":
                _require(
                    "Invalid rainfall attributes",
                    condition=all(f.properties["Name"] == "RF01" and f.properties["f1"] == 1 for f in features),
                )
    _require("Code and rainfall footprints differ", condition=geometries["2d_code"].equals(geometries["2d_rf"]))
    _require("Location does not cover active domain", condition=geometries["2d_loc"].covers(geometries["2d_code"]))
    with fiona.open(project / "model/gis/hydrology.gpkg", layer="aoi") as src:
        aoi = unary_union([feature_geometry(f) for f in src])
    _require("Active domain does not cover AOI", condition=geometries["2d_code"].covers(aoi))
    with fiona.open(project / "model/gis/hydrology.gpkg", layer="contributing_area") as src:
        contributing = unary_union([feature_geometry(f) for f in src])
    with rasterio.open(project / "terrain/hydrology/analysis_dem.tif") as src:
        pixels = rasterize(
            [(geometries["2d_code"], 1)], out_shape=src.shape, transform=src.transform, all_touched=True
        ).astype(bool)
        dem = src.read(1)
        _require("NoData inside active domain", condition=not bool(np.any(src.read_masks(1)[pixels] == 0)))
        _require("Nonfinite active terrain", condition=bool(np.all(np.isfinite(dem[pixels]))))
        grid = GridQA(
            shape=list(src.shape),
            resolution_m=list(src.res),
            active_elevation_range_m=[float(dem[pixels].min()), float(dem[pixels].max())],
        )
    return ProjectQA(
        layers=layers,
        contributing_area_km2=contributing.area / 1e6,
        analysis_grid=grid,
        streams=verify_streams(project),
        subcatchments=verify_subcatchments(project, contributing),
        inflows=verify_inflows(project),
    )
