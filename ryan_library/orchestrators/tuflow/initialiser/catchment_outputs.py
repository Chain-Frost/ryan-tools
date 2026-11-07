"""GIS and JSON audit outputs for per-AOI exterior inflow catchments."""

import json
from pathlib import Path

import fiona
import rasterio
from shapely.geometry import LineString, MultiPolygon, Polygon, mapping
from shapely.geometry.base import BaseGeometry

from .catchments import InflowCatchment, OutletCatchment
from .coordinates import pixel_centre
from .gis_types import LayerSchema


def _multipolygon(geom: BaseGeometry) -> MultiPolygon:
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, MultiPolygon):
        return geom
    msg = "Catchment/AOI geometry must be polygonal"
    raise ValueError(msg)


def write_inflow_products(
    path: Path,
    *,
    aois: list[BaseGeometry],
    inflows: list[InflowCatchment],
    transform: rasterio.Affine,
    crs_wkt: str,
) -> None:
    """Write identity-preserving AOIs, crossing catchments and LFP polylines.

    These are per-entry incremental exterior catchments, NOT yet the complete
    stream-reach subcatchment product required by issue #3.
    """
    aois_schema: LayerSchema = {
        "geometry": "MultiPolygon",
        "properties": {"aoi_id": "int"},
    }
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="aoi_inputs",
        crs_wkt=crs_wkt,
        schema=aois_schema,
    ) as output:
        for aoi_id, geom in enumerate(aois, start=1):
            output.write(
                {
                    "geometry": mapping(_multipolygon(geom)),
                    "properties": {"aoi_id": aoi_id},
                }
            )

    areas_schema: LayerSchema = {
        "geometry": "MultiPolygon",
        "properties": {
            "aoi_id": "int",
            "catch_id": "int",
            "inlet_row": "int",
            "inlet_col": "int",
            "outlet_row": "int",
            "outlet_col": "int",
            "area_m2": "float",
            "aoi_total_m2": "float",
            "z_min_m": "float",
            "z_max_m": "float",
            "z_mean_m": "float",
            "status": "str:48",
        },
    }
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="aoi_inflow_catchments",
        crs_wkt=crs_wkt,
        schema=areas_schema,
    ) as output:
        for item in inflows:
            output.write(
                {
                    "geometry": mapping(_multipolygon(item.polygon)),
                    "properties": {
                        "aoi_id": item.aoi_id,
                        "catch_id": item.catchment_id,
                        "inlet_row": item.inlet[0],
                        "inlet_col": item.inlet[1],
                        "outlet_row": item.outlet[0],
                        "outlet_col": item.outlet[1],
                        "area_m2": item.local_area_m2,
                        "aoi_total_m2": item.aoi_cumulative_area_m2,
                        "z_min_m": item.min_elevation_m,
                        "z_max_m": item.max_elevation_m,
                        "z_mean_m": item.mean_elevation_m,
                        "status": item.status,
                    },
                }
            )

    lines_schema: LayerSchema = {
        "geometry": "LineString",
        "properties": {
            "aoi_id": "int",
            "catch_id": "int",
            "flow_path_id": "int",
            "length_m": "float",
            "slope_ea": "float",
            "source_z_m": "float",
            "outlet_z_m": "float",
            "signed_slope_qa": "str:32",
            "origin": "str:24",
        },
    }
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="longest_flow_paths",
        crs_wkt=crs_wkt,
        schema=lines_schema,
    ) as output:
        for item in inflows:
            if not item.path or item.length_m is None or item.slope_ea is None:
                continue
            geometry = LineString([pixel_centre(transform, row, col) for row, col in item.path])
            if abs(geometry.length - item.length_m) > 1e-6:
                msg = "GIS LFP length differs from computed D8 length"
                raise ValueError(msg)
            output.write(
                {
                    "geometry": mapping(geometry),
                    "properties": {
                        "aoi_id": item.aoi_id,
                        "catch_id": item.catchment_id,
                        "flow_path_id": item.catchment_id,
                        "length_m": item.length_m,
                        "slope_ea": item.slope_ea,
                        "source_z_m": item.source_elevation_m,
                        "outlet_z_m": item.outlet_elevation_m,
                        "signed_slope_qa": ("negative" if item.slope_ea < 0 else "nonnegative"),
                        "origin": "inferred_D8",
                    },
                }
            )


def write_inflow_summary(path: Path, inflows: list[InflowCatchment]) -> None:
    """Write a machine-readable record without conflating cumulative AOI totals."""
    path.write_text(
        json.dumps(
            {
                "definition": "Per-AOI first-entry D8 exterior partitions, not stream-reach basins",
                "cumulative_areas_may_overlap_between_aois": True,
                "catchments": [
                    {
                        "aoi_id": item.aoi_id,
                        "catchment_id": item.catchment_id,
                        "inlet_cell": list(item.inlet),
                        "outlet_cell": list(item.outlet),
                        "local_exterior_area_m2": item.local_area_m2,
                        "aoi_cumulative_area_m2": item.aoi_cumulative_area_m2,
                        "longest_path_length_m": item.length_m,
                        "longest_path_slope_ea": item.slope_ea,
                        "analysis_status": item.status,
                    }
                    for item in inflows
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def write_catchment_polygons(path: Path, inflows: list[InflowCatchment], *, crs_wkt: str) -> None:
    """Write only the requested per-entry upstream catchment polygons."""
    schema: LayerSchema = {
        "geometry": "MultiPolygon",
        "properties": {
            "interest_id": "int",
            "catch_id": "int",
            "area_m2": "float",
            "z_min_m": "float",
            "z_max_m": "float",
            "z_mean_m": "float",
        },
    }
    with fiona.open(path, "w", driver="GPKG", layer="catchments", crs_wkt=crs_wkt, schema=schema) as output:
        for item in inflows:
            output.write(
                {
                    "geometry": mapping(_multipolygon(item.polygon)),
                    "properties": {
                        "interest_id": item.aoi_id,
                        "catch_id": item.catchment_id,
                        "area_m2": item.local_area_m2,
                        "z_min_m": item.min_elevation_m,
                        "z_max_m": item.max_elevation_m,
                        "z_mean_m": item.mean_elevation_m,
                    },
                }
            )


def write_outlet_catchment_polygons(path: Path, catchments: list[OutletCatchment], *, crs_wkt: str) -> None:
    """Write one upstream catchment polygon for each point interest feature."""
    schema: LayerSchema = {
        "geometry": "MultiPolygon",
        "properties": {
            "interest_id": "int",
            "area_m2": "float",
            "outlet_row": "int",
            "outlet_col": "int",
            "snap_m": "float",
            "z_min_m": "float",
            "z_max_m": "float",
            "z_mean_m": "float",
            "status": "str:64",
        },
    }
    with fiona.open(path, "w", driver="GPKG", layer="catchments", crs_wkt=crs_wkt, schema=schema) as output:
        for item in catchments:
            output.write(
                {
                    "geometry": mapping(_multipolygon(item.polygon)),
                    "properties": {
                        "interest_id": item.interest_id,
                        "area_m2": item.area_m2,
                        "outlet_row": item.outlet[0],
                        "outlet_col": item.outlet[1],
                        "snap_m": item.snap_distance_m,
                        "z_min_m": item.min_elevation_m,
                        "z_max_m": item.max_elevation_m,
                        "z_mean_m": item.mean_elevation_m,
                        "status": item.status,
                    },
                }
            )
