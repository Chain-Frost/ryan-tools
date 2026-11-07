"""Write disjoint reach-local subcatchments into the existing hydrology GPKG."""

import json
from pathlib import Path

import fiona
import rasterio
from shapely.geometry import MultiPolygon, Polygon, mapping
from shapely.geometry.base import BaseGeometry

from .coordinates import pixel_centre
from .gis_types import LayerSchema
from .subcatchments import SubcatchmentResult


def _polygonal(geometry: BaseGeometry) -> MultiPolygon:
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    if isinstance(geometry, MultiPolygon):
        return geometry
    msg = "Local subcatchment must be a Polygon or MultiPolygon"
    raise ValueError(msg)


def write_reach_subcatchments(
    hydrology_gpkg: Path, result: SubcatchmentResult, *, crs_wkt: str, transform: rasterio.Affine
) -> None:
    """Append local per-reach coverage without replacing existing hydrology GIS."""
    if not hydrology_gpkg.is_file():
        raise FileNotFoundError(hydrology_gpkg)
    schema: LayerSchema = {
        "geometry": "MultiPolygon",
        "properties": {
            "subcatchment_id": "int",
            "type": "str:24",
            "reach_id": "int",
            "aoi_id": "int",
            "downstream_id": "int",
            "downstream_reach_id": "int",
            "outlet_x": "float",
            "outlet_y": "float",
            "local_area_m2": "float",
            "full_local_area_m2": "float",
            "upstream_area_m2": "float",
            "elevation_min_m": "float",
            "elevation_max_m": "float",
            "elevation_mean_m": "float",
            "terrain_slope_mean": "float",
            "stream_length_m": "float",
            "stream_slope_ea": "float",
            "status": "str:48",
        },
    }
    with fiona.open(
        hydrology_gpkg, "w", driver="GPKG", layer="subcatchments", schema=schema, crs_wkt=crs_wkt
    ) as output:
        for item in result.subcatchments:
            xy = pixel_centre(transform, *item.outlet) if item.outlet is not None else None
            output.write(
                {
                    "geometry": mapping(_polygonal(item.polygon)),
                    "properties": {
                        "subcatchment_id": item.subcatchment_id,
                        "type": item.kind,
                        "reach_id": item.reach_id,
                        "aoi_id": item.aoi_id,
                        "downstream_id": item.downstream_subcatchment_id,
                        "downstream_reach_id": item.downstream_reach_id,
                        "outlet_x": xy[0] if xy is not None else None,
                        "outlet_y": xy[1] if xy is not None else None,
                        "local_area_m2": item.local_area_m2,
                        "full_local_area_m2": item.full_local_area_m2,
                        "upstream_area_m2": item.cumulative_area_m2,
                        "elevation_min_m": item.min_elevation_m,
                        "elevation_max_m": item.max_elevation_m,
                        "elevation_mean_m": item.mean_elevation_m,
                        "terrain_slope_mean": item.mean_terrain_slope,
                        "stream_length_m": item.stream_length_m,
                        "stream_slope_ea": item.stream_slope_ea,
                        "status": item.profile_status,
                    },
                }
            )


def write_reach_subcatchment_summary(path: Path, result: SubcatchmentResult) -> None:
    """Retain reproducible conservation evidence and stable reach identifiers."""
    path.write_text(
        json.dumps(
            {
                "definition": "Disjoint local D8 catchments of threshold-defined reaches and direct-AOI residuals",
                "cumulative_upstream_area_m2": "At reach outlet, may overlap; never sum across reaches",
                "analysed_cell_count": result.analysed_cell_count,
                "excluded_cell_count": result.excluded_cell_count,
                "full_contributing_cell_count": result.full_contributing_cell_count,
                "channel_reach_count": result.channel_reach_count,
                "local_area_m2_sum": sum(item.local_area_m2 for item in result.subcatchments),
                "items": [
                    {
                        "subcatchment_id": item.subcatchment_id,
                        "type": item.kind,
                        "reach_id": item.reach_id,
                        "aoi_id": item.aoi_id,
                        "downstream_id": item.downstream_subcatchment_id,
                        "downstream_reach_id": item.downstream_reach_id,
                        "outlet_cell": list(item.outlet) if item.outlet is not None else None,
                        "local_area_m2": item.local_area_m2,
                        "full_local_area_m2": item.full_local_area_m2,
                        "upstream_area_m2": item.cumulative_area_m2,
                        "elevation_min_m": item.min_elevation_m,
                        "elevation_max_m": item.max_elevation_m,
                        "elevation_mean_m": item.mean_elevation_m,
                        "terrain_slope_mean": item.mean_terrain_slope,
                        "stream_length_m": item.stream_length_m,
                        "stream_slope_ea": item.stream_slope_ea,
                        "status": item.profile_status,
                    }
                    for item in result.subcatchments
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
