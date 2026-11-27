"""Canonical-schema TUFLOW GIS and analysis GeoPackage writers."""

from pathlib import Path

import fiona
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from .geometry import Domain


def _parts(geometry: BaseGeometry) -> list[Polygon]:
    if isinstance(geometry, Polygon):
        return [geometry]
    if isinstance(geometry, MultiPolygon):
        return list(geometry.geoms)
    msg = f"Unsupported model polygon geometry: {geometry.geom_type}"
    raise ValueError(msg)


def _write_features(target: Path, geom: BaseGeometry, properties: dict[str, float | int | str]) -> None:
    """Append to scenario GIS created from canonical TUFLOW empties."""
    if not target.is_file():
        msg = f"Missing canonical working TUFLOW layer: {target}"
        raise FileNotFoundError(msg)
    with fiona.open(target, mode="a") as collection:
        if collection.schema["geometry"] != "Polygon":
            msg = f"Expected Polygon layer: {target}"
            raise ValueError(msg)
        schema = collection.schema["properties"]
        unknown = set(properties) - set(schema)
        if unknown:
            msg = f"TUFLOW empty schema lacks required attributes: {unknown}"
            raise ValueError(msg)
        for polygon in _parts(geom):
            if polygon.is_empty or not polygon.is_valid or polygon.area <= 0:
                msg = "Invalid output polygon"
                raise ValueError(msg)
            values = dict.fromkeys(schema)
            values.update(properties)
            collection.write({"geometry": mapping(polygon), "properties": values})
    with fiona.open(target) as collection:
        features = list(collection)
        if not features:
            msg = f"Writing to {target} produced an empty layer"
            raise ValueError(msg)
        actual = unary_union([feature_geometry(f) for f in features])
        if not actual.equals(geom):
            msg = f"Written geometry differs from requested geometry: {target}"
            raise ValueError(msg)


def feature_geometry(feature: fiona.model.Feature) -> BaseGeometry:
    """Decode a required feature geometry, rejecting null GIS records."""
    if feature.geometry is None:
        msg = "GIS feature has no geometry"
        raise ValueError(msg)
    return shape(feature.geometry)


def write_tuflow_layers(project_dir: Path, *, scenario: str, domain: Domain, rainfall_name: str) -> dict[str, Path]:
    """Populate canonical code/rainfall/location layers, with geometry checks."""
    if not rainfall_name or not rainfall_name.strip():
        msg = "Nonempty rainfall name required"
        raise ValueError(msg)
    directory = project_dir / "model" / "gis" / scenario
    paths = {name: directory / f"{name}_{scenario}_01_R.gpkg" for name in ("2d_code", "2d_rf", "2d_loc")}
    _write_features(paths["2d_code"], domain.active, {"Code": 1})
    with fiona.open(paths["2d_rf"]) as rainfall_layer:
        rf_fields = rainfall_layer.schema["properties"]
        attributes: dict[str, float | int | str] = {"Name": rainfall_name, "f1": 1.0}
        if "f2" in rf_fields:
            attributes["f2"] = 1.0
    _write_features(paths["2d_rf"], domain.active, attributes)
    _write_features(paths["2d_loc"], domain.location, {})
    return paths


def write_analysis(
    path: Path,
    *,
    domain: Domain,
    crs_wkt: str,
    analyse_exclusions: bool = False,
) -> None:
    """Write plain analysis geometries separate from canonical TUFLOW GIS."""
    outputs = {
        "aoi": domain.aoi,
        "contributing_area": domain.contributing,
        "aoi_buffer": domain.buffer,
        "active_extent": domain.active,
        "detailed_analysis_extent": (
            domain.contributing
            if analyse_exclusions or domain.excluded_area is None
            else domain.contributing.difference(domain.excluded_area)
        ),
    }
    if domain.exclusions is not None:
        outputs["exclusions_supplied"] = domain.exclusions
    if domain.excluded_area is not None:
        outputs["exclusions_applied"] = domain.excluded_area
        if not analyse_exclusions:
            outputs["exclusions_analysis_skipped"] = domain.excluded_area
    for name, geom in outputs.items():
        with fiona.open(
            path,
            mode="w",
            driver="GPKG",
            layer=name,
            schema={"geometry": "MultiPolygon", "properties": {"area_km2": "float"}},
            crs_wkt=crs_wkt,
        ) as dst:
            polygons = _parts(geom)
            dst.write(
                {
                    "geometry": mapping(MultiPolygon(polygons)),
                    "properties": {"area_km2": geom.area / 1_000_000.0},
                }
            )
