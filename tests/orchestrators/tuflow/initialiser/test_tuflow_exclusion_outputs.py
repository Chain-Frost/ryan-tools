"""TUFLOW GIS schema/geometry tests independent of a licensed solver."""

from pathlib import Path

import fiona
import numpy as np
import pytest
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ryan_library.orchestrators.tuflow.initialiser.geometry import make_domain
from ryan_library.orchestrators.tuflow.initialiser.gis_writer import (
    feature_geometry,
    write_analysis,
    write_tuflow_layers,
)


def _empty(path: Path, props: dict[str, str], *, crs_wkt: str) -> None:
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer=path.stem,
        crs_wkt=crs_wkt,
        schema={"geometry": "Polygon", "properties": props},
    ):
        pass


def test_exclusions_are_removed_from_both_tuflow_layers(tmp_path: Path) -> None:
    directions = np.zeros((12, 12), dtype=np.int16)
    directions[3:8, 6] = 6
    domain = make_domain(
        directions,
        aoi=box(6.2, 3.2, 6.8, 3.8),
        transform=from_origin(0, 12, 1, 1),
        raster_bounds=(0, 0, 12, 12),
        buffer_m=0.25,
        exclusions=box(6.1, 6.1, 6.9, 6.9),
    )
    model = tmp_path / "project"
    gis = model / "model" / "gis" / "EXG"
    gis.mkdir(parents=True)
    crs = CRS.from_epsg(28350).to_wkt()
    for prefix, props in (
        ("2d_code", {"Code": "int"}),
        ("2d_loc", {"Comment": "str:250"}),
        ("2d_rf", {"Name": "str:100", "f1": "float", "f2": "float"}),
    ):
        _empty(gis / f"{prefix}_EXG_01_R.gpkg", props, crs_wkt=crs)
    paths = write_tuflow_layers(model, scenario="EXG", domain=domain, rainfall_name="Rainfall")
    geometries: dict[str, BaseGeometry] = {}
    for name, path in paths.items():
        with fiona.open(path) as layer:
            assert len(layer) > 0
            assert layer.crs_wkt is not None
            geometries[name] = unary_union([feature_geometry(feature) for feature in layer])
            if name == "2d_code":
                assert all(feature.properties["Code"] == 1 for feature in layer)
            elif name == "2d_rf":
                assert all(feature.properties["Name"] == "Rainfall" for feature in layer)
                assert all(feature.properties["f1"] == pytest.approx(1) for feature in layer)
                assert all(feature.properties["f2"] == pytest.approx(1) for feature in layer)
    assert geometries["2d_code"].equals(geometries["2d_rf"])
    assert geometries["2d_code"].equals(domain.active)
    assert geometries["2d_loc"].covers(domain.active)
    assert domain.excluded_area is not None
    assert geometries["2d_code"].intersection(domain.excluded_area).area == 0

    analysis = model / "model" / "gis" / "hydrology.gpkg"
    write_analysis(analysis, domain=domain, crs_wkt=crs)
    assert {
        "aoi",
        "contributing_area",
        "aoi_buffer",
        "active_extent",
        "exclusions_supplied",
        "exclusions_applied",
        "detailed_analysis_extent",
        "exclusions_analysis_skipped",
    } <= set(fiona.listlayers(analysis))
    with fiona.open(analysis, layer="detailed_analysis_extent") as detail:
        clipped = unary_union([feature_geometry(row) for row in detail])
    assert domain.excluded_area is not None
    assert clipped.intersection(domain.excluded_area).area == 0

    alternate = model / "model" / "gis" / "hydrology_with_excluded_analysis.gpkg"
    write_analysis(alternate, domain=domain, crs_wkt=crs, analyse_exclusions=True)
    assert "exclusions_analysis_skipped" not in fiona.listlayers(alternate)
    with fiona.open(alternate, layer="detailed_analysis_extent") as detail:
        included = unary_union([feature_geometry(row) for row in detail])
    assert included.equals(domain.contributing)
