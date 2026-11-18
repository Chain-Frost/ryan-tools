"""Loss-equivalence grouping from observed cell assignments, never defined-ID products."""

from pathlib import Path

import fiona
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box, mapping

from ryan_library.functions.tuflow.loss_combinations import (
    ObservedPair,
    UnresolvedLossCombinationError,
    effective_loss_signature,
    group_observed_pairs,
    observed_pairs_from_check_grids,
    observed_pairs_from_grd_check,
)
from ryan_library.orchestrators.tuflow.rainfall_input_audit import MaterialDefinition as Mat
from ryan_library.orchestrators.tuflow.rainfall_input_audit import SoilDefinition as Soil


def mat(material_id: int, il: float = 0.0, cl: float = 0.0, imp: float = 0.0) -> Mat:
    return Mat(material_id, il, cl, imp, "defined")


def soil(soil_id: int, il: float = 0.0, cl: float = 0.0) -> Soil:
    return Soil(soil_id, "ILCL", (("IL", str(il)), ("CL", str(cl))), "defined")


def test_only_observed_pairs_and_neutral_collapse() -> None:
    materials = {1: mat(1), 2: mat(2), 3: mat(3, 20.0, 3.0), 4: mat(4, 20.0, 3.0)}
    soils = {1: soil(1), 2: soil(2, 10.0, 1.0), 3: Soil(3, "NONE", (), "defined")}
    groups = group_observed_pairs(
        [
            ObservedPair(1, 2, 8),
            ObservedPair(2, 2, 9),
            ObservedPair(3, 1, 7),
            ObservedPair(4, 3, 11),
            ObservedPair(1, 1, 2),
        ],
        materials,
        soils,
    )
    assert sum(group.cell_count for group in groups) == 37
    assert len(groups) == 3
    assert {group.category for group in groups} == {"no-loss", "soil-only", "material-only"}
    assert next(group for group in groups if group.category == "soil-only").material_ids == (1, 2)
    assert next(group for group in groups if group.category == "material-only").material_ids == (3, 4)


def test_impervious_matters_only_when_soil_active() -> None:
    wet = soil(2, 12.0, 2.0)
    dry = soil(1)
    assert effective_loss_signature(mat(1, imp=0.5), wet)[0] == "coupled"
    assert effective_loss_signature(mat(1, imp=0.5), dry)[0] == "no-loss"
    assert effective_loss_signature(mat(1, imp=0.0), wet)[0] == "soil-only"
    assert effective_loss_signature(mat(1, imp=1.0), wet)[1] != effective_loss_signature(mat(1, imp=0.5), wet)[1]


def test_unsupported_soil_fails_closed() -> None:
    with pytest.raises(UnresolvedLossCombinationError):
        effective_loss_signature(mat(1), Soil(1, "MYSTERY", (), "defined"))
    with pytest.raises(UnresolvedLossCombinationError):
        group_observed_pairs([ObservedPair(1, 99, 1)], {1: mat(1)}, {})
    green_ampt = Soil(1, "GA", (("Suction", "40"), ("Hydraulic Conductivity", "3"), ("Porosity", "0.4")), "defined")
    assert effective_loss_signature(mat(1), green_ampt)[0] == "soil-only"


@pytest.mark.parametrize(
    ("method", "fields"),
    [
        ("GA", (("Suction", "40"),)),
        ("HO", (("IL", "1"), ("IL Rate", "3"))),
        ("CO", (("Hydraulic Conductivity", "3"),)),
        ("SCS", (("Curve Number", "80"),)),
        ("ILCL", (("IL", "nan"), ("CL", "1"))),
        ("GA", (("USDA Soil Type", "CLAY"), ("Porosity", "0.4"))),
    ],
)
def test_missing_or_conflicting_method_parameters_fail_closed(method: str, fields: tuple[tuple[str, str], ...]) -> None:
    with pytest.raises(UnresolvedLossCombinationError):
        effective_loss_signature(mat(1), Soil(1, method, fields, "defined"))


def test_raster_pairs_only_observed(tmp_path: Path) -> None:
    def write(name: str, values: list[list[int]]) -> Path:
        path = tmp_path / name
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=2,
            width=3,
            count=1,
            dtype="int16",
            crs="EPSG:28350",
            transform=from_origin(0, 2, 1, 1),
            nodata=-9999,
        ) as dataset:
            dataset.write(np.asarray(values, dtype="int16"), 1)
        return path

    materials = write("mat.tif", [[1, 1, 2], [2, 2, -9999]])
    soils = write("soil.tif", [[3, 3, 4], [4, 3, 3]])
    active = write("active.tif", [[1, 0, 1], [1, 1, 0]])
    assert observed_pairs_from_check_grids(materials, soils, active_mask_grid=active) == (
        ObservedPair(1, 3, 1),
        ObservedPair(2, 3, 1),
        ObservedPair(2, 4, 2),
    )


def test_explicitly_active_cell_missing_soil_fails_closed(tmp_path: Path) -> None:
    def raster(name: str, values: list[list[int]]) -> Path:
        path = tmp_path / name
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=1,
            width=2,
            count=1,
            dtype="int16",
            crs="EPSG:28350",
            transform=from_origin(0, 1, 1, 1),
            nodata=-9999,
        ) as dst:
            dst.write(np.asarray(values, dtype="int16"), 1)
        return path

    mat_path = raster("mat.tif", [[1, 1]])
    soil_path = raster("soil.tif", [[2, -9999]])
    active_path = raster("active.tif", [[1, 1]])
    with pytest.raises(UnresolvedLossCombinationError, match="missing material/soil"):
        observed_pairs_from_check_grids(mat_path, soil_path, active_mask_grid=active_path)


def test_native_grd_check_counts_active_colocated_pairs(tmp_path: Path) -> None:
    source = tmp_path / "check.gpkg"
    schema = {"geometry": "Polygon", "properties": {"Code": "int", "Material": "int", "Soil_ID": "int"}}
    with fiona.open(source, "w", driver="GPKG", layer="model_grd_check", crs="EPSG:28350", schema=schema) as dst:
        for i, (code, material, soil_id) in enumerate(((1, 10, 20), (1, 10, 20), (-1, 11, 21), (1, 11, 20))):
            dst.write(
                {
                    "geometry": mapping(box(i, 0, i + 1, 1)),
                    "properties": {"Code": code, "Material": material, "Soil_ID": soil_id},
                }
            )
    assert observed_pairs_from_grd_check(source) == (ObservedPair(10, 20, 2), ObservedPair(11, 20, 1))


def test_native_grd_check_without_soil_field_is_neutral(tmp_path: Path) -> None:
    source = tmp_path / "model_grd_check.gpkg"
    schema = {"geometry": "Polygon", "properties": {"Code": "int", "Material": "int"}}
    with fiona.open(source, "w", driver="GPKG", layer="model_grd_check", crs="EPSG:28350", schema=schema) as dst:
        dst.write({"geometry": mapping(box(0, 0, 1, 1)), "properties": {"Code": 1, "Material": 8}})
    assert observed_pairs_from_grd_check(source) == (ObservedPair(8, -1, 1),)
