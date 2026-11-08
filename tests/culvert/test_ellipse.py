"""Bounded concrete ellipse integration with independent source/geometry checks."""

import json
from dataclasses import replace
from math import pi
from pathlib import Path

import pytest
from culvert_solver import (
    EntranceLossCoefficient,
    TailwaterCondition,
    solve_barrel_discharge_for_headwater,
    solve_barrel_hydraulics,
)

from ryan_library.classes.culvert import (
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    CulvertProject,
    DesignCriteria,
    HorizontalEllipseBarrelDefinition,
    Scenario,
)
from ryan_library.functions.culvert.adapter import build_solver_barrel
from ryan_library.functions.culvert.assessment import assess_scenario
from ryan_library.functions.culvert.config import export_project_json, load_project_json
from ryan_library.functions.culvert.export import crossing_definition_record, scenario_result_record
from ryan_library.orchestrators.culvert.analyse import analyse_project


def _definition() -> HorizontalEllipseBarrelDefinition:
    return HorizontalEllipseBarrelDefinition(
        span_mm=2000,
        rise_mm=1000,
        length=20,
        inlet_invert=1,
        outlet_invert=0.8,
        roughness=0.013,
        physical_inlet="square_edge_headwall",
    )


def test_ellipse_geometry_and_source_values() -> None:
    barrel = build_solver_barrel(_definition())
    assert barrel.geometry.area_full == pytest.approx(pi / 2)
    assert barrel.geometry.rise == 1
    inlet = barrel.inlet_coefficients
    assert inlet is not None
    # FHWA HDS-5 (2012), Table A.2, Chart 29 Scale 1; not circular coefficients.
    assert (inlet.chart, inlet.scale, inlet.k, inlet.m, inlet.c, inlet.y) == (29, 1, 0.01, 2, 0.0398, 0.67)
    assert inlet.reference.source_id == "FHWA-HDS5-2012-TABLE-A2"
    loss = barrel.entrance_loss_coefficient
    assert isinstance(loss, EntranceLossCoefficient)
    assert loss.ke == 0.5
    assert loss.reference.source_id == "FHWA-HDS5-2012-TABLE-C2"


def test_native_forward_inverse_and_project_round_trip(tmp_path: Path) -> None:
    definition = _definition()
    barrel = build_solver_barrel(definition)
    forward = solve_barrel_hydraulics(barrel, 1, 0.8)
    inverse = solve_barrel_discharge_for_headwater(barrel, forward.headwater_elevation, 0.8)
    # Numerical regression, not independent HY-8 acceptance; 1e-6 m/m3/s gates.
    assert forward.headwater_elevation == pytest.approx(1.556387336974289, abs=1e-6)
    assert inverse == pytest.approx(1, abs=1e-6)
    crossing = CrossingDefinition("Ellipse", (CulvertGroupDefinition("Concrete", definition),), source="synthetic")
    project = CulvertProject("Ellipse", (crossing,), (Scenario("Design", 1, TailwaterCondition(0.8)),))
    restored = load_project_json(export_project_json(project, tmp_path / "project.json"))
    assert restored == project
    record = crossing_definition_record(restored.crossings[0])
    assert record["groups"] == [
        {
            "name": "Concrete",
            "quantity": 1,
            "barrel": {
                "shape": "horizontal_ellipse",
                "span_mm": 2000,
                "rise_mm": 1000,
                "physical_inlet": "square_edge_headwall",
                "length_m": 20,
                "inlet_invert_m": 1,
                "outlet_invert_m": 0.8,
                "roughness": 0.013,
                "material": "concrete_pipe",
                "label": "",
            },
        }
    ]
    result = analyse_project(restored)[0]
    payload = scenario_result_record(result)
    text = json.dumps(payload)
    assert "FHWA-HDS5-2012-TABLE-A2" in text
    assert "FHWA-HDS5-2012-TABLE-C2" in text
    assert payload["warning_codes"] == list(result.warning_codes)
    assert result.hydraulic_result.total_discharge == pytest.approx(1)


@pytest.mark.parametrize(
    "material",
    [CulvertMaterialName.CORRUGATED_STEEL, CulvertMaterialName.SMOOTH_HDPE, CulvertMaterialName.CONCRETE_BOX],
)
def test_ellipse_rejects_unsupported_material(material: CulvertMaterialName) -> None:
    with pytest.raises(ValueError, match="concrete_pipe"):
        replace(_definition(), material=material)


@pytest.mark.parametrize(("span", "rise"), [(1000, 2000), (1000, 1000), (float("nan"), 1000), (2000, 0)])
def test_ellipse_rejects_invalid_axes(span: float, rise: float) -> None:
    with pytest.raises(ValueError):
        replace(_definition(), span_mm=span, rise_mm=rise)


@pytest.mark.parametrize("inlet", ["", "groove_end", "projecting"])
def test_ellipse_requires_explicit_supported_inlet(inlet: str) -> None:
    with pytest.raises(ValueError, match="physical_inlet"):
        replace(_definition(), physical_inlet=inlet)


def test_high_head_warning_is_preserved() -> None:
    crossing = CrossingDefinition("Ellipse", (CulvertGroupDefinition("Concrete", _definition()),))
    project = CulvertProject("Ellipse", (crossing,), (Scenario("High head", 10, 0.8),))
    result = analyse_project(project)[0]
    assert "inlet_control_high_head_extension" in result.warning_codes
    payload = scenario_result_record(result)
    assert "inlet_control_high_head_extension" in json.dumps(payload)
    depth = result.hydraulic_result.group_results[0].barrel_result.headwater_depth
    # Independent HDS-5 A.3 SI calculation: A=pi*span*rise/4, Ku=1.811,
    # Table A.2 c=.0398, Y=.67, slope correction=-.5 for slope .01.
    expected_ratio = 0.0398 * (1.811 * 10 / (pi / 2)) ** 2 + 0.67 - 0.5 * 0.01
    assert depth == pytest.approx(expected_ratio, abs=1e-6)
    # Rise=1 m; dividing by span=2 m would wrongly pass this criterion.
    failures = assess_scenario(result, crossing, DesignCriteria(maximum_headwater_ratio=4))
    assert any("HW/D" in reason for reason in failures)


def test_project_rejects_missing_inlet(tmp_path: Path) -> None:
    crossing = CrossingDefinition("Ellipse", (CulvertGroupDefinition("Concrete", _definition()),))
    project = CulvertProject("Ellipse", (crossing,), (Scenario("Design", 1, 0.8),))
    path = export_project_json(project, tmp_path / "project.json")
    payload = json.loads(path.read_text())
    del payload["crossings"][0]["groups"][0]["barrel"]["physical_inlet"]
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="physical_inlet"):
        load_project_json(path)
