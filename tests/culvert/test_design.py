"""Focused tests for explicit culvert candidate generation and assessment."""

import json
from pathlib import Path

import pytest
from culvert_solver import HydraulicResultStatus, InvalidInputError

import ryan_library.orchestrators.culvert.design as design_module
from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    DesignCriteria,
    Scenario,
)
from ryan_library.functions.culvert.candidate_generation import generate_circular_candidates
from ryan_library.functions.culvert.export import export_design_result_json
from ryan_library.orchestrators.culvert.design import design_crossing


def _template() -> CrossingDefinition:
    return CrossingDefinition(
        name="Design crossing",
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                barrel=CircularBarrelDefinition(
                    diameter_mm=1200.0,
                    length=40.0,
                    inlet_invert=10.0,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )


def test_design_search_ranks_feasible_candidates() -> None:
    candidates = generate_circular_candidates(
        _template(),
        diameters_mm=(1200.0, 1500.0),
        quantities=(1, 2),
    )
    result = design_crossing(
        candidates,
        (Scenario(name="Design", discharge=2.0, tailwater=9.5),),
        DesignCriteria(maximum_headwater_elevation=100.0),
    )

    assert result.recommended is not None
    assert result.recommended.passed
    assert result.recommended.candidate.name == "1x_1200_mm"
    assert len(result.assessments) == 4


def test_rejected_candidate_retains_governing_reason() -> None:
    candidates = generate_circular_candidates(
        _template(),
        diameters_mm=(1200.0,),
        quantities=(1,),
    )
    result = design_crossing(
        candidates,
        (Scenario(name="Design", discharge=2.0, tailwater=9.5),),
        DesignCriteria(maximum_headwater_elevation=9.0),
    )

    assessment = result.assessments[0]
    assert not assessment.passed
    assert assessment.failure_reasons
    assert assessment.failures
    assert assessment.failures[0].code.value == "criterion"
    assert "headwater elevation" in assessment.failure_reasons[0]
    assert result.recommended is None


def test_design_export_retains_criteria_and_candidate_definition(tmp_path: Path) -> None:
    candidates = generate_circular_candidates(_template(), diameters_mm=(1200.0,), quantities=(2,))
    criteria = DesignCriteria(maximum_headwater_elevation=100.0)
    result = design_crossing(candidates, (Scenario(name="Design", discharge=2.0, tailwater=9.5),), criteria)

    path = export_design_result_json(result, tmp_path / "design.json", criteria=criteria)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["design_criteria"]["maximum_headwater_elevation_m"] == 100.0
    group = payload["assessments"][0]["crossing_definition"]["groups"][0]
    assert group["quantity"] == 2
    assert group["barrel"]["diameter_mm"] == 1200.0


def test_design_applies_ratio_count_and_width_constraints() -> None:
    candidates = generate_circular_candidates(
        _template(),
        diameters_mm=(1200.0,),
        quantities=(2,),
    )
    criteria = DesignCriteria(
        maximum_headwater_ratio=0.1,
        maximum_barrel_count=1,
        maximum_total_structure_width=2.0,
    )

    assessment = design_crossing(
        candidates,
        (Scenario(name="Design", discharge=2.0, tailwater=9.5),),
        criteria,
    ).assessments[0]

    assert not assessment.passed
    assert any("HW/D" in reason for reason in assessment.failure_reasons)
    assert any("barrel count" in reason for reason in assessment.failure_reasons)
    assert any("structure width" in reason for reason in assessment.failure_reasons)


def test_design_requires_every_scenario_to_pass() -> None:
    candidates = generate_circular_candidates(
        _template(),
        diameters_mm=(1200.0,),
        quantities=(1,),
    )

    assessment = design_crossing(
        candidates,
        (
            Scenario(name="Minor", discharge=0.5, tailwater=9.5),
            Scenario(name="Major", discharge=5.0, tailwater=9.5),
        ),
        DesignCriteria(maximum_headwater_elevation=11.0),
    ).assessments[0]

    assert not assessment.passed
    assert len(assessment.scenario_results) == 2
    assert any(failure.scenario_name == "Major" for failure in assessment.failures)


def test_solver_failure_is_exported_as_unresolved(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    candidates = generate_circular_candidates(
        _template(),
        diameters_mm=(1200.0,),
        quantities=(1,),
    )

    def fail_solver(*_args: object, **_kwargs: object) -> None:
        raise InvalidInputError("synthetic solver failure")

    monkeypatch.setattr(design_module, "solve_crossing_scenario", fail_solver)
    result = design_module.design_crossing(
        candidates,
        (Scenario(name="Design", discharge=2.0, tailwater=9.5),),
        DesignCriteria(maximum_headwater_elevation=100.0),
    )

    assessment = result.assessments[0]
    assert not assessment.passed
    assert not assessment.scenario_results
    assert assessment.worst_status is HydraulicResultStatus.UNRESOLVED

    path = export_design_result_json(result, tmp_path / "design.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["assessments"][0]["worst_status"] == "unresolved"
