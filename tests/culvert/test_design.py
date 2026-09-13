"""Focused tests for explicit culvert candidate generation and assessment."""

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    DesignCriteria,
    Scenario,
)
from ryan_library.functions.culvert.candidate_generation import generate_circular_candidates
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
    assert "headwater elevation" in assessment.failure_reasons[0]
