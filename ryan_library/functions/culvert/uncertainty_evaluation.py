"""Apply hydraulic samples to public crossing inputs without reproducing hydraulics."""

from dataclasses import replace

from culvert_solver import (
    ConvergenceError,
    CulvertBarrel,
    CulvertCrossing,
    EntranceLossCoefficient,
    EvaluationFailureKind,
    HydraulicEvaluationFailure,
    HydraulicSample,
    HydraulicUncertaintyParameter,
    InvalidInputError,
    RoughnessSelectionBasis,
    TailwaterInput,
    solve_crossing_hydraulics,
)

from ...classes.culvert.alternative import Alternative
from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.results import ScenarioResult
from ...classes.culvert.scenario import Scenario
from ...classes.culvert.uncertainty_results import StudyEvaluation
from .adapter import build_solver_crossing


def _sample_barrel(barrel: CulvertBarrel, sample: HydraulicSample) -> CulvertBarrel:
    for parameter in sample.parameters:
        if parameter.parameter is HydraulicUncertaintyParameter.MANNING_ROUGHNESS:
            barrel = replace(
                barrel,
                roughness=parameter.value,
                roughness_selection_basis=RoughnessSelectionBasis.USER_OVERRIDE,
                roughness_source=parameter.source,
                roughness_selection=None,
                parameter_set_id=None,
            )
        elif parameter.parameter is HydraulicUncertaintyParameter.ENTRANCE_LOSS_COEFFICIENT:
            barrel = replace(
                barrel,
                entrance_loss_coefficient=EntranceLossCoefficient(
                    name="Sampled entrance loss",
                    ke=parameter.value,
                    reference=parameter.source,
                ),
            )
    return barrel


def sampled_crossing_inputs(
    crossing: CrossingDefinition, scenario: Scenario, sample: HydraulicSample
) -> tuple[CulvertCrossing, float, TailwaterInput]:
    """Apply absolute SI values; barrel parameters affect every group in the crossing.

    Sampled discharge replaces total crossing flow; the upstream solver allocates flow
    to groups and roadway. Sampled tailwater replaces the boundary with an absolute
    elevation. Unvaried boundaries are resolved by the solver at the sampled discharge.
    """
    base = build_solver_crossing(crossing)
    varied = CulvertCrossing(
        groups=tuple(replace(group, barrel=_sample_barrel(group.barrel, sample)) for group in base.groups),
        roadway=base.roadway,
    )
    discharge = scenario.discharge
    tailwater = scenario.tailwater
    for parameter in sample.parameters:
        if parameter.parameter is HydraulicUncertaintyParameter.DISCHARGE:
            discharge = parameter.value
        elif parameter.parameter is HydraulicUncertaintyParameter.TAILWATER_ELEVATION:
            tailwater = parameter.value
    return varied, discharge, tailwater


def evaluate_crossing_sample(
    crossing: CrossingDefinition,
    scenario: Scenario,
    sample: HydraulicSample,
    *,
    alternative: Alternative | None = None,
) -> StudyEvaluation:
    """Retain input-domain and convergence failures; unexpected programming errors propagate."""
    try:
        varied, discharge, tailwater = sampled_crossing_inputs(crossing, scenario, sample)
        hydraulic = solve_crossing_hydraulics(varied, total_discharge=discharge, tailwater=tailwater)
    except ConvergenceError as exc:
        failure = HydraulicEvaluationFailure(
            kind=EvaluationFailureKind.CONVERGENCE,
            exception_type=type(exc).__name__,
            message=str(exc),
            bracket=exc.bracket,
            iterations=exc.iterations,
        )
    except InvalidInputError as exc:
        failure = HydraulicEvaluationFailure(
            kind=EvaluationFailureKind.INVALID_INPUT, exception_type=type(exc).__name__, message=str(exc)
        )
    else:
        return StudyEvaluation(
            sample=sample,
            crossing=crossing,
            scenario=scenario,
            alternative=alternative,
            result=ScenarioResult(
                crossing_name=crossing.name,
                scenario_name=scenario.name,
                alternative_name=None if alternative is None else alternative.name,
                hydraulic_result=hydraulic,
                aep_percent=scenario.aep_percent,
                source=scenario.source,
                notes=scenario.notes,
                target_headwater_elevation=scenario.target_headwater_elevation,
                tailwater_override_elevation=(
                    None
                    if any(
                        parameter.parameter is HydraulicUncertaintyParameter.TAILWATER_ELEVATION
                        for parameter in sample.parameters
                    )
                    else scenario.tailwater_override_elevation
                ),
            ),
        )
    return StudyEvaluation(
        sample=sample, crossing=crossing, scenario=scenario, alternative=alternative, failure=failure
    )
