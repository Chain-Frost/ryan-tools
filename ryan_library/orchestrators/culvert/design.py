"""Evaluate and rank explicit culvert design candidates."""

from collections.abc import Sequence

from culvert_solver import ConvergenceError, InvalidInputError

from ...classes.culvert.candidate import DesignCandidate
from ...classes.culvert.criteria import DesignCriteria
from ...classes.culvert.results import CandidateAssessment, DesignResult, ScenarioResult
from ...classes.culvert.scenario import Scenario
from ...functions.culvert.adapter import build_solver_crossing
from ...functions.culvert.assessment import assess_scenario
from .solve import solve_crossing_scenario


def _rank_key(assessment: CandidateAssessment) -> tuple[int, float, int, str]:
    solver_crossing = build_solver_crossing(assessment.candidate.crossing)
    return (
        0 if assessment.passed else 1,
        solver_crossing.total_full_area,
        solver_crossing.total_barrels,
        assessment.candidate.name,
    )


def design_crossing(
    candidates: Sequence[DesignCandidate],
    scenarios: Sequence[Scenario],
    criteria: DesignCriteria,
) -> DesignResult:
    """Test explicit candidates against all scenarios and rank feasible options by size."""
    if not candidates:
        msg = "candidates must contain at least one design candidate."
        raise ValueError(msg)
    if not scenarios:
        msg = "scenarios must contain at least one scenario."
        raise ValueError(msg)

    assessments: list[CandidateAssessment] = []
    for candidate in candidates:
        scenario_results: list[ScenarioResult] = []
        failure_reasons: list[str] = []
        for scenario in scenarios:
            try:
                result = solve_crossing_scenario(candidate.crossing, scenario)
            except (InvalidInputError, ConvergenceError) as exc:
                failure_reasons.append(f"{scenario.name}: solver failure: {exc}")
                continue
            scenario_results.append(result)
            failure_reasons.extend(assess_scenario(result, candidate.crossing, criteria))
        assessments.append(
            CandidateAssessment(
                candidate=candidate,
                scenario_results=tuple(scenario_results),
                passed=not failure_reasons,
                failure_reasons=tuple(dict.fromkeys(failure_reasons)),
            )
        )

    return DesignResult(assessments=tuple(sorted(assessments, key=_rank_key)))
