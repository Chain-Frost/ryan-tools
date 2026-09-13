"""Evaluate and rank explicit culvert design candidates."""

from collections.abc import Sequence

from culvert_solver import ConvergenceError, InvalidInputError

from ...classes.culvert.candidate import DesignCandidate
from ...classes.culvert.criteria import DesignCriteria
from ...classes.culvert.results import (
    CandidateAssessment,
    DesignFailure,
    DesignFailureCode,
    DesignResult,
    ScenarioResult,
)
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
        failures: list[DesignFailure] = []
        for scenario in scenarios:
            try:
                result = solve_crossing_scenario(candidate.crossing, scenario)
            except (InvalidInputError, ConvergenceError) as exc:
                message = f"{scenario.name}: solver failure: {exc}"
                failure_reasons.append(message)
                failures.append(
                    DesignFailure(
                        code=DesignFailureCode.SOLVER_FAILURE,
                        scenario_name=scenario.name,
                        message=message,
                    )
                )
                continue
            scenario_results.append(result)
            criterion_reasons = assess_scenario(result, candidate.crossing, criteria)
            failure_reasons.extend(criterion_reasons)
            failures.extend(
                DesignFailure(
                    code=DesignFailureCode.CRITERION,
                    scenario_name=scenario.name,
                    message=message,
                )
                for message in criterion_reasons
            )
        assessments.append(
            CandidateAssessment(
                candidate=candidate,
                scenario_results=tuple(scenario_results),
                passed=not failure_reasons,
                failure_reasons=tuple(dict.fromkeys(failure_reasons)),
                failures=tuple(dict.fromkeys(failures)),
            )
        )

    return DesignResult(assessments=tuple(sorted(assessments, key=_rank_key)))
