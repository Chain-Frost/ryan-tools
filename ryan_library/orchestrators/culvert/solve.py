"""Solve one crossing/scenario pair through ``culvert_solver``."""

from culvert_solver import solve_crossing_hydraulics

from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.results import ScenarioResult
from ...classes.culvert.scenario import Scenario
from ...functions.culvert.adapter import build_solver_crossing


def solve_crossing_scenario(
    crossing: CrossingDefinition,
    scenario: Scenario,
    *,
    alternative_name: str | None = None,
) -> ScenarioResult:
    """Evaluate one crossing and retain the complete authoritative hydraulic result."""
    solver_crossing = build_solver_crossing(crossing)
    hydraulic_result = solve_crossing_hydraulics(
        crossing=solver_crossing,
        total_discharge=scenario.discharge,
        tailwater=scenario.tailwater,
    )
    return ScenarioResult(
        crossing_name=crossing.name,
        scenario_name=scenario.name,
        alternative_name=alternative_name,
        hydraulic_result=hydraulic_result,
    )
