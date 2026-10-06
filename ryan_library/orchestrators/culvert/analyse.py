"""Batch analysis across project crossings, scenarios, and alternatives."""

from ...classes.culvert.project import CulvertProject
from ...classes.culvert.results import ScenarioResult
from .solve import solve_crossing_scenario


def analyse_project(project: CulvertProject) -> tuple[ScenarioResult, ...]:
    """Evaluate all base crossings and alternatives for every project scenario."""
    results: list[ScenarioResult] = []
    for crossing in project.crossings:
        results.extend(solve_crossing_scenario(crossing, scenario) for scenario in project.scenarios)
    for alternative in project.alternatives:
        results.extend(
            solve_crossing_scenario(
                alternative.crossing,
                scenario,
                alternative_name=alternative.name,
            )
            for scenario in project.scenarios
        )
    return tuple(results)
