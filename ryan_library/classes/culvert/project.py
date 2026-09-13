"""Typed project-level configuration for culvert workflows."""

from dataclasses import dataclass

from .alternative import Alternative
from .crossing import CrossingDefinition
from .scenario import Scenario


@dataclass(frozen=True, slots=True)
class CulvertProject:
    """A reproducible set of crossings, scenarios, and design alternatives."""

    name: str
    crossings: tuple[CrossingDefinition, ...]
    scenarios: tuple[Scenario, ...]
    alternatives: tuple[Alternative, ...] = ()

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            raise ValueError("name must be nonempty text.")
        crossings = tuple(self.crossings)
        scenarios = tuple(self.scenarios)
        alternatives = tuple(self.alternatives)
        if not crossings:
            raise ValueError("crossings must contain at least one crossing.")
        if not scenarios:
            raise ValueError("scenarios must contain at least one scenario.")
        crossing_names = [crossing.name for crossing in crossings]
        scenario_names = [scenario.name for scenario in scenarios]
        alternative_names = [alternative.name for alternative in alternatives]
        if len(crossing_names) != len(set(crossing_names)):
            raise ValueError("Crossing names must be unique within a project.")
        if len(scenario_names) != len(set(scenario_names)):
            raise ValueError("Scenario names must be unique within a project.")
        if len(alternative_names) != len(set(alternative_names)):
            raise ValueError("Alternative names must be unique within a project.")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "crossings", crossings)
        object.__setattr__(self, "scenarios", scenarios)
        object.__setattr__(self, "alternatives", alternatives)
