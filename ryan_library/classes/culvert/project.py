"""Typed project-level configuration for culvert workflows."""

from dataclasses import dataclass

from .alternative import Alternative
from .criteria import DesignCriteria
from .crossing import CrossingDefinition
from .scenario import Scenario


@dataclass(frozen=True, slots=True)
class CulvertProject:
    """A reproducible set of crossings, scenarios, and design alternatives."""

    name: str
    crossings: tuple[CrossingDefinition, ...]
    scenarios: tuple[Scenario, ...]
    alternatives: tuple[Alternative, ...] = ()
    design_criteria: DesignCriteria | None = None
    schema_version: int = 1
    source: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty text."
            raise ValueError(msg)
        crossings = tuple(self.crossings)
        scenarios = tuple(self.scenarios)
        alternatives = tuple(self.alternatives)
        if not crossings:
            msg = "crossings must contain at least one crossing."
            raise ValueError(msg)
        if not scenarios:
            msg = "scenarios must contain at least one scenario."
            raise ValueError(msg)
        if self.schema_version != 1:
            msg = f"Unsupported culvert project schema_version {self.schema_version!r}; supported version: 1."
            raise ValueError(msg)
        crossing_names = [crossing.name for crossing in crossings]
        scenario_names = [scenario.name for scenario in scenarios]
        alternative_names = [alternative.name for alternative in alternatives]
        if len(crossing_names) != len(set(crossing_names)):
            msg = "Crossing names must be unique within a project."
            raise ValueError(msg)
        if len(scenario_names) != len(set(scenario_names)):
            msg = "Scenario names must be unique within a project."
            raise ValueError(msg)
        if len(alternative_names) != len(set(alternative_names)):
            msg = "Alternative names must be unique within a project."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "crossings", crossings)
        object.__setattr__(self, "scenarios", scenarios)
        object.__setattr__(self, "alternatives", alternatives)
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
