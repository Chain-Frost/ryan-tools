"""Typed hydraulic scenario configuration for culvert workflows."""

from dataclasses import dataclass
from math import isfinite

from culvert_solver import TailwaterBoundary, TailwaterInput


@dataclass(frozen=True, slots=True)
class Scenario:
    """One design flow and downstream boundary condition."""

    name: str
    discharge: float
    tailwater: TailwaterInput

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            raise ValueError("name must be nonempty text.")
        discharge = float(self.discharge)
        if not isfinite(discharge) or discharge <= 0.0:
            raise ValueError("discharge must be finite and strictly positive.")
        tailwater = self.tailwater
        if isinstance(tailwater, bool):
            raise ValueError("tailwater must be a finite elevation or TailwaterBoundary.")
        if isinstance(tailwater, (float, int)):
            if not isfinite(float(tailwater)):
                raise ValueError("Numeric tailwater elevation must be finite.")
        elif not isinstance(tailwater, TailwaterBoundary):
            raise ValueError("tailwater must be a finite elevation or TailwaterBoundary.")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "discharge", discharge)
