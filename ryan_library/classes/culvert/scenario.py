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
            msg = "name must be nonempty text."
            raise ValueError(msg)
        discharge = float(self.discharge)
        if not isfinite(discharge) or discharge <= 0.0:
            msg = "discharge must be finite and strictly positive."
            raise ValueError(msg)
        tailwater: object = self.tailwater
        if isinstance(tailwater, bool):
            msg = "tailwater must be a finite elevation or TailwaterBoundary."
            raise ValueError(msg)
        if isinstance(tailwater, (float, int)):
            if not isfinite(float(tailwater)):
                msg = "Numeric tailwater elevation must be finite."
                raise ValueError(msg)
        elif not isinstance(tailwater, TailwaterBoundary):  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = "tailwater must be a finite elevation or TailwaterBoundary."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "discharge", discharge)
