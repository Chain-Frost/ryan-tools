"""Typed hydraulic scenario configuration for culvert workflows."""

from dataclasses import dataclass
from math import isfinite

from culvert_solver import TailwaterBoundary, TailwaterInput


def _validate_aep(value: float | None) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not isfinite(result) or not 0.0 < result <= 100.0:
        msg = "aep_percent must be greater than 0 and no greater than 100."
        raise ValueError(msg)
    return result


def _validate_target(value: float | None) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not isfinite(result):
        msg = "target_headwater_elevation must be finite when supplied."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class Scenario:
    """One design flow and downstream boundary condition."""

    name: str
    discharge: float
    tailwater: TailwaterInput
    aep_percent: float | None = None
    source: str | None = None
    notes: str = ""
    target_headwater_elevation: float | None = None

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
        object.__setattr__(self, "aep_percent", _validate_aep(self.aep_percent))
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
        object.__setattr__(
            self,
            "target_headwater_elevation",
            _validate_target(self.target_headwater_elevation),
        )
