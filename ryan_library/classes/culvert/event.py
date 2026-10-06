"""Imported hydraulic event targets; no hydrology is performed here."""

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True, slots=True)
class EventDefinition:
    """One imported event with exactly one supplied hydraulic target."""

    name: str | None = None
    aep_percent: float | None = None
    discharge_m3s: float | None = None
    target_headwater_elevation_m: float | None = None
    tailwater_elevation_m: float | None = None
    source: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = None if self.name is None else self.name.strip()
        if not name and self.aep_percent is None:
            msg = "An imported event requires name, aep_percent, or both."
            raise ValueError(msg)
        if (self.discharge_m3s is None) == (self.target_headwater_elevation_m is None):
            msg = "An imported event requires exactly one of discharge_m3s or target_headwater_elevation_m."
            raise ValueError(msg)
        for field_name in (
            "aep_percent",
            "discharge_m3s",
            "target_headwater_elevation_m",
            "tailwater_elevation_m",
        ):
            value = getattr(self, field_name)
            if value is not None and not isfinite(float(value)):
                msg = f"{field_name} must be finite when supplied."
                raise ValueError(msg)
        if self.aep_percent is not None and not 0.0 < self.aep_percent <= 100.0:
            msg = "aep_percent must be greater than 0 and no greater than 100."
            raise ValueError(msg)
        if self.discharge_m3s is not None and self.discharge_m3s <= 0.0:
            msg = "discharge_m3s must be strictly positive."
            raise ValueError(msg)
        object.__setattr__(self, "name", name or None)
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())

    @property
    def scenario_name(self) -> str:
        """Return a stable display name when only AEP was supplied."""
        return self.name or f"{self.aep_percent:g}% AEP"
