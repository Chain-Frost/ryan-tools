"""Typed design criteria for culvert candidate assessment."""

from dataclasses import dataclass
from math import isfinite


def _optional_limit(value: float | None, name: str, *, nonnegative: bool = False) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not isfinite(result):
        raise ValueError(f"{name} must be finite when supplied.")
    if nonnegative and result < 0.0:
        raise ValueError(f"{name} must be nonnegative when supplied.")
    return result


@dataclass(frozen=True, slots=True)
class DesignCriteria:
    """Explicit hydraulic constraints applied to every design scenario."""

    maximum_headwater_elevation: float | None = None
    maximum_headwater_depth: float | None = None
    maximum_outlet_velocity: float | None = None
    maximum_roadway_discharge: float | None = None
    require_resolved_result: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "maximum_headwater_elevation",
            _optional_limit(self.maximum_headwater_elevation, "maximum_headwater_elevation"),
        )
        object.__setattr__(
            self,
            "maximum_headwater_depth",
            _optional_limit(self.maximum_headwater_depth, "maximum_headwater_depth", nonnegative=True),
        )
        object.__setattr__(
            self,
            "maximum_outlet_velocity",
            _optional_limit(self.maximum_outlet_velocity, "maximum_outlet_velocity", nonnegative=True),
        )
        object.__setattr__(
            self,
            "maximum_roadway_discharge",
            _optional_limit(self.maximum_roadway_discharge, "maximum_roadway_discharge", nonnegative=True),
        )
        if (
            self.maximum_headwater_elevation is None
            and self.maximum_headwater_depth is None
            and self.maximum_outlet_velocity is None
            and self.maximum_roadway_discharge is None
            and not self.require_resolved_result
        ):
            raise ValueError("DesignCriteria must contain at least one active constraint.")
