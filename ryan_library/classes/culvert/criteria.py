"""Typed design criteria for culvert candidate assessment."""

from dataclasses import dataclass
from math import isfinite


def _optional_limit(value: float | None, name: str, *, nonnegative: bool = False) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not isfinite(result):
        msg = f"{name} must be finite when supplied."
        raise ValueError(msg)
    if nonnegative and result < 0.0:
        msg = f"{name} must be nonnegative when supplied."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class DesignCriteria:
    """Explicit hydraulic constraints applied to every design scenario."""

    maximum_headwater_elevation: float | None = None
    maximum_headwater_depth: float | None = None
    maximum_headwater_ratio: float | None = None
    minimum_freeboard: float | None = None
    maximum_outlet_velocity: float | None = None
    maximum_roadway_discharge: float | None = None
    maximum_barrel_count: int | None = None
    maximum_total_structure_width: float | None = None
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
            "maximum_headwater_ratio",
            _optional_limit(self.maximum_headwater_ratio, "maximum_headwater_ratio", nonnegative=True),
        )
        object.__setattr__(
            self,
            "minimum_freeboard",
            _optional_limit(self.minimum_freeboard, "minimum_freeboard", nonnegative=True),
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
        object.__setattr__(
            self,
            "maximum_total_structure_width",
            _optional_limit(
                self.maximum_total_structure_width,
                "maximum_total_structure_width",
                nonnegative=True,
            ),
        )
        if self.maximum_barrel_count is not None:
            value: object = self.maximum_barrel_count
            if (
                isinstance(value, bool)
                or not isinstance(value, int)  # pyright: ignore[reportUnnecessaryIsInstance]
                or value <= 0
            ):
                msg = "maximum_barrel_count must be a strictly positive integer."
                raise ValueError(msg)
        if (
            self.maximum_headwater_elevation is None
            and self.maximum_headwater_depth is None
            and self.maximum_headwater_ratio is None
            and self.minimum_freeboard is None
            and self.maximum_outlet_velocity is None
            and self.maximum_roadway_discharge is None
            and self.maximum_barrel_count is None
            and self.maximum_total_structure_width is None
            and not self.require_resolved_result
        ):
            msg = "DesignCriteria must contain at least one active constraint."
            raise ValueError(msg)
