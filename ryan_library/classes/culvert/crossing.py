"""Typed crossing definitions owned by the culvert workflow layer."""

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite


class CulvertMaterialName(StrEnum):
    """Supported solver material selections for the first workflow increment."""

    CONCRETE_BOX = "concrete_box"
    CONCRETE_PIPE = "concrete_pipe"
    CORRUGATED_STEEL = "corrugated_steel"


def _positive(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result) or result <= 0.0:
        msg = f"{name} must be finite and strictly positive."
        raise ValueError(msg)
    return result


def _finite(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result):
        msg = f"{name} must be finite."
        raise ValueError(msg)
    return result


def _nonempty(value: str, name: str) -> str:
    result = value.strip()
    if not result:
        msg = f"{name} must be nonempty text."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class RectangularBarrelDefinition:
    """Rectangular culvert barrel definition using internal dimensions in millimetres."""

    span_mm: float
    rise_mm: float
    length: float
    inlet_invert: float
    outlet_invert: float
    roughness: float
    material: CulvertMaterialName = CulvertMaterialName.CONCRETE_BOX
    label: str = ""

    def __post_init__(self) -> None:
        if self.material is not CulvertMaterialName.CONCRETE_BOX:
            msg = "Rectangular barrels currently require material='concrete_box'."
            raise ValueError(msg)
        object.__setattr__(self, "span_mm", _positive(self.span_mm, "span_mm"))
        object.__setattr__(self, "rise_mm", _positive(self.rise_mm, "rise_mm"))
        object.__setattr__(self, "length", _positive(self.length, "length"))
        inlet = _finite(self.inlet_invert, "inlet_invert")
        outlet = _finite(self.outlet_invert, "outlet_invert")
        if outlet > inlet:
            msg = "outlet_invert must not exceed inlet_invert."
            raise ValueError(msg)
        object.__setattr__(self, "inlet_invert", inlet)
        object.__setattr__(self, "outlet_invert", outlet)
        object.__setattr__(self, "roughness", _positive(self.roughness, "roughness"))
        object.__setattr__(self, "label", self.label.strip())


@dataclass(frozen=True, slots=True)
class CircularBarrelDefinition:
    """Circular culvert barrel definition using internal diameter in millimetres."""

    diameter_mm: float
    length: float
    inlet_invert: float
    outlet_invert: float
    roughness: float
    material: CulvertMaterialName = CulvertMaterialName.CONCRETE_PIPE
    label: str = ""

    def __post_init__(self) -> None:
        if self.material not in {
            CulvertMaterialName.CONCRETE_PIPE,
            CulvertMaterialName.CORRUGATED_STEEL,
        }:
            msg = "Circular barrels require concrete_pipe or corrugated_steel material."
            raise ValueError(msg)
        object.__setattr__(self, "diameter_mm", _positive(self.diameter_mm, "diameter_mm"))
        object.__setattr__(self, "length", _positive(self.length, "length"))
        inlet = _finite(self.inlet_invert, "inlet_invert")
        outlet = _finite(self.outlet_invert, "outlet_invert")
        if outlet > inlet:
            msg = "outlet_invert must not exceed inlet_invert."
            raise ValueError(msg)
        object.__setattr__(self, "inlet_invert", inlet)
        object.__setattr__(self, "outlet_invert", outlet)
        object.__setattr__(self, "roughness", _positive(self.roughness, "roughness"))
        object.__setattr__(self, "label", self.label.strip())


type BarrelDefinition = RectangularBarrelDefinition | CircularBarrelDefinition


@dataclass(frozen=True, slots=True)
class CulvertGroupDefinition:
    """One named group of hydraulically identical parallel barrels."""

    name: str
    barrel: BarrelDefinition
    quantity: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _nonempty(self.name, "name"))
        quantity: object = self.quantity
        if (
            isinstance(quantity, bool)
            or not isinstance(quantity, int)  # pyright: ignore[reportUnnecessaryIsInstance]
            or quantity <= 0
        ):
            msg = "quantity must be a strictly positive integer."
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class RoadwayDefinition:
    """Optional constant-elevation roadway-weir definition."""

    crest_elevation: float
    crest_length: float
    discharge_coefficient: float
    label: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "crest_elevation", _finite(self.crest_elevation, "crest_elevation"))
        object.__setattr__(self, "crest_length", _positive(self.crest_length, "crest_length"))
        object.__setattr__(
            self,
            "discharge_coefficient",
            _positive(self.discharge_coefficient, "discharge_coefficient"),
        )
        object.__setattr__(self, "label", self.label.strip())


@dataclass(frozen=True, slots=True)
class CrossingDefinition:
    """A named crossing made from one or more culvert groups and optional roadway."""

    name: str
    groups: tuple[CulvertGroupDefinition, ...]
    roadway: RoadwayDefinition | None = None
    source: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _nonempty(self.name, "name"))
        groups = tuple(self.groups)
        if not groups:
            msg = "groups must contain at least one culvert group."
            raise ValueError(msg)
        names = [group.name for group in groups]
        if len(names) != len(set(names)):
            msg = "Culvert group names must be unique within a crossing."
            raise ValueError(msg)
        object.__setattr__(self, "groups", groups)
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
