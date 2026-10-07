"""Typed physical configuration for TUFLOW culvert reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite

from ...classes.culvert import CulvertMaterialName


class CulvertShapeName(StrEnum):
    """Physical culvert shape independently of any hydraulic backend."""

    CIRCULAR = "circular"
    BOX = "box"


class CircularInletConfiguration(StrEnum):
    """Physical inlet treatments supported by the circular TUFLOW adapter."""

    SQUARE_EDGE_HEADWALL = "square-edge-headwall"
    THIN_EDGE_PROJECTING = "thin-edge-projecting"
    GROOVED_END_PROJECTING = "grooved-end-projecting"
    GROOVED_END_HEADWALL = "grooved-end-headwall"
    MITERED_TO_SLOPE = "mitered-to-slope"
    BEVELED_EDGE_1_TO_1 = "beveled-edge-1-to-1"
    BEVELED_EDGE_1_5_TO_1 = "beveled-edge-1-5-to-1"


_ALLOWED_CIRCULAR_INLETS: dict[CulvertMaterialName, frozenset[CircularInletConfiguration]] = {
    CulvertMaterialName.CONCRETE_PIPE: frozenset(
        {
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
            CircularInletConfiguration.GROOVED_END_PROJECTING,
            CircularInletConfiguration.GROOVED_END_HEADWALL,
            CircularInletConfiguration.MITERED_TO_SLOPE,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1,
        }
    ),
    CulvertMaterialName.CORRUGATED_STEEL: frozenset(
        {
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
            CircularInletConfiguration.THIN_EDGE_PROJECTING,
            CircularInletConfiguration.MITERED_TO_SLOPE,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1,
        }
    ),
    CulvertMaterialName.SMOOTH_HDPE: frozenset(
        {
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
            CircularInletConfiguration.THIN_EDGE_PROJECTING,
            CircularInletConfiguration.MITERED_TO_SLOPE,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1,
        }
    ),
}


@dataclass(frozen=True, slots=True)
class CircularCulvertConfiguration:
    """Physical configuration of a circular culvert barrel."""

    material: CulvertMaterialName
    inlet: CircularInletConfiguration

    @property
    def shape(self) -> CulvertShapeName:
        return CulvertShapeName.CIRCULAR

    def __post_init__(self) -> None:
        if self.material not in _ALLOWED_CIRCULAR_INLETS:
            msg = f"Unsupported circular culvert material: {self.material.value}."
            raise ValueError(msg)
        if self.inlet not in _ALLOWED_CIRCULAR_INLETS[self.material]:
            msg = (
                f"Inlet configuration {self.inlet.value!r} is not valid for circular material {self.material.value!r}."
            )
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TuflowLossParameters:
    """TUFLOW numeric loss parameters kept separate from physical configuration."""

    entry_loss_coefficient: float | None = None
    exit_loss_coefficient: float | None = None
    form_loss_coefficient: float | None = None
    width_contraction_coefficient: float | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "entry_loss_coefficient",
            "exit_loss_coefficient",
            "form_loss_coefficient",
            "width_contraction_coefficient",
        ):
            raw = getattr(self, field_name)
            if raw is None:
                continue
            value = float(raw)
            if not isfinite(value):
                msg = f"{field_name} must be finite when supplied."
                raise ValueError(msg)
            if field_name != "width_contraction_coefficient" and value < 0.0:
                msg = f"{field_name} must be nonnegative when supplied."
                raise ValueError(msg)
            object.__setattr__(self, field_name, value)


_SHAPE_ALIASES: dict[str, CulvertShapeName] = {
    "c": CulvertShapeName.CIRCULAR,
    "circular": CulvertShapeName.CIRCULAR,
    "pipe": CulvertShapeName.CIRCULAR,
    "r": CulvertShapeName.BOX,
    "box": CulvertShapeName.BOX,
    "rectangular": CulvertShapeName.BOX,
}

_INLET_ALIASES: dict[str, CircularInletConfiguration] = {
    "square-edge-headwall": CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
    "square-edge-with-headwall": CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
    "headwall": CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
    "thin-edge-projecting": CircularInletConfiguration.THIN_EDGE_PROJECTING,
    "projecting": CircularInletConfiguration.THIN_EDGE_PROJECTING,
    "grooved-end-projecting": CircularInletConfiguration.GROOVED_END_PROJECTING,
    "grooved-end-headwall": CircularInletConfiguration.GROOVED_END_HEADWALL,
    "grooved-end-in-headwall": CircularInletConfiguration.GROOVED_END_HEADWALL,
    "mitered-to-slope": CircularInletConfiguration.MITERED_TO_SLOPE,
    "mitered-to-conform-to-slope": CircularInletConfiguration.MITERED_TO_SLOPE,
    "beveled-edge-1-to-1": CircularInletConfiguration.BEVELED_EDGE_1_TO_1,
    "beveled-edge-1-5-to-1": CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1,
}


def _normalise_token(value: str) -> str:
    return "-".join(value.strip().lower().replace("_", " ").split())


def parse_tuflow_shape(value: str) -> CulvertShapeName:
    """Parse TUFLOW Type values into a physical shape."""
    key = _normalise_token(value)
    shape = _SHAPE_ALIASES.get(key)
    if shape is None:
        msg = f"Unsupported TUFLOW culvert shape/type {value!r}."
        raise ValueError(msg)
    return shape


def parse_circular_inlet_configuration(value: str) -> CircularInletConfiguration:
    """Parse a backend-neutral physical circular inlet description."""
    key = _normalise_token(value)
    inlet = _INLET_ALIASES.get(key)
    if inlet is None:
        available = ", ".join(item.value for item in CircularInletConfiguration)
        msg = f"Unsupported circular inlet configuration {value!r}; expected one of: {available}."
        raise ValueError(msg)
    return inlet


__all__ = [
    "CircularCulvertConfiguration",
    "CircularInletConfiguration",
    "CulvertShapeName",
    "TuflowLossParameters",
    "parse_circular_inlet_configuration",
    "parse_tuflow_shape",
]
