"""Per-crossing physical configuration and TUFLOW loss attributes."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd

from ...classes.culvert import CulvertMaterialName
from .tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    TuflowLossParameters,
    parse_circular_inlet_configuration,
)

ID_FIELDS: tuple[str, ...] = ("ID", "Chan ID", "Crossing")
MATERIAL_FIELDS: tuple[str, ...] = ("Material", "Culvert Material", "Culvert_Material")
INLET_FIELDS: tuple[str, ...] = (
    "Inlet",
    "Inlet Configuration",
    "InletConfiguration",
    "Inlet_Configuration",
)
ENTRY_LOSS_FIELDS: tuple[str, ...] = ("Entry Loss", "Entry_Loss", "EntryC_or_WSa")
EXIT_LOSS_FIELDS: tuple[str, ...] = ("Exit Loss", "Exit_Loss", "ExitC_or_WSb")
FORM_LOSS_FIELDS: tuple[str, ...] = ("Form_Loss", "Form Loss", "Fixed Loss", "Fixed_Loss")
WIDTH_CONTRACTION_FIELDS: tuple[str, ...] = (
    "WConF_or_WEx",
    "Width Contraction",
    "Width_Contraction",
)

_MATERIAL_ALIASES: dict[str, CulvertMaterialName] = {
    "concrete": CulvertMaterialName.CONCRETE_PIPE,
    "concrete_pipe": CulvertMaterialName.CONCRETE_PIPE,
    "concrete pipe": CulvertMaterialName.CONCRETE_PIPE,
    "rcp": CulvertMaterialName.CONCRETE_PIPE,
    "csp": CulvertMaterialName.CORRUGATED_STEEL,
    "corrugated_steel": CulvertMaterialName.CORRUGATED_STEEL,
    "corrugated steel": CulvertMaterialName.CORRUGATED_STEEL,
    "hdpe": CulvertMaterialName.SMOOTH_HDPE,
    "smooth_hdpe": CulvertMaterialName.SMOOTH_HDPE,
    "smooth hdpe": CulvertMaterialName.SMOOTH_HDPE,
}


@dataclass(frozen=True, slots=True)
class TuflowCulvertAttributes:
    """Optional per-crossing physical and model-specific attributes."""

    crossing_id: str
    material: CulvertMaterialName | None = None
    inlet_configuration: CircularInletConfiguration | None = None
    losses: TuflowLossParameters = field(default_factory=TuflowLossParameters)


def parse_culvert_material(value: str) -> CulvertMaterialName:
    """Parse an explicit circular culvert material."""
    key = value.strip().lower()
    material = _MATERIAL_ALIASES.get(key)
    if material is None:
        allowed = "concrete/rcp, csp/corrugated_steel, or hdpe/smooth_hdpe"
        msg = f"Unsupported culvert material {value!r}; expected {allowed}."
        raise ValueError(msg)
    return material


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return cast("bool", pd.isna(cast("Any", value)))
    except TypeError, ValueError:
        return False


def _first_text(row: dict[str, Any], fields: tuple[str, ...]) -> str | None:
    for field_name in fields:
        value = row.get(field_name)
        if _is_missing(value):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_float(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    for field_name in fields:
        raw = row.get(field_name)
        if _is_missing(raw):
            continue
        try:
            value = float(cast("Any", raw))
        except (TypeError, ValueError) as exc:
            msg = f"{field_name} must contain a numeric value."
            raise ValueError(msg) from exc
        if not isfinite(value):
            msg = f"{field_name} must contain a finite numeric value."
            raise ValueError(msg)
        return value
    return None


def material_from_mapping(row: dict[str, Any]) -> CulvertMaterialName | None:
    """Return an explicit material from a mapping when present."""
    value = _first_text(row, MATERIAL_FIELDS)
    return None if value is None else parse_culvert_material(value)


def inlet_from_mapping(row: dict[str, Any]) -> CircularInletConfiguration | None:
    """Return an explicit physical inlet configuration when present."""
    value = _first_text(row, INLET_FIELDS)
    return None if value is None else parse_circular_inlet_configuration(value)


def losses_from_mapping(row: dict[str, Any]) -> TuflowLossParameters:
    """Read TUFLOW numeric loss fields without inferring physical configuration."""
    return TuflowLossParameters(
        entry_loss_coefficient=_first_float(row, ENTRY_LOSS_FIELDS),
        exit_loss_coefficient=_first_float(row, EXIT_LOSS_FIELDS),
        form_loss_coefficient=_first_float(row, FORM_LOSS_FIELDS),
        width_contraction_coefficient=_first_float(row, WIDTH_CONTRACTION_FIELDS),
    )


def resolve_circular_configuration(
    row: dict[str, Any],
    *,
    attributes: TuflowCulvertAttributes | None = None,
    material_override: CulvertMaterialName | None = None,
    inlet_override: CircularInletConfiguration | None = None,
) -> CircularCulvertConfiguration:
    """Resolve physical configuration by explicit precedence, never from roughness/losses."""
    material = material_override
    if material is None and attributes is not None:
        material = attributes.material
    if material is None:
        material = material_from_mapping(row)

    inlet = inlet_override
    if inlet is None and attributes is not None:
        inlet = attributes.inlet_configuration
    if inlet is None:
        inlet = inlet_from_mapping(row)

    if material is None:
        msg = (
            "Circular culvert material is unresolved; provide --material, a per-crossing "
            "--culvert-attributes source, or an explicit Material column."
        )
        raise ValueError(msg)
    if inlet is None:
        msg = (
            "Circular inlet configuration is unresolved; provide --inlet-configuration, "
            "a per-crossing --culvert-attributes source, or an explicit Inlet Configuration column."
        )
        raise ValueError(msg)
    return CircularCulvertConfiguration(material=material, inlet=inlet)


def resolve_tuflow_losses(
    row: dict[str, Any],
    *,
    attributes: TuflowCulvertAttributes | None = None,
) -> TuflowLossParameters:
    """Merge model-specific loss evidence, with per-crossing attributes taking precedence."""
    inline = losses_from_mapping(row)
    if attributes is None:
        return inline
    external = attributes.losses
    return TuflowLossParameters(
        entry_loss_coefficient=(
            external.entry_loss_coefficient
            if external.entry_loss_coefficient is not None
            else inline.entry_loss_coefficient
        ),
        exit_loss_coefficient=(
            external.exit_loss_coefficient
            if external.exit_loss_coefficient is not None
            else inline.exit_loss_coefficient
        ),
        form_loss_coefficient=(
            external.form_loss_coefficient
            if external.form_loss_coefficient is not None
            else inline.form_loss_coefficient
        ),
        width_contraction_coefficient=(
            external.width_contraction_coefficient
            if external.width_contraction_coefficient is not None
            else inline.width_contraction_coefficient
        ),
    )


def _resolve_vector_layer(path: Path, requested: str | None) -> str | None:
    if requested:
        return requested
    if path.suffix.lower() != ".gpkg":
        return None
    layers = gpd.list_layers(path)
    names = [str(name).strip() for name in layers["name"].tolist() if str(name).strip()]
    candidates = [name for name in names if "1d_nwk" in name.lower()]
    if len(candidates) == 1:
        return candidates[0]
    if len(names) == 1:
        return names[0]
    if not names:
        msg = f"No layers found in culvert attribute GeoPackage: {path}."
        raise ValueError(msg)
    joined = ", ".join(names)
    msg = f"Culvert attribute GeoPackage layer is ambiguous; pass --attributes-layer. Available layers: {joined}"
    raise ValueError(msg)


def load_tuflow_culvert_attributes(
    path: Path | None,
    *,
    layer: str | None = None,
) -> dict[str, TuflowCulvertAttributes]:
    """Load optional per-crossing physical/loss attributes from CSV or vector data."""
    if path is None:
        return {}

    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, dtype=dict.fromkeys(ID_FIELDS, str))
    else:
        resolved_layer = _resolve_vector_layer(path, layer)
        frame = gpd.read_file(path, layer=resolved_layer)  # pyright: ignore[reportUnknownMemberType]

    rows = cast("list[dict[str, Any]]", frame.where(frame.notna(), None).to_dict(orient="records"))
    result: dict[str, TuflowCulvertAttributes] = {}
    for row_number, row in enumerate(rows, start=1):
        crossing_id = _first_text(row, ID_FIELDS)
        if crossing_id is None:
            msg = f"Culvert attribute source row {row_number} requires one identifier column: " + ", ".join(ID_FIELDS)
            raise ValueError(msg)
        if crossing_id in result:
            msg = f"Duplicate culvert attribute identifier {crossing_id!r}."
            raise ValueError(msg)

        material_text = _first_text(row, MATERIAL_FIELDS)
        inlet_text = _first_text(row, INLET_FIELDS)
        result[crossing_id] = TuflowCulvertAttributes(
            crossing_id=crossing_id,
            material=parse_culvert_material(material_text) if material_text is not None else None,
            inlet_configuration=(parse_circular_inlet_configuration(inlet_text) if inlet_text is not None else None),
            losses=losses_from_mapping(row),
        )
    return result


__all__ = [
    "TuflowCulvertAttributes",
    "inlet_from_mapping",
    "load_tuflow_culvert_attributes",
    "losses_from_mapping",
    "material_from_mapping",
    "parse_culvert_material",
    "resolve_circular_configuration",
    "resolve_tuflow_losses",
]
