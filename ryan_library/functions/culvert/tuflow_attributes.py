"""Per-crossing material and loss attributes for TUFLOW culvert wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd

from ...classes.culvert import CulvertMaterialName

ID_FIELDS: tuple[str, ...] = ("ID", "Chan ID", "Crossing")
MATERIAL_FIELDS: tuple[str, ...] = ("Material", "Culvert Material", "Culvert_Material")
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
    "rcp": CulvertMaterialName.CONCRETE_PIPE,
    "csp": CulvertMaterialName.CORRUGATED_STEEL,
    "corrugated_steel": CulvertMaterialName.CORRUGATED_STEEL,
    "corrugated steel": CulvertMaterialName.CORRUGATED_STEEL,
}


@dataclass(frozen=True, slots=True)
class TuflowCulvertAttributes:
    """Optional per-crossing material and loss attributes."""

    crossing_id: str
    material: CulvertMaterialName | None = None
    entry_loss: float | None = None
    exit_loss: float | None = None
    form_loss: float | None = None
    width_contraction: float | None = None


def parse_culvert_material(value: str) -> CulvertMaterialName:
    """Parse a supported explicit circular culvert material."""
    key = value.strip().lower()
    material = _MATERIAL_ALIASES.get(key)
    if material is None:
        allowed = "concrete/concrete_pipe/rcp or csp/corrugated_steel"
        msg = f"Unsupported culvert material {value!r}; expected {allowed}."
        raise ValueError(msg)
    return material


def material_from_mapping(row: dict[str, Any]) -> CulvertMaterialName | None:
    """Return an explicit material from a mapping when one is present."""
    for field in MATERIAL_FIELDS:
        value = row.get(field)
        if _is_missing(value):
            continue
        text = str(value).strip()
        if text:
            return parse_culvert_material(text)
    return None


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _first_text(row: dict[str, Any], fields: tuple[str, ...]) -> str | None:
    for field in fields:
        value = row.get(field)
        if _is_missing(value):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_float(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    for field in fields:
        raw = row.get(field)
        if _is_missing(raw):
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            msg = f"{field} must contain a numeric value."
            raise ValueError(msg) from exc
        if not isfinite(value):
            msg = f"{field} must contain a finite numeric value."
            raise ValueError(msg)
        return value
    return None


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
    msg = (
        "Culvert attribute GeoPackage layer is ambiguous; pass --attributes-layer. "
        f"Available layers: {joined}"
    )
    raise ValueError(msg)


def load_tuflow_culvert_attributes(
    path: Path | None,
    *,
    layer: str | None = None,
) -> dict[str, TuflowCulvertAttributes]:
    """Load optional per-crossing material/loss attributes from CSV or vector data."""
    if path is None:
        return {}

    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    else:
        resolved_layer = _resolve_vector_layer(path, layer)
        frame = gpd.read_file(path, layer=resolved_layer)  # pyright: ignore[reportUnknownMemberType]

    rows = cast("list[dict[str, Any]]", frame.where(frame.notna(), None).to_dict(orient="records"))
    result: dict[str, TuflowCulvertAttributes] = {}
    for row_number, row in enumerate(rows, start=1):
        crossing_id = _first_text(row, ID_FIELDS)
        if crossing_id is None:
            msg = (
                f"Culvert attribute source row {row_number} requires one identifier column: "
                + ", ".join(ID_FIELDS)
            )
            raise ValueError(msg)
        if crossing_id in result:
            msg = f"Duplicate culvert attribute identifier {crossing_id!r}."
            raise ValueError(msg)

        material_text = _first_text(row, MATERIAL_FIELDS)
        result[crossing_id] = TuflowCulvertAttributes(
            crossing_id=crossing_id,
            material=parse_culvert_material(material_text) if material_text is not None else None,
            entry_loss=_first_float(row, ENTRY_LOSS_FIELDS),
            exit_loss=_first_float(row, EXIT_LOSS_FIELDS),
            form_loss=_first_float(row, FORM_LOSS_FIELDS),
            width_contraction=_first_float(row, WIDTH_CONTRACTION_FIELDS),
        )
    return result
