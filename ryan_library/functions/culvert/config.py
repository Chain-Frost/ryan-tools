"""Load simple JSON project configuration into typed culvert workflow models."""

import json
from pathlib import Path
from typing import cast

from ...classes.culvert.alternative import Alternative
from ...classes.culvert.crossing import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    RectangularBarrelDefinition,
    RoadwayDefinition,
)
from ...classes.culvert.project import CulvertProject
from ...classes.culvert.scenario import Scenario


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"{name} must be a JSON object."
        raise ValueError(msg)
    raw_mapping = cast("dict[object, object]", value)
    if not all(isinstance(key, str) for key in raw_mapping):
        msg = f"{name} must contain only string keys."
        raise ValueError(msg)
    return cast("dict[str, object]", raw_mapping)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        msg = f"{name} must be a JSON array."
        raise ValueError(msg)
    return cast(list[object], value)


def _text(mapping: dict[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        msg = f"{key} must be nonempty text."
        raise ValueError(msg)
    return value


def _float(mapping: dict[str, object], key: str) -> float:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        msg = f"{key} must be numeric."
        raise ValueError(msg)
    return float(value)


def _integer(mapping: dict[str, object], key: str, *, default: int | None = None) -> int:
    value = mapping.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{key} must be an integer."
        raise ValueError(msg)
    return value


def _parse_barrel(value: object) -> RectangularBarrelDefinition | CircularBarrelDefinition:
    data = _mapping(value, "barrel")
    shape = _text(data, "shape").lower()
    label_value = data.get("label", "")
    if not isinstance(label_value, str):
        msg = "barrel label must be text."
        raise ValueError(msg)
    material = CulvertMaterialName(_text(data, "material"))
    length = _float(data, "length")
    inlet_invert = _float(data, "inlet_invert")
    outlet_invert = _float(data, "outlet_invert")
    roughness = _float(data, "roughness")
    if shape == "rectangular":
        return RectangularBarrelDefinition(
            span_mm=_float(data, "span_mm"),
            rise_mm=_float(data, "rise_mm"),
            length=length,
            inlet_invert=inlet_invert,
            outlet_invert=outlet_invert,
            roughness=roughness,
            material=material,
            label=label_value,
        )
    if shape == "circular":
        return CircularBarrelDefinition(
            diameter_mm=_float(data, "diameter_mm"),
            length=length,
            inlet_invert=inlet_invert,
            outlet_invert=outlet_invert,
            roughness=roughness,
            material=material,
            label=label_value,
        )
    msg = "barrel shape must be 'rectangular' or 'circular'."
    raise ValueError(msg)


def _parse_crossing(value: object) -> CrossingDefinition:
    data = _mapping(value, "crossing")
    groups: list[CulvertGroupDefinition] = []
    for group_value in _list(data.get("groups"), "groups"):
        group_data = _mapping(group_value, "group")
        groups.append(
            CulvertGroupDefinition(
                name=_text(group_data, "name"),
                barrel=_parse_barrel(group_data.get("barrel")),
                quantity=_integer(group_data, "quantity", default=1),
            )
        )
    roadway: RoadwayDefinition | None = None
    roadway_value = data.get("roadway")
    if roadway_value is not None:
        roadway_data = _mapping(roadway_value, "roadway")
        label_value = roadway_data.get("label", "")
        if not isinstance(label_value, str):
            msg = "roadway label must be text."
            raise ValueError(msg)
        roadway = RoadwayDefinition(
            crest_elevation=_float(roadway_data, "crest_elevation"),
            crest_length=_float(roadway_data, "crest_length"),
            discharge_coefficient=_float(roadway_data, "discharge_coefficient"),
            label=label_value,
        )
    return CrossingDefinition(name=_text(data, "name"), groups=tuple(groups), roadway=roadway)


def _parse_scenario(value: object) -> Scenario:
    data = _mapping(value, "scenario")
    return Scenario(
        name=_text(data, "name"),
        discharge=_float(data, "discharge"),
        tailwater=_float(data, "tailwater_elevation"),
    )


def load_project_json(path: Path) -> CulvertProject:
    """Load a project JSON file. JSON configuration currently supports fixed tailwater elevations."""
    source = Path(path)
    raw_payload = cast(object, json.loads(source.read_text(encoding="utf-8")))
    payload = _mapping(raw_payload, "project")
    crossings = tuple(_parse_crossing(value) for value in _list(payload.get("crossings"), "crossings"))
    scenarios = tuple(_parse_scenario(value) for value in _list(payload.get("scenarios"), "scenarios"))
    alternatives_value = payload.get("alternatives", [])
    alternatives: list[Alternative] = []
    for value in _list(alternatives_value, "alternatives"):
        data = _mapping(value, "alternative")
        alternatives.append(
            Alternative(
                name=_text(data, "name"),
                crossing=_parse_crossing(data.get("crossing")),
            )
        )
    return CulvertProject(
        name=_text(payload, "name"),
        crossings=crossings,
        scenarios=scenarios,
        alternatives=tuple(alternatives),
    )
