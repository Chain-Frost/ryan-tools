"""Strict versioned project-file parsing and deterministic JSON serialization."""

import json
import tomllib
from pathlib import Path
from typing import cast

from culvert_solver import ManningChannelTailwater, RectangularChannel, TailwaterCondition, TrapezoidalChannel

from ...classes.culvert.alternative import Alternative
from ...classes.culvert.criteria import DesignCriteria
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

SCHEMA_VERSION = 1


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"{name} must be an object/table."
        raise ValueError(msg)
    raw = cast("dict[object, object]", value)
    if not all(isinstance(key, str) for key in raw):
        msg = f"{name} must contain only string keys."
        raise ValueError(msg)
    return cast("dict[str, object]", raw)


def _reject_unknown(data: dict[str, object], allowed: set[str], name: str) -> None:
    if unknown := sorted(data.keys() - allowed):
        msg = f"{name} contains unknown field(s): {', '.join(unknown)}."
        raise ValueError(msg)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        msg = f"{name} must be an array."
        raise ValueError(msg)
    return cast("list[object]", value)


def _text(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        msg = f"{key} must be nonempty text."
        raise ValueError(msg)
    return value.strip()


def _optional_text(data: dict[str, object], key: str) -> str | None:
    return None if data.get(key) is None else _text(data, key)


def _number(data: dict[str, object], key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        msg = f"{key} must be numeric."
        raise ValueError(msg)
    return float(value)


def _integer(data: dict[str, object], key: str, default: int | None = None) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{key} must be an integer."
        raise ValueError(msg)
    return value


def _optional_number(data: dict[str, object], key: str) -> float | None:
    return None if data.get(key) is None else _number(data, key)


def _metadata(data: dict[str, object]) -> tuple[str | None, str]:
    source = _optional_text(data, "source")
    notes = data.get("notes", "")
    if not isinstance(notes, str):
        msg = "notes must be text."
        raise ValueError(msg)
    return source, notes


def _parse_barrel(value: object) -> RectangularBarrelDefinition | CircularBarrelDefinition:
    data = _mapping(value, "barrel")
    common = {
        "shape",
        "material",
        "length_m",
        "inlet_invert_elevation_m",
        "outlet_invert_elevation_m",
        "roughness_manning_n",
        "label",
    }
    shape = _text(data, "shape")
    dimensions = {"span_mm", "rise_mm"} if shape == "rectangular" else {"diameter_mm"}
    _reject_unknown(data, common | dimensions, "barrel")
    label = data.get("label", "")
    if not isinstance(label, str):
        msg = "barrel label must be text."
        raise ValueError(msg)
    material = _text(data, "material")
    length = _number(data, "length_m")
    inlet_invert = _number(data, "inlet_invert_elevation_m")
    outlet_invert = _number(data, "outlet_invert_elevation_m")
    roughness = _number(data, "roughness_manning_n")
    material_name = CulvertMaterialName(material)
    if shape == "rectangular":
        return RectangularBarrelDefinition(
            span_mm=_number(data, "span_mm"),
            rise_mm=_number(data, "rise_mm"),
            length=length,
            inlet_invert=inlet_invert,
            outlet_invert=outlet_invert,
            roughness=roughness,
            material=material_name,
            label=label,
        )
    if shape == "circular":
        return CircularBarrelDefinition(
            diameter_mm=_number(data, "diameter_mm"),
            length=length,
            inlet_invert=inlet_invert,
            outlet_invert=outlet_invert,
            roughness=roughness,
            material=material_name,
            label=label,
        )
    msg = "barrel shape must be 'rectangular' or 'circular'."
    raise ValueError(msg)


def _parse_roadway(value: object) -> RoadwayDefinition:
    data = _mapping(value, "roadway")
    _reject_unknown(
        data,
        {"crest_elevation_m", "crest_length_m", "discharge_coefficient", "label"},
        "roadway",
    )
    label = data.get("label", "")
    if not isinstance(label, str):
        msg = "roadway label must be text."
        raise ValueError(msg)
    return RoadwayDefinition(
        crest_elevation=_number(data, "crest_elevation_m"),
        crest_length=_number(data, "crest_length_m"),
        discharge_coefficient=_number(data, "discharge_coefficient"),
        label=label,
    )


def _parse_crossing(value: object) -> CrossingDefinition:
    data = _mapping(value, "crossing")
    _reject_unknown(data, {"name", "groups", "roadway", "source", "notes"}, "crossing")
    groups: list[CulvertGroupDefinition] = []
    for value in _list(data.get("groups"), "groups"):
        group = _mapping(value, "group")
        _reject_unknown(group, {"name", "quantity", "barrel"}, "group")
        name = _text(group, "name")
        groups.append(
            CulvertGroupDefinition(
                name=name,
                barrel=_parse_barrel(group.get("barrel")),
                quantity=_integer(group, "quantity", 1),
            )
        )
    name = _text(data, "name")
    source, notes = _metadata(data)
    roadway_value = data.get("roadway")
    return CrossingDefinition(
        name=name,
        groups=tuple(groups),
        roadway=None if roadway_value is None else _parse_roadway(roadway_value),
        source=source,
        notes=notes,
    )


def _parse_tailwater(value: object) -> TailwaterCondition | ManningChannelTailwater:
    data = _mapping(value, "tailwater")
    tailwater_type = _text(data, "type")
    if tailwater_type == "fixed":
        _reject_unknown(data, {"type", "elevation_m"}, "tailwater")
        return TailwaterCondition(elevation=_number(data, "elevation_m"))
    if tailwater_type != "manning_channel":
        msg = "tailwater type must be 'fixed' or 'manning_channel'."
        raise ValueError(msg)
    _reject_unknown(
        data,
        {
            "type",
            "channel_invert_elevation_m",
            "roughness_manning_n",
            "friction_slope",
            "section",
        },
        "tailwater",
    )
    section_data = _mapping(data.get("section"), "tailwater.section")
    shape = _text(section_data, "shape")
    if shape == "rectangular":
        _reject_unknown(section_data, {"shape", "bottom_width_m"}, "tailwater.section")
        section = RectangularChannel(bottom_width=_number(section_data, "bottom_width_m"))
    elif shape == "trapezoidal":
        _reject_unknown(
            section_data,
            {
                "shape",
                "bottom_width_m",
                "left_side_slope_h_to_v",
                "right_side_slope_h_to_v",
            },
            "tailwater.section",
        )
        section = TrapezoidalChannel(
            bottom_width=_number(section_data, "bottom_width_m"),
            left_side_slope=_number(section_data, "left_side_slope_h_to_v"),
            right_side_slope=_number(section_data, "right_side_slope_h_to_v"),
        )
    else:
        msg = "tailwater.section shape must be 'rectangular' or 'trapezoidal'."
        raise ValueError(msg)
    return ManningChannelTailwater(
        section=section,
        channel_invert_elevation=_number(data, "channel_invert_elevation_m"),
        roughness=_number(data, "roughness_manning_n"),
        friction_slope=_number(data, "friction_slope"),
    )


def _parse_scenario(value: object) -> Scenario:
    data = _mapping(value, "scenario")
    _reject_unknown(
        data,
        {"name", "discharge_m3s", "aep_percent", "tailwater", "source", "notes"},
        "scenario",
    )
    name = _text(data, "name")
    source, notes = _metadata(data)
    aep = None if data.get("aep_percent") is None else _number(data, "aep_percent")
    return Scenario(
        name=name,
        discharge=_number(data, "discharge_m3s"),
        tailwater=_parse_tailwater(data.get("tailwater")),
        aep_percent=aep,
        source=source,
        notes=notes,
    )


def _parse_design_criteria(value: object) -> DesignCriteria:
    data = _mapping(value, "design_criteria")
    _reject_unknown(
        data,
        {
            "maximum_headwater_elevation_m",
            "maximum_headwater_depth_m",
            "maximum_headwater_ratio",
            "minimum_freeboard_m",
            "maximum_outlet_velocity_ms",
            "maximum_roadway_discharge_m3s",
            "maximum_barrel_count",
            "maximum_total_structure_width_m",
            "require_resolved_result",
        },
        "design_criteria",
    )
    resolved = data.get("require_resolved_result", True)
    if not isinstance(resolved, bool):
        msg = "require_resolved_result must be boolean."
        raise ValueError(msg)
    return DesignCriteria(
        maximum_headwater_elevation=_optional_number(data, "maximum_headwater_elevation_m"),
        maximum_headwater_depth=_optional_number(data, "maximum_headwater_depth_m"),
        maximum_headwater_ratio=_optional_number(data, "maximum_headwater_ratio"),
        minimum_freeboard=_optional_number(data, "minimum_freeboard_m"),
        maximum_outlet_velocity=_optional_number(data, "maximum_outlet_velocity_ms"),
        maximum_roadway_discharge=_optional_number(data, "maximum_roadway_discharge_m3s"),
        maximum_barrel_count=(
            None if data.get("maximum_barrel_count") is None else _integer(data, "maximum_barrel_count")
        ),
        maximum_total_structure_width=_optional_number(data, "maximum_total_structure_width_m"),
        require_resolved_result=resolved,
    )


def _parse_project(raw: object) -> CulvertProject:
    data = _mapping(raw, "project")
    _reject_unknown(
        data,
        {
            "schema_version",
            "name",
            "crossings",
            "scenarios",
            "alternatives",
            "design_criteria",
            "source",
            "notes",
        },
        "project",
    )
    schema_version = _integer(data, "schema_version")
    if schema_version != SCHEMA_VERSION:
        msg = f"Unsupported culvert project schema_version {schema_version!r}; supported version: {SCHEMA_VERSION}."
        raise ValueError(msg)
    alternatives: list[Alternative] = []
    for value in _list(data.get("alternatives", []), "alternatives"):
        item = _mapping(value, "alternative")
        _reject_unknown(item, {"name", "crossing", "source", "notes"}, "alternative")
        name = _text(item, "name")
        source, notes = _metadata(item)
        alternatives.append(
            Alternative(
                name=name,
                crossing=_parse_crossing(item.get("crossing")),
                source=source,
                notes=notes,
            )
        )
    name = _text(data, "name")
    source, notes = _metadata(data)
    return CulvertProject(
        name=name,
        crossings=tuple(_parse_crossing(value) for value in _list(data.get("crossings"), "crossings")),
        scenarios=tuple(_parse_scenario(value) for value in _list(data.get("scenarios"), "scenarios")),
        alternatives=tuple(alternatives),
        design_criteria=(
            None if data.get("design_criteria") is None else _parse_design_criteria(data["design_criteria"])
        ),
        schema_version=schema_version,
        source=source,
        notes=notes,
    )


def load_project(path: Path) -> CulvertProject:
    """Load a strict schema-versioned JSON or TOML culvert project."""
    source = Path(path)
    if source.suffix.lower() == ".json":
        payload = cast(object, json.loads(source.read_text(encoding="utf-8")))
    elif source.suffix.lower() == ".toml":
        with source.open("rb") as stream:
            payload = cast(object, tomllib.load(stream))
    else:
        msg = "Culvert project files must use a .json or .toml extension."
        raise ValueError(msg)
    return _parse_project(payload)


def load_project_json(path: Path) -> CulvertProject:
    """Load a strict schema-versioned JSON culvert project."""
    if Path(path).suffix.lower() != ".json":
        msg = "load_project_json requires a .json file."
        raise ValueError(msg)
    return load_project(path)


def _tailwater_record(tailwater: object) -> dict[str, object]:
    if isinstance(tailwater, TailwaterCondition):
        return {"type": "fixed", "elevation_m": tailwater.elevation}
    if isinstance(tailwater, ManningChannelTailwater):
        section = tailwater.section
        if isinstance(section, RectangularChannel):
            section_record: dict[str, object] = {
                "shape": "rectangular",
                "bottom_width_m": section.bottom_width,
            }
        elif isinstance(section, TrapezoidalChannel):
            section_record = {
                "shape": "trapezoidal",
                "bottom_width_m": section.bottom_width,
                "left_side_slope_h_to_v": section.left_side_slope,
                "right_side_slope_h_to_v": section.right_side_slope,
            }
        else:
            msg = f"Unsupported tailwater section type: {type(section).__name__}."
            raise TypeError(msg)
        return {
            "type": "manning_channel",
            "channel_invert_elevation_m": tailwater.channel_invert_elevation,
            "roughness_manning_n": tailwater.roughness,
            "friction_slope": tailwater.friction_slope,
            "section": section_record,
        }
    if isinstance(tailwater, bool) or not isinstance(tailwater, (float, int)):
        msg = f"Unsupported tailwater type for serialization: {type(tailwater).__name__}."
        raise TypeError(msg)
    return {"type": "fixed", "elevation_m": float(tailwater)}


def _crossing_record(crossing: CrossingDefinition) -> dict[str, object]:
    groups: list[dict[str, object]] = []
    for group in crossing.groups:
        barrel = group.barrel
        record: dict[str, object] = {
            "material": barrel.material.value,
            "length_m": barrel.length,
            "inlet_invert_elevation_m": barrel.inlet_invert,
            "outlet_invert_elevation_m": barrel.outlet_invert,
            "roughness_manning_n": barrel.roughness,
        }
        if barrel.label:
            record["label"] = barrel.label
        if isinstance(barrel, CircularBarrelDefinition):
            record.update(shape="circular", diameter_mm=barrel.diameter_mm)
        else:
            record.update(shape="rectangular", span_mm=barrel.span_mm, rise_mm=barrel.rise_mm)
        groups.append({"name": group.name, "quantity": group.quantity, "barrel": record})
    result: dict[str, object] = {"name": crossing.name, "groups": groups}
    if crossing.roadway is not None:
        roadway = crossing.roadway
        result["roadway"] = {
            "crest_elevation_m": roadway.crest_elevation,
            "crest_length_m": roadway.crest_length,
            "discharge_coefficient": roadway.discharge_coefficient,
            "label": roadway.label,
        }
    if crossing.source is not None:
        result["source"] = crossing.source
    if crossing.notes:
        result["notes"] = crossing.notes
    return result


def _scenario_record(scenario: Scenario) -> dict[str, object]:
    item: dict[str, object] = {
        "name": scenario.name,
        "discharge_m3s": scenario.discharge,
        "tailwater": _tailwater_record(scenario.tailwater),
    }
    if scenario.aep_percent is not None:
        item["aep_percent"] = scenario.aep_percent
    if scenario.source is not None:
        item["source"] = scenario.source
    if scenario.notes:
        item["notes"] = scenario.notes
    return item


def _alternative_record(alternative: Alternative) -> dict[str, object]:
    item: dict[str, object] = {
        "name": alternative.name,
        "crossing": _crossing_record(alternative.crossing),
    }
    if alternative.source is not None:
        item["source"] = alternative.source
    if alternative.notes:
        item["notes"] = alternative.notes
    return item


def _criteria_record(criteria: DesignCriteria) -> dict[str, object]:
    return {
        "maximum_headwater_elevation_m": criteria.maximum_headwater_elevation,
        "maximum_headwater_depth_m": criteria.maximum_headwater_depth,
        "maximum_headwater_ratio": criteria.maximum_headwater_ratio,
        "minimum_freeboard_m": criteria.minimum_freeboard,
        "maximum_outlet_velocity_ms": criteria.maximum_outlet_velocity,
        "maximum_roadway_discharge_m3s": criteria.maximum_roadway_discharge,
        "maximum_barrel_count": criteria.maximum_barrel_count,
        "maximum_total_structure_width_m": criteria.maximum_total_structure_width,
        "require_resolved_result": criteria.require_resolved_result,
    }


def project_record(project: CulvertProject) -> dict[str, object]:
    """Return the canonical deterministic interchange representation."""
    result: dict[str, object] = {
        "schema_version": project.schema_version,
        "name": project.name,
        "crossings": [_crossing_record(crossing) for crossing in project.crossings],
        "scenarios": [_scenario_record(scenario) for scenario in project.scenarios],
        "alternatives": [_alternative_record(alternative) for alternative in project.alternatives],
    }
    if project.design_criteria is not None:
        result["design_criteria"] = _criteria_record(project.design_criteria)
    if project.source is not None:
        result["source"] = project.source
    if project.notes:
        result["notes"] = project.notes
    return result


def export_project_json(project: CulvertProject, path: Path) -> Path:
    """Write canonical JSON with stable field and collection ordering."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(project_record(project), indent=2) + "\n", encoding="utf-8")
    return target
