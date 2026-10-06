"""Strict versioned floodway formation configuration parsing and serialization."""

import json
import tomllib
from pathlib import Path
from typing import cast

from ...classes.floodway import (
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayZone,
    Hec23RiprapDesignInput,
)

FLOODWAY_FORMATION_SCHEMA_VERSION = 1


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"{name} must be an object/table."
        raise ValueError(msg)
    raw = cast("dict[object, object]", value)
    if not all(isinstance(key, str) for key in raw):
        msg = f"{name} must contain only string keys."
        raise ValueError(msg)
    return cast("dict[str, object]", raw)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        msg = f"{name} must be an array."
        raise ValueError(msg)
    return cast("list[object]", value)


def _reject_unknown(data: dict[str, object], allowed: set[str], name: str) -> None:
    if unknown := sorted(data.keys() - allowed):
        msg = f"{name} contains unknown field(s): {', '.join(unknown)}."
        raise ValueError(msg)


def _text(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        msg = f"{key} must be nonempty text."
        raise ValueError(msg)
    return value.strip()


def _number(data: dict[str, object], key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        msg = f"{key} must be numeric."
        raise ValueError(msg)
    return float(value)


def _optional_number(data: dict[str, object], key: str) -> float | None:
    return None if data.get(key) is None else _number(data, key)


def _integer(data: dict[str, object], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{key} must be an integer."
        raise ValueError(msg)
    return value


def _parse_hec23(value: object) -> Hec23RiprapDesignInput:
    data = _mapping(value, "hec23_riprap")
    _reject_unknown(
        data,
        {
            "selected_d50_m",
            "uniformity_coefficient",
            "porosity",
            "specific_gravity",
            "angle_of_repose_degrees",
        },
        "hec23_riprap",
    )
    return Hec23RiprapDesignInput(
        selected_d50_m=_number(data, "selected_d50_m"),
        uniformity_coefficient=_number(data, "uniformity_coefficient"),
        porosity=_number(data, "porosity"),
        specific_gravity=2.65 if data.get("specific_gravity") is None else _number(data, "specific_gravity"),
        angle_of_repose_degrees=(
            42.0 if data.get("angle_of_repose_degrees") is None else _number(data, "angle_of_repose_degrees")
        ),
    )


def _parse_zone(value: object, index: int) -> FloodwayFormationZone:
    data = _mapping(value, f"zones[{index}]")
    _reject_unknown(
        data,
        {
            "zone",
            "slope",
            "roughness_manning_n",
            "elevation_m",
            "hec23_riprap",
            "label",
        },
        f"zones[{index}]",
    )
    label = data.get("label", "")
    if not isinstance(label, str):
        msg = f"zones[{index}].label must be text."
        raise ValueError(msg)
    try:
        zone = FloodwayZone(_text(data, "zone").upper())
    except ValueError as exc:
        msg = f"zones[{index}].zone must be one of A, B, C, D, E or F."
        raise ValueError(msg) from exc

    hec23_value = data.get("hec23_riprap")
    return FloodwayFormationZone(
        zone=zone,
        slope=_optional_number(data, "slope"),
        roughness=_optional_number(data, "roughness_manning_n"),
        elevation=_optional_number(data, "elevation_m"),
        hec23_riprap=None if hec23_value is None else _parse_hec23(hec23_value),
        label=label,
    )


def _parse_formation(raw: object) -> FloodwayFormation:
    data = _mapping(raw, "floodway formation")
    _reject_unknown(
        data,
        {"schema_version", "name", "crest_flow_length_m", "two_d_verification_reason", "zones"},
        "floodway formation",
    )
    schema_version = _integer(data, "schema_version")
    if schema_version != FLOODWAY_FORMATION_SCHEMA_VERSION:
        msg = (
            f"Unsupported floodway formation schema_version {schema_version!r}; "
            f"supported version: {FLOODWAY_FORMATION_SCHEMA_VERSION}."
        )
        raise ValueError(msg)

    two_d_reason = data.get("two_d_verification_reason", "")
    if not isinstance(two_d_reason, str):
        msg = "two_d_verification_reason must be text."
        raise ValueError(msg)

    return FloodwayFormation(
        name=_text(data, "name"),
        crest_flow_length=_optional_number(data, "crest_flow_length_m"),
        two_d_verification_reason=two_d_reason,
        zones=tuple(_parse_zone(item, index) for index, item in enumerate(_list(data.get("zones"), "zones"))),
    )


def load_floodway_formation(path: Path) -> FloodwayFormation:
    """Load a strict schema-versioned JSON or TOML floodway formation."""
    source = Path(path)
    if source.suffix.lower() == ".json":
        payload = cast(object, json.loads(source.read_text(encoding="utf-8")))
    elif source.suffix.lower() == ".toml":
        with source.open("rb") as stream:
            payload = cast(object, tomllib.load(stream))
    else:
        msg = "Floodway formation files must use a .json or .toml extension."
        raise ValueError(msg)
    return _parse_formation(payload)


def load_floodway_formation_json(path: Path) -> FloodwayFormation:
    """Load a strict schema-versioned JSON floodway formation."""
    if Path(path).suffix.lower() != ".json":
        msg = "load_floodway_formation_json requires a .json file."
        raise ValueError(msg)
    return load_floodway_formation(path)


def _hec23_record(value: Hec23RiprapDesignInput) -> dict[str, object]:
    return {
        "selected_d50_m": value.selected_d50_m,
        "uniformity_coefficient": value.uniformity_coefficient,
        "porosity": value.porosity,
        "specific_gravity": value.specific_gravity,
        "angle_of_repose_degrees": value.angle_of_repose_degrees,
    }


def floodway_formation_record(formation: FloodwayFormation) -> dict[str, object]:
    """Return the deterministic floodway formation interchange representation."""
    return {
        "schema_version": FLOODWAY_FORMATION_SCHEMA_VERSION,
        "name": formation.name,
        "crest_flow_length_m": formation.crest_flow_length,
        "two_d_verification_reason": formation.two_d_verification_reason,
        "zones": [
            {
                "zone": zone.zone.value,
                "slope": zone.slope,
                "roughness_manning_n": zone.roughness,
                "elevation_m": zone.elevation,
                "hec23_riprap": None if zone.hec23_riprap is None else _hec23_record(zone.hec23_riprap),
                "label": zone.label,
            }
            for zone in formation.zones
        ],
    }


def export_floodway_formation_json(formation: FloodwayFormation, path: Path) -> Path:
    """Write canonical floodway formation JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(floodway_formation_record(formation), indent=2) + "\n", encoding="utf-8")
    return target
