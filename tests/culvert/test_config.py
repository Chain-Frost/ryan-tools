"""Tests for simple fixed-tailwater project JSON configuration."""

import json
from pathlib import Path

import pytest
from culvert_solver import ManningChannelTailwater, TailwaterCondition

from ryan_library.classes.culvert import CircularBarrelDefinition, RectangularBarrelDefinition
from ryan_library.functions.culvert.config import export_project_json, load_project, load_project_json


def test_load_project_json(tmp_path: Path) -> None:
    path = tmp_path / "culvert_project.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "Demo",
                "crossings": [
                    {
                        "name": "Crossing A",
                        "groups": [
                            {
                                "name": "Pipes",
                                "quantity": 2,
                                "barrel": {
                                    "shape": "circular",
                                    "diameter_mm": 1200,
                                    "length_m": 40,
                                    "inlet_invert_elevation_m": 10.0,
                                    "outlet_invert_elevation_m": 9.5,
                                    "roughness_manning_n": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                    }
                ],
                "scenarios": [
                    {
                        "name": "Design",
                        "discharge_m3s": 4.0,
                        "tailwater": {"type": "fixed", "elevation_m": 10.0},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    project = load_project_json(path)

    assert project.name == "Demo"
    assert project.crossings[0].groups[0].quantity == 2
    assert isinstance(project.crossings[0].groups[0].barrel, CircularBarrelDefinition)
    assert project.scenarios[0].tailwater == TailwaterCondition(10.0)


def test_load_project_json_supports_roadway_and_rectangular_alternative(tmp_path: Path) -> None:
    path = tmp_path / "culvert_project.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "Demo",
                "crossings": [
                    {
                        "name": "Existing",
                        "groups": [
                            {
                                "name": "Pipe",
                                "barrel": {
                                    "shape": "circular",
                                    "diameter_mm": 900,
                                    "length_m": 30,
                                    "inlet_invert_elevation_m": 10,
                                    "outlet_invert_elevation_m": 9.5,
                                    "roughness_manning_n": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                        "roadway": {
                            "crest_elevation_m": 12,
                            "crest_length_m": 20,
                            "discharge_coefficient": 1.7,
                        },
                    }
                ],
                "scenarios": [
                    {
                        "name": "Design",
                        "discharge_m3s": 4,
                        "tailwater": {"type": "fixed", "elevation_m": 10},
                    }
                ],
                "alternatives": [
                    {
                        "name": "Box upgrade",
                        "crossing": {
                            "name": "Proposed",
                            "groups": [
                                {
                                    "name": "Box",
                                    "quantity": 2,
                                    "barrel": {
                                        "shape": "rectangular",
                                        "span_mm": 1800,
                                        "rise_mm": 1200,
                                        "length_m": 30,
                                        "inlet_invert_elevation_m": 10,
                                        "outlet_invert_elevation_m": 9.5,
                                        "roughness_manning_n": 0.013,
                                        "material": "concrete_box",
                                    },
                                }
                            ],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    project = load_project_json(path)

    assert project.crossings[0].roadway is not None
    assert project.crossings[0].roadway.crest_elevation == 12.0
    assert isinstance(project.alternatives[0].crossing.groups[0].barrel, RectangularBarrelDefinition)


def test_load_project_toml_supports_manning_tailwater_and_metadata(tmp_path: Path) -> None:
    path = tmp_path / "culvert_project.toml"
    path.write_text(
        """
schema_version = 1
name = "Creek crossing"
source = "Design brief"
notes = "Concept design"

[[crossings]]
name = "Existing"
[[crossings.groups]]
name = "Pipes"
quantity = 2
[crossings.groups.barrel]
shape = "circular"
diameter_mm = 1200
length_m = 40
inlet_invert_elevation_m = 10.0
outlet_invert_elevation_m = 9.5
roughness_manning_n = 0.013
material = "concrete_pipe"

[[scenarios]]
name = "1% AEP"
aep_percent = 1.0
discharge_m3s = 4.0
source = "External hydrology"
[scenarios.tailwater]
type = "manning_channel"
channel_invert_elevation_m = 9.2
roughness_manning_n = 0.035
friction_slope = 0.002
[scenarios.tailwater.section]
shape = "trapezoidal"
bottom_width_m = 3.0
left_side_slope_h_to_v = 2.0
right_side_slope_h_to_v = 2.0
""".strip(),
        encoding="utf-8",
    )

    project = load_project(path)

    assert project.schema_version == 1
    assert project.source == "Design brief"
    assert project.scenarios[0].aep_percent == 1.0
    assert isinstance(project.scenarios[0].tailwater, ManningChannelTailwater)

    exported = export_project_json(project, tmp_path / "export.json")
    assert load_project_json(exported) == project


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"schema_version": 2}, "Unsupported culvert project schema_version"),
        ({"unexpected": True}, "unknown field"),
    ],
)
def test_load_project_rejects_version_and_unknown_fields(
    tmp_path: Path,
    change: dict[str, object],
    message: str,
) -> None:
    payload: dict[str, object] = {
        "schema_version": 1,
        "name": "Demo",
        "crossings": [],
        "scenarios": [],
    }
    payload.update(change)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_project_json(path)
