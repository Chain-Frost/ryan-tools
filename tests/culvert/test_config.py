"""Tests for simple fixed-tailwater project JSON configuration."""

import json
from pathlib import Path

from ryan_library.classes.culvert import CircularBarrelDefinition, RectangularBarrelDefinition
from ryan_library.functions.culvert.config import load_project_json


def test_load_project_json(tmp_path: Path) -> None:
    path = tmp_path / "culvert_project.json"
    path.write_text(
        json.dumps(
            {
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
                                    "length": 40,
                                    "inlet_invert": 10.0,
                                    "outlet_invert": 9.5,
                                    "roughness": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                    }
                ],
                "scenarios": [{"name": "Design", "discharge": 4.0, "tailwater_elevation": 10.0}],
            }
        ),
        encoding="utf-8",
    )

    project = load_project_json(path)

    assert project.name == "Demo"
    assert project.crossings[0].groups[0].quantity == 2
    assert isinstance(project.crossings[0].groups[0].barrel, CircularBarrelDefinition)
    assert project.scenarios[0].tailwater == 10.0


def test_load_project_json_supports_roadway_and_rectangular_alternative(tmp_path: Path) -> None:
    path = tmp_path / "culvert_project.json"
    path.write_text(
        json.dumps(
            {
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
                                    "length": 30,
                                    "inlet_invert": 10,
                                    "outlet_invert": 9.5,
                                    "roughness": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                        "roadway": {
                            "crest_elevation": 12,
                            "crest_length": 20,
                            "discharge_coefficient": 1.7,
                        },
                    }
                ],
                "scenarios": [{"name": "Design", "discharge": 4, "tailwater_elevation": 10}],
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
                                        "length": 30,
                                        "inlet_invert": 10,
                                        "outlet_invert": 9.5,
                                        "roughness": 0.013,
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
