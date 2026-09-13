"""Tests for simple fixed-tailwater project JSON configuration."""

import json

from ryan_library.classes.culvert import CircularBarrelDefinition
from ryan_library.functions.culvert.config import load_project_json


def test_load_project_json(tmp_path) -> None:
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
                "scenarios": [
                    {"name": "Design", "discharge": 4.0, "tailwater_elevation": 10.0}
                ],
            }
        ),
        encoding="utf-8",
    )

    project = load_project_json(path)

    assert project.name == "Demo"
    assert project.crossings[0].groups[0].quantity == 2
    assert isinstance(project.crossings[0].groups[0].barrel, CircularBarrelDefinition)
    assert project.scenarios[0].tailwater == 10.0
