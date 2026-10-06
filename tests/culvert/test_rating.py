"""Focused tests for the crossing rating-curve workflow."""

import json
from pathlib import Path

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
)
from ryan_library.functions.culvert.export import export_crossing_rating_csv, export_crossing_rating_json
from ryan_library.orchestrators.culvert.rating import generate_crossing_rating


def test_rating_curve_delegates_to_solver() -> None:
    crossing = CrossingDefinition(
        name="Crossing A",
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                quantity=2,
                barrel=CircularBarrelDefinition(
                    diameter_mm=1200.0,
                    length=40.0,
                    inlet_invert=10.0,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )

    result = generate_crossing_rating(crossing, (1.0, 2.0, 3.0), 9.5)

    assert result.crossing_name == "Crossing A"
    assert tuple(point.discharge for point in result.rating_curve.points) == (1.0, 2.0, 3.0)
    assert all(point.headwater_elevation > 9.5 for point in result.rating_curve.points)


def test_rating_exports_retain_hydraulic_details(tmp_path: Path) -> None:
    crossing = CrossingDefinition(
        name="Crossing A",
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                quantity=2,
                barrel=CircularBarrelDefinition(
                    diameter_mm=1200.0,
                    length=40.0,
                    inlet_invert=10.0,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )
    result = generate_crossing_rating(crossing, (1.0, 20.0), 9.5)

    json_path = export_crossing_rating_json(result, tmp_path / "rating.json")
    csv_path = export_crossing_rating_csv(result, tmp_path / "rating.csv")
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    assert payload[0]["control_type"]
    assert payload[0]["flow_regime"]
    assert payload[0]["tailwater_resolution"]["method"] == "fixed_elevation"
    assert "warning_codes" in csv_path.read_text(encoding="utf-8").splitlines()[0]
