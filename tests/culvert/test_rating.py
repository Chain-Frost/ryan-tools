"""Focused tests for the crossing rating-curve workflow."""

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
)
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
