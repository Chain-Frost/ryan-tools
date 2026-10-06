"""Plotting tests using real, already-computed solver results."""

from pathlib import Path

from matplotlib import pyplot as plt

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    Scenario,
)
from ryan_library.functions.culvert.plotting import plot_longitudinal_profile, plot_rating_curve, save_figure
from ryan_library.orchestrators.culvert.rating import generate_crossing_rating
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def _crossing() -> CrossingDefinition:
    return CrossingDefinition(
        name="Plot crossing",
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                quantity=2,
                barrel=CircularBarrelDefinition(
                    diameter_mm=1200,
                    length=40,
                    inlet_invert=10,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )


def test_plot_longitudinal_profile_from_computed_result(tmp_path: Path) -> None:
    result = solve_crossing_scenario(_crossing(), Scenario("Design", 2.0, 9.5))

    figure = plot_longitudinal_profile(result)
    target = save_figure(figure, tmp_path / "profile.png")

    assert target.stat().st_size > 0
    assert len(figure.axes[0].lines) >= 4
    plt.close(figure)


def test_plot_rating_curve_from_computed_result(tmp_path: Path) -> None:
    result = generate_crossing_rating(_crossing(), (1.0, 2.0, 3.0), 9.5)

    figure = plot_rating_curve(result)
    target = save_figure(figure, tmp_path / "rating.png")

    assert target.stat().st_size > 0
    assert len(figure.axes) == 2
    plt.close(figure)
