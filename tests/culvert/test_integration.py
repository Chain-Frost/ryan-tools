"""Focused integration tests for the ryan-tools/culvert_solver boundary."""

from typing import Any, cast

from culvert_solver import CircularGeometry, CulvertCrossing

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    Scenario,
)
from ryan_library.functions.culvert.adapter import build_solver_crossing
from ryan_library.functions.culvert.export import scenario_result_record
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def _crossing(*, diameter_mm: float = 1200.0, quantity: int = 3) -> CrossingDefinition:
    return CrossingDefinition(
        name="Crossing A",
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                quantity=quantity,
                barrel=CircularBarrelDefinition(
                    diameter_mm=diameter_mm,
                    length=40.0,
                    inlet_invert=10.0,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )


def test_adapter_builds_public_solver_crossing() -> None:
    solver_crossing = build_solver_crossing(_crossing())

    assert isinstance(solver_crossing, CulvertCrossing)
    assert solver_crossing.total_barrels == 3
    assert isinstance(solver_crossing.groups[0].barrel.geometry, CircularGeometry)
    assert solver_crossing.groups[0].barrel.geometry.diameter == 1.2


def test_forward_solve_retains_solver_result_and_notices() -> None:
    result = solve_crossing_scenario(
        _crossing(),
        Scenario(name="Design", discharge=6.0, tailwater=10.0),
    )

    assert result.hydraulic_result.total_discharge == 6.0
    assert result.hydraulic_result.headwater_elevation > 10.0
    assert result.hydraulic_result.group_results[0].barrel_discharge == 2.0
    assert "representative_barrel_equal_flow" in result.applicability_codes


def test_machine_record_retains_warning_messages_sources_and_solver_detail() -> None:
    result = solve_crossing_scenario(
        _crossing(),
        Scenario(name="Extreme", discharge=50.0, tailwater=10.0),
    )

    record = scenario_result_record(result)
    warnings = cast("list[dict[str, Any]]", record["warnings"])
    notices = cast("list[dict[str, Any]]", record["applicability_notices"])
    groups = cast("list[dict[str, Any]]", record["group_results"])
    group = groups[0]

    assert warnings[0]["message"]
    assert notices[0]["source"]["source_id"]
    assert group["critical_depth_m"] > 0.0
    assert "normal_depth_m" in group
    assert "adopted_roughness_manning_n" in group
    assert "inlet_coefficient_selection" in group
    assert "entrance_loss_selection" in group
    assert "exit_loss_selection" in group
    assert "outlet_control_losses" in group
    assert "convergence" in group
    assert "headwater_convergence" in record
