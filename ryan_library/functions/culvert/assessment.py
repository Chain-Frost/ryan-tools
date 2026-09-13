"""Assess hydraulic results against explicit design criteria."""

from culvert_solver import HydraulicResultStatus

from ...classes.culvert.criteria import DesignCriteria
from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.results import ScenarioResult


def assess_scenario(
    result: ScenarioResult,
    crossing: CrossingDefinition,
    criteria: DesignCriteria,
) -> tuple[str, ...]:
    """Return governing failure reasons for one scenario, or an empty tuple when it passes."""
    failures: list[str] = []
    hydraulic = result.hydraulic_result
    minimum_inlet = min(group.barrel.inlet_invert for group in crossing.groups)
    headwater_depth = hydraulic.headwater_elevation - minimum_inlet

    if criteria.require_resolved_result and hydraulic.status is HydraulicResultStatus.UNRESOLVED:
        failures.append(f"{result.scenario_name}: hydraulic result is unresolved")
    if (
        criteria.maximum_headwater_elevation is not None
        and hydraulic.headwater_elevation > criteria.maximum_headwater_elevation
    ):
        failures.append(
            f"{result.scenario_name}: headwater elevation {hydraulic.headwater_elevation:.3f} m exceeds "
            f"{criteria.maximum_headwater_elevation:.3f} m"
        )
    if criteria.maximum_headwater_depth is not None and headwater_depth > criteria.maximum_headwater_depth:
        failures.append(
            f"{result.scenario_name}: headwater depth {headwater_depth:.3f} m exceeds "
            f"{criteria.maximum_headwater_depth:.3f} m"
        )
    if (
        criteria.maximum_outlet_velocity is not None
        and result.maximum_outlet_velocity > criteria.maximum_outlet_velocity
    ):
        failures.append(
            f"{result.scenario_name}: outlet velocity {result.maximum_outlet_velocity:.3f} m/s exceeds "
            f"{criteria.maximum_outlet_velocity:.3f} m/s"
        )
    if (
        criteria.maximum_roadway_discharge is not None
        and hydraulic.roadway_discharge > criteria.maximum_roadway_discharge
    ):
        failures.append(
            f"{result.scenario_name}: roadway discharge {hydraulic.roadway_discharge:.3f} m3/s exceeds "
            f"{criteria.maximum_roadway_discharge:.3f} m3/s"
        )
    return tuple(failures)
