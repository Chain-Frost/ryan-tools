"""Assess hydraulic results against explicit design criteria."""

from culvert_solver import HydraulicResultStatus

from ...classes.culvert.criteria import DesignCriteria
from ...classes.culvert.crossing import CircularBarrelDefinition, CrossingDefinition
from ...classes.culvert.results import ScenarioResult


def _crossing_geometry_failures(
    crossing: CrossingDefinition,
    criteria: DesignCriteria,
    scenario_name: str,
) -> list[str]:
    failures: list[str] = []
    barrel_count = sum(group.quantity for group in crossing.groups)
    structure_width = sum(
        group.quantity
        * (group.barrel.diameter_mm if isinstance(group.barrel, CircularBarrelDefinition) else group.barrel.span_mm)
        / 1000.0
        for group in crossing.groups
    )
    if criteria.maximum_barrel_count is not None and barrel_count > criteria.maximum_barrel_count:
        failures.append(f"{scenario_name}: barrel count {barrel_count} exceeds {criteria.maximum_barrel_count}")
    if criteria.maximum_total_structure_width is not None and structure_width > criteria.maximum_total_structure_width:
        failures.append(
            f"{scenario_name}: nominal structure width {structure_width:.3f} m exceeds "
            f"{criteria.maximum_total_structure_width:.3f} m"
        )
    return failures


def _freeboard_failure(
    result: ScenarioResult,
    crossing: CrossingDefinition,
    minimum_freeboard: float | None,
) -> str | None:
    if minimum_freeboard is None:
        return None
    if crossing.roadway is None:
        return f"{result.scenario_name}: minimum freeboard criterion requires a roadway crest"
    freeboard = crossing.roadway.crest_elevation - result.hydraulic_result.headwater_elevation
    if freeboard >= minimum_freeboard:
        return None
    return f"{result.scenario_name}: freeboard {freeboard:.3f} m is less than {minimum_freeboard:.3f} m"


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
    headwater_ratio = max(
        (
            (hydraulic.headwater_elevation - group.barrel.inlet_invert)
            / (
                group.barrel.diameter_mm / 1000.0
                if isinstance(group.barrel, CircularBarrelDefinition)
                else group.barrel.rise_mm / 1000.0
            )
            for group in crossing.groups
        ),
        default=0.0,
    )
    failures.extend(_crossing_geometry_failures(crossing, criteria, result.scenario_name))

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
    if criteria.maximum_headwater_ratio is not None and headwater_ratio > criteria.maximum_headwater_ratio:
        failures.append(
            f"{result.scenario_name}: HW/D {headwater_ratio:.3f} exceeds {criteria.maximum_headwater_ratio:.3f}"
        )
    if freeboard_failure := _freeboard_failure(result, crossing, criteria.minimum_freeboard):
        failures.append(freeboard_failure)
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
