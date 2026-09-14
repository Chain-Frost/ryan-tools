"""Focused tests for the initial floodway hydraulic-demand primitives."""

import pytest

from ryan_library.classes.floodway import FloodwayApplicabilityStatus, FloodwayZone
from ryan_library.functions.floodway import (
    build_zone_demand,
    calculate_mrwa_surface_velocity,
    dynamic_pressure,
    momentum_flux_per_width,
    mrwa_specific_energy,
    mrwa_steady_state_velocity,
    rectangular_critical_depth,
)


def test_mrwa_equation_4_and_6_are_recomputed_directly() -> None:
    unit_discharge = 2.5
    slope = 0.05
    roughness = 0.03

    velocity = mrwa_steady_state_velocity(unit_discharge, slope, roughness)
    expected_velocity = ((1.0 / roughness) * unit_discharge ** (2.0 / 3.0) * slope**0.5) ** (3.0 / 5.0)

    assert velocity == pytest.approx(expected_velocity)
    assert mrwa_specific_energy(unit_discharge, velocity) == pytest.approx(velocity**2 / (2.0 * 9.80665) + unit_discharge / velocity)


def test_mrwa_velocity_requires_source_data_before_equation_7_is_claimed() -> None:
    result = calculate_mrwa_surface_velocity(
        zone=FloodwayZone.DOWNSTREAM_BATTER,
        unit_discharge=3.0,
        slope=0.1,
        roughness=0.05,
    )

    assert result.maximum_attainable_velocity is None
    assert result.applicability is FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED
    assert result.adopted_velocity == result.steady_state_velocity


def test_mrwa_velocity_uses_lesser_of_steady_and_maximum_attainable() -> None:
    result = calculate_mrwa_surface_velocity(
        zone=FloodwayZone.DOWNSTREAM_BATTER,
        unit_discharge=3.0,
        slope=0.1,
        roughness=0.05,
        total_head=1.2,
        coefficient_k=3.5,
    )

    assert result.maximum_attainable_velocity == pytest.approx(3.5 * 1.2**0.5)
    assert result.adopted_velocity == min(result.steady_state_velocity, result.maximum_attainable_velocity)
    assert result.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION


def test_physical_demands_remain_separate_quantities() -> None:
    result = calculate_mrwa_surface_velocity(
        zone=FloodwayZone.PAVEMENT,
        unit_discharge=2.0,
        slope=0.02,
        roughness=0.016,
        total_head=1.0,
        coefficient_k=4.0,
    )
    demand = build_zone_demand(result)

    assert demand.dynamic_pressure_pa == pytest.approx(dynamic_pressure(demand.velocity))
    assert demand.momentum_flux_per_width_npm == pytest.approx(momentum_flux_per_width(2.0, demand.velocity))
    assert demand.dynamic_pressure_pa != demand.momentum_flux_per_width_npm


def test_rectangular_critical_depth_is_diagnostic_primitive() -> None:
    assert rectangular_critical_depth(0.0) == 0.0
    assert rectangular_critical_depth(2.0) == pytest.approx((4.0 / 9.80665) ** (1.0 / 3.0))
