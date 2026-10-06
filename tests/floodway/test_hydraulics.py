"""Focused tests for the initial floodway hydraulic-demand primitives."""

import pytest

from ryan_library.classes.floodway import FloodwayApplicabilityStatus, FloodwayZone
from ryan_library.functions.floodway import (
    build_zone_demand,
    calculate_mrwa_submerged_pavement_velocity,
    calculate_mrwa_surface_velocity,
    dynamic_pressure,
    momentum_flux_per_width,
    mrwa_appendix_submergence_reached,
    mrwa_figure_4_6_k,
    mrwa_simplified_free_flow_applicable,
    mrwa_specific_energy,
    mrwa_steady_state_velocity,
    mrwa_transition_submergence_ratio,
    rectangular_critical_depth,
)


def test_mrwa_equation_4_and_6_are_recomputed_directly() -> None:
    unit_discharge = 2.5
    slope = 0.05
    roughness = 0.03

    velocity = mrwa_steady_state_velocity(unit_discharge, slope, roughness)
    expected_velocity = ((1.0 / roughness) * unit_discharge ** (2.0 / 3.0) * slope**0.5) ** (3.0 / 5.0)

    assert velocity == pytest.approx(expected_velocity)
    assert mrwa_specific_energy(unit_discharge, velocity) == pytest.approx(
        velocity**2 / (2.0 * 9.80665) + unit_discharge / velocity
    )


@pytest.mark.parametrize(
    ("delta_p_over_head", "graph_k"),
    (
        (0.104, 3.50),
        (0.150, 3.70),
        (0.307, 4.20),
        (0.318, 4.25),
    ),
)
def test_figure_4_6_reconstruction_matches_published_worked_example_graph_reads(
    delta_p_over_head: float,
    graph_k: float,
) -> None:
    assert mrwa_figure_4_6_k(delta_p_over_head) == pytest.approx(graph_k, abs=0.03)


def test_figure_4_6_reconstruction_rejects_extrapolation() -> None:
    with pytest.raises(ValueError, match="outside the MRWA Figure 4.6 domain"):
        mrwa_figure_4_6_k(1.81)


def test_figure_4_5_digitisation_matches_worked_example_transition_anchors() -> None:
    assert mrwa_transition_submergence_ratio(0.10) == pytest.approx(0.60)
    assert mrwa_transition_submergence_ratio(1.48 / 9.0) == pytest.approx(0.673, abs=0.01)


def test_figure_4_5_digitisation_rejects_extrapolation() -> None:
    with pytest.raises(ValueError, match="outside the digitised MRWA Figure 4.5 domain"):
        mrwa_transition_submergence_ratio(0.21)


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


def test_mrwa_velocity_can_reconstruct_figure_4_6_from_head_drop() -> None:
    result = calculate_mrwa_surface_velocity(
        zone=FloodwayZone.DOWNSTREAM_BATTER,
        unit_discharge=1.443,
        slope=1.0 / 3.0,
        roughness=0.04,
        total_head=0.90,
        delta_p=0.135,
    )

    assert result.coefficient_k == pytest.approx(3.70, abs=0.03)
    assert result.maximum_attainable_velocity == pytest.approx(3.51, abs=0.03)
    assert result.adopted_velocity == pytest.approx(3.51, abs=0.03)
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
    assert demand.depth_m == pytest.approx(2.0 / demand.velocity)
    assert demand.froude_number is not None
    assert demand.velocity_head_m == pytest.approx(demand.velocity**2 / (2.0 * 9.80665))
    assert demand.dynamic_pressure_pa != demand.momentum_flux_per_width_npm


def test_rectangular_critical_depth_is_diagnostic_primitive() -> None:
    assert rectangular_critical_depth(0.0) == 0.0
    assert rectangular_critical_depth(2.0) == pytest.approx((4.0 / 9.80665) ** (1.0 / 3.0))



def test_mrwa_submerged_pavement_velocity_uses_q_over_downstream_depth() -> None:
    result = calculate_mrwa_submerged_pavement_velocity(
        unit_discharge=1.2,
        downstream_depth=0.4,
    )

    assert result.velocity == pytest.approx(3.0)
    assert result.downstream_depth == pytest.approx(0.4)
    assert result.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION



def test_mrwa_source_thresholds_remain_distinct() -> None:
    assert mrwa_simplified_free_flow_applicable(0.759)
    assert not mrwa_simplified_free_flow_applicable(0.76)
    assert not mrwa_appendix_submergence_reached(0.799)
    assert mrwa_appendix_submergence_reached(0.8)
