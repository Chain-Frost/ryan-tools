"""Regression tests for FHWA HEC-23 Volume II Design Guideline 5."""

import pytest

from ryan_library.functions.floodway import evaluate_hec23_overtopping_riprap, hec23_overtopping_riprap_d50

_COMMON = {
    "unit_discharge": 0.186,
    "uniformity_coefficient": 2.1,
    "porosity": 0.45,
    "specific_gravity": 2.65,
    "angle_of_repose_degrees": 42.0,
}


def test_equation_5_2_matches_mild_and_steep_source_vectors() -> None:
    mild = hec23_overtopping_riprap_d50(
        unit_discharge=0.186,
        slope=0.20,
        uniformity_coefficient=2.1,
        specific_gravity=2.65,
        angle_of_repose_degrees=42.0,
    )
    steep = hec23_overtopping_riprap_d50(
        unit_discharge=0.186,
        slope=0.50,
        uniformity_coefficient=2.1,
        specific_gravity=2.65,
        angle_of_repose_degrees=42.0,
    )

    assert mild == pytest.approx(0.09416, rel=2e-3)
    assert steep == pytest.approx(0.2890, rel=2e-3)


def test_mild_slope_example_uses_selected_class_for_capacity_checks() -> None:
    result = evaluate_hec23_overtopping_riprap(
        **_COMMON,
        slope=0.20,
        selected_d50_m=0.15,
    )

    assert result.minimum_d50_m == pytest.approx(0.09416, rel=2e-3)
    assert result.selected_d50_m == pytest.approx(0.15)
    assert result.interstitial_velocity_ms == pytest.approx(0.228, rel=0.01)
    assert result.average_interstitial_velocity_ms == pytest.approx(0.103, rel=0.01)
    assert result.all_flow_interstitial_depth_m == pytest.approx(1.81, rel=0.02)
    assert result.allowable_surface_depth_m == pytest.approx(0.069, rel=0.02)
    assert result.surface_unit_discharge_m2s == pytest.approx(0.172, rel=0.02)
    assert result.required_interstitial_unit_discharge_m2s == pytest.approx(0.0143, rel=0.03)
    assert result.two_d50_sufficient
    assert result.four_d50_sufficient
    assert result.recommended_thickness_m == pytest.approx(0.30)
    assert not result.requires_larger_gradation


def test_steep_slope_first_selected_class_requires_larger_gradation() -> None:
    result = evaluate_hec23_overtopping_riprap(
        **_COMMON,
        slope=0.50,
        selected_d50_m=0.3048,
    )

    assert result.minimum_d50_m == pytest.approx(0.289, rel=2e-3)
    assert result.interstitial_velocity_ms == pytest.approx(0.552, rel=0.02)
    assert result.average_interstitial_velocity_ms == pytest.approx(0.249, rel=0.02)
    assert result.all_flow_interstitial_depth_m == pytest.approx(0.748, rel=0.02)
    assert result.allowable_surface_depth_m is None
    assert not result.two_d50_sufficient
    assert result.four_d50_sufficient
    assert result.recommended_thickness_m is None
    assert result.requires_larger_gradation


def test_steep_slope_next_class_is_sufficient_at_two_d50() -> None:
    result = evaluate_hec23_overtopping_riprap(
        **_COMMON,
        slope=0.50,
        selected_d50_m=0.381,
    )

    assert result.interstitial_velocity_ms == pytest.approx(0.618, rel=0.02)
    assert result.average_interstitial_velocity_ms == pytest.approx(0.278, rel=0.02)
    assert result.all_flow_interstitial_depth_m == pytest.approx(0.669, rel=0.02)
    assert result.two_d50_sufficient
    assert result.four_d50_sufficient
    assert result.recommended_thickness_m == pytest.approx(0.762)
    assert not result.requires_larger_gradation


def test_selected_gradation_cannot_be_smaller_than_equation_5_2_minimum() -> None:
    with pytest.raises(ValueError, match="smaller than the HEC-23 Equation 5.2 minimum"):
        evaluate_hec23_overtopping_riprap(
            **_COMMON,
            slope=0.20,
            selected_d50_m=0.05,
        )
