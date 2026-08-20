"""Tests for culvert outlet rock-protection selection."""

import pytest

from ryan_library.functions.rock_protection import multi_pipe_outlet_rock_class, multi_pipe_outlet_rock_protection


@pytest.mark.parametrize(
    ("diameter_m", "velocity_m_per_s", "expected_class"),
    [
        (1.5, 2.61, "Facing"),  # Workbook X3.
        (1.8, 3.41, "1/4 Tonne"),  # Workbook X4.
        (2.1, 3.67, "1/4 Tonne"),  # Workbook X11.
        (2.4, 3.90, "1/2 Tonne"),  # Workbook X12.
    ],
)
def test_multi_pipe_outlet_rock_class_matches_workbook_examples(
    diameter_m: float,
    velocity_m_per_s: float,
    expected_class: str,
) -> None:
    assert multi_pipe_outlet_rock_class(diameter_m, velocity_m_per_s) == expected_class


def test_velocity_band_upper_bound_is_inclusive() -> None:
    assert multi_pipe_outlet_rock_class(0.9, 1.3) == "Facing"
    assert multi_pipe_outlet_rock_class(0.9, 1.300_001) == "Facing"
    assert multi_pipe_outlet_rock_protection(0.9, 2.9).nominal_d50_mm == 200
    assert multi_pipe_outlet_rock_protection(0.9, 2.900_001).nominal_d50_mm == 300


def test_recommendation_includes_related_workbook_values() -> None:
    recommendation = multi_pipe_outlet_rock_protection(2.4, 3.9)

    assert recommendation.rock_class == "1/2 Tonne"
    assert recommendation.nominal_d50_mm == 600
    assert recommendation.rock_thickness_m == 1.25
    assert recommendation.rounded_rock_d50_mm == 700
    assert recommendation.rock_d50_mm == 700
    assert recommendation.apron_length_diameters == 5


def test_corrected_1_2_m_high_velocity_band_is_available() -> None:
    assert multi_pipe_outlet_rock_class(1.2, 4.8) == "1/2 Tonne"


@pytest.mark.parametrize(
    ("diameter_m", "velocity_m_per_s"),
    [(1.0, 2.0), (0.9, 0.0), (0.9, 5.0), (float("nan"), 2.0), (1.5, float("inf"))],
)
def test_unavailable_or_non_finite_inputs_raise(diameter_m: float, velocity_m_per_s: float) -> None:
    with pytest.raises(ValueError):
        multi_pipe_outlet_rock_class(diameter_m, velocity_m_per_s)
