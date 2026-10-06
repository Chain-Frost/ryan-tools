"""Regression tests for MRWA 2006 Table 5.1 rock slope protection."""

import pytest

from ryan_library.classes.floodway import FloodwayApplicabilityStatus
from ryan_library.functions.floodway import select_mrwa_rock_slope_protection


@pytest.mark.parametrize(
    ("velocity", "rock_class", "thickness"),
    [
        (1.99, "None", None),
        (2.00, "Facing", 0.50),
        (2.59, "Facing", 0.50),
        (2.60, "Light", 0.75),
        (2.82, "Light", 0.75),
        (2.90, "1/4 tonne", 1.00),
        (3.51, "1/4 tonne", 1.00),
        (3.90, "1/2 tonne", 1.25),
        (4.50, "1 tonne", 1.60),
        (5.10, "2 tonne", 2.00),
        (5.70, "4 tonne", 2.50),
        (6.40, "4 tonne", 2.50),
    ],
)
def test_table_5_1_velocity_bands(
    velocity: float,
    rock_class: str,
    thickness: float | None,
) -> None:
    result = select_mrwa_rock_slope_protection(velocity)

    assert result.rock_class == rock_class
    assert result.section_thickness_m == thickness
    assert result.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION


def test_table_5_1_above_source_range_requires_special_design() -> None:
    result = select_mrwa_rock_slope_protection(6.41)

    assert result.rock_class == "Special"
    assert result.section_thickness_m is None
    assert result.requires_special_design
    assert result.applicability is FloodwayApplicabilityStatus.SPECIALIST_REVIEW_REQUIRED
