"""MRWA legacy scour-protection selection for floodway batter slopes."""

from math import isfinite

from ...classes.floodway.protection import MrwaRockSlopeProtectionResult
from ...classes.floodway.models import FloodwayApplicabilityStatus


def select_mrwa_rock_slope_protection(velocity_ms: float) -> MrwaRockSlopeProtectionResult:
    """Return the MRWA 2006 Table 5.1 dumped-rock class for batter velocity.

    The table is reproduced as discrete source bands. At shared band boundaries,
    the higher protection class is selected conservatively. Velocities above
    6.4 m/s are returned as a source-defined special-design condition rather than
    extrapolating the table.
    """
    velocity = float(velocity_ms)
    if not isfinite(velocity) or velocity < 0.0:
        msg = "velocity_ms must be finite and non-negative."
        raise ValueError(msg)

    if velocity < 2.0:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="None",
            section_thickness_m=None,
            no_rock_required=True,
        )
    if velocity < 2.6:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="Facing",
            section_thickness_m=0.50,
        )
    if velocity < 2.9:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="Light",
            section_thickness_m=0.75,
        )
    if velocity < 3.9:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="1/4 tonne",
            section_thickness_m=1.00,
            nominal_class_tonnes=0.25,
        )
    if velocity < 4.5:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="1/2 tonne",
            section_thickness_m=1.25,
            nominal_class_tonnes=0.50,
        )
    if velocity < 5.1:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="1 tonne",
            section_thickness_m=1.60,
            nominal_class_tonnes=1.0,
        )
    if velocity < 5.7:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="2 tonne",
            section_thickness_m=2.00,
            nominal_class_tonnes=2.0,
        )
    if velocity <= 6.4:
        return MrwaRockSlopeProtectionResult(
            velocity_ms=velocity,
            rock_class="4 tonne",
            section_thickness_m=2.50,
            nominal_class_tonnes=4.0,
        )
    return MrwaRockSlopeProtectionResult(
        velocity_ms=velocity,
        rock_class="Special",
        section_thickness_m=None,
        requires_special_design=True,
        applicability=FloodwayApplicabilityStatus.SPECIALIST_REVIEW_REQUIRED,
    )
