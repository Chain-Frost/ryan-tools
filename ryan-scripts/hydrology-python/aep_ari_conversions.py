"""
Mathematical conversions between Annual Exceedance Probability (AEP) and Average Recurrence Interval (ARI).
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations
import math


def aep_from_ari(ari: float) -> float:
    """Convert a positive ARI in years to AEP percentage."""
    if not math.isfinite(ari) or ari <= 0:
        raise ValueError("ARI must be a positive finite number of years")
    return -math.expm1(-1.0 / ari) * 100.0


def ari_from_aep(aep_percentage: float) -> float:
    """Convert an AEP percentage greater than zero and less than 100 to ARI."""
    if not math.isfinite(aep_percentage) or not 0 < aep_percentage < 100:
        raise ValueError("AEP percentage must be finite and between 0 and 100")
    return 1.0 / -math.log1p(-(aep_percentage / 100.0))


def aep_1_in_x_from_ari(ari: float) -> float:
    """Convert a positive ARI in years to the equivalent one-in-X AEP."""
    aep_fraction = aep_from_ari(ari) / 100.0
    return 1.0 / aep_fraction
