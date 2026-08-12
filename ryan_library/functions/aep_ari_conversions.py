"""
Mathematical conversions between Annual Exceedance Probability (AEP) and Average Recurrence Interval (ARI).
"""

from __future__ import annotations
import math


def aep_from_ari(ari: float) -> float:
    """
    Convert Average Recurrence Interval (ARI) in years to Annual Exceedance Probability (AEP) percentage.
    """
    aep_fraction: float = (math.exp(1.0 / ari) - 1.0) / math.exp(1.0 / ari)
    return aep_fraction * 100.0


def ari_from_aep(aep_percentage: float) -> float:
    """
    Convert Annual Exceedance Probability (AEP) percentage to Average Recurrence Interval (ARI) in years.
    """
    return 1.0 / (-math.log(1.0 - (aep_percentage / 100.0)))


def aep_1_in_x_from_ari(ari: float) -> float:
    """
    Convert Average Recurrence Interval (ARI) in years to 1-in-X AEP format.
    """
    ey: float = 1.0 / ari
    return math.exp(ey) / (math.exp(ey) - 1.0)
