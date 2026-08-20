"""Focused checks for the promoted AEP/ARI conversion utility."""

from __future__ import annotations

import math

import pytest

from aep_ari_conversions import aep_1_in_x_from_ari, aep_from_ari, ari_from_aep


def test_aep_and_ari_round_trip() -> None:
    aep = aep_from_ari(100.0)

    assert ari_from_aep(aep) == pytest.approx(100.0)
    assert aep_1_in_x_from_ari(100.0) == pytest.approx(1.0 / (aep / 100.0))


@pytest.mark.parametrize("invalid", [0.0, -1.0, math.inf, math.nan])
def test_aep_from_ari_rejects_invalid_intervals(invalid: float) -> None:
    with pytest.raises(ValueError):
        aep_from_ari(invalid)


@pytest.mark.parametrize("invalid", [0.0, 100.0, -1.0, math.inf, math.nan])
def test_ari_from_aep_rejects_invalid_percentages(invalid: float) -> None:
    with pytest.raises(ValueError):
        ari_from_aep(invalid)
