"""Bounded numerical acceptance of a repository-only pure hydrology utility.

This test does not approve wheel promotion or establish engineering ARR parity.
"""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).absolute().parents[2]


def _load_legacy_conversions() -> ModuleType:
    """Import only the source-inspected pure-maths module, never bulk legacy scripts."""
    source = ROOT / "ryan-scripts" / "hydrology-python" / "aep_ari_conversions.py"
    spec = importlib.util.spec_from_file_location("repo_only_aep_ari", source)
    if spec is None or spec.loader is None:
        msg = f"Cannot load {source}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


conversions = _load_legacy_conversions()


@pytest.mark.parametrize("ari_years", [1.0, 2.0, 10.0, 100.0, 1000.0])
def test_aep_ari_round_trip_for_finite_positive_inputs(ari_years: float) -> None:
    aep = conversions.aep_from_ari(ari_years)
    assert 0.0 < aep < 100.0
    assert conversions.ari_from_aep(aep) == pytest.approx(ari_years, rel=1e-12)
    assert conversions.aep_1_in_x_from_ari(ari_years) == pytest.approx(100.0 / aep, rel=1e-12)


def test_one_year_ari_returns_poisson_annual_exceedance_probability() -> None:
    assert conversions.aep_from_ari(1.0) == pytest.approx(-math.expm1(-1.0) * 100.0)


@pytest.mark.parametrize("bad_ari", [0.0, -1.0, math.inf, -math.inf, math.nan])
def test_invalid_ari_inputs_fail_without_side_effects(bad_ari: float) -> None:
    with pytest.raises(ValueError, match="positive finite"):
        conversions.aep_from_ari(bad_ari)


@pytest.mark.parametrize("bad_aep", [-1.0, 0.0, 100.0, 101.0, math.inf, math.nan])
def test_invalid_aep_inputs_are_rejected(bad_aep: float) -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        conversions.ari_from_aep(bad_aep)
