"""Synthetic reference-year and climate-factor contracts; no solver parity claimed."""

from __future__ import annotations

from math import inf, nan
from types import SimpleNamespace

import pytest

from ryan_library.functions.hydrology.arr_user_climate_losses import user_loss_adjustments
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec, ScreeningStatus
from ryan_library.orchestrators.hydrology.rainfall_loss_workflow import assemble_arr_events


def _event(
    scenario: str | None,
    *,
    aep: str = "1%",
    duration: int = 60,
    rain: float = 40.0,
) -> SimpleNamespace:
    pattern = SimpleNamespace(
        event_id=1, tp_number=1, timestep=60, increments=[100], source="point", region="west"
    )
    return SimpleNamespace(
        aep_name=aep,
        duration=duration,
        cc_scenario=scenario,
        depth_point=rain,
        depth_areal=rain,
        arf=1.0,
        initial_loss=99.0,
        continuing_loss=0.0,
        patterns=[pattern],
        preburst=None,
    )


def test_historical_manual_loss_cc_applies_distinct_il_cl_and_preserves_baseline() -> None:
    events = [_event(None), _event("2050_SSP2")]
    calls: list[tuple[int, str]] = []

    def factor(year: int, ssp: str) -> tuple[float, float]:
        calls.append((year, ssp))
        return 0.8, 1.2

    base = LossSpec(50.0, continuing_loss_mm_per_hour=5.0)
    changes = user_loss_adjustments(
        events, default_losses=base, losses_by_aep={}, reference_year=1990,
        factor_provider=factor, complete_storm=True,
    )
    unchanged = changes["1%", 60, None]
    adjusted = changes["1%", 60, "2050_SSP2"]
    assert unchanged.effective is not base
    assert unchanged.effective.initial_loss_mm == 50.0
    assert unchanged.effective.continuing_loss_mm_per_hour == 5.0
    assert adjusted.effective.initial_loss_mm == pytest.approx(40)
    assert adjusted.effective.continuing_loss_mm_per_hour == pytest.approx(6)
    assert (adjusted.il_multiplier, adjusted.cl_multiplier) == pytest.approx((0.8, 1.2))
    assert adjusted.method == "full_storm"
    assert calls == [(2050, "SSP2")]
    assert base.initial_loss_mm == 50.0


def test_current_2030_loss_reference_uses_same_ssp_relative_ratios() -> None:
    factors = {
        (2030, "SSP2"): (0.9, 1.05),
        (2050, "SSP2"): (0.72, 1.26),
        (2030, "SSP5"): (0.95, 1.1),
        (2090, "SSP5"): (0.57, 1.65),
    }
    events = [_event(None), _event("2050_SSP2"), _event("2090_SSP5")]
    changes = user_loss_adjustments(
        events,
        default_losses=LossSpec(50, continuing_loss_mm_per_hour=5),
        losses_by_aep={},
        reference_year=2030,
        factor_provider=lambda year, ssp: factors[year, ssp],
        complete_storm=False,
    )
    assert changes["1%", 60, None].effective.initial_loss_mm == 50
    assert changes["1%", 60, "2050_SSP2"].effective.initial_loss_mm == pytest.approx(40)
    assert changes["1%", 60, "2050_SSP2"].effective.continuing_loss_mm_per_hour == pytest.approx(6)
    assert changes["1%", 60, "2090_SSP5"].effective.initial_loss_mm == pytest.approx(30)
    assert changes["1%", 60, "2090_SSP5"].effective.continuing_loss_mm_per_hour == pytest.approx(7.5)
    assert changes["1%", 60, "2090_SSP5"].method == "direct_burst"


def test_duration_overrides_do_not_overwrite_other_durations_or_aep() -> None:
    events = [_event("2050_SSP2", duration=60), _event("2050_SSP2", duration=120), _event("2050_SSP2", aep="2%")]
    selected = user_loss_adjustments(
        events,
        default_losses=LossSpec(80, continuing_loss_mm_per_hour=2),
        losses_by_aep={"1%": {60: LossSpec(30, continuing_loss_mm_per_hour=4)}},
        reference_year=1990,
        factor_provider=lambda _year, _ssp: (0.5, 1.5),
        complete_storm=True,
    )
    assert selected["1%", 60, "2050_SSP2"].effective.initial_loss_mm == pytest.approx(15)
    assert selected["1%", 60, "2050_SSP2"].effective.continuing_loss_mm_per_hour == pytest.approx(6)
    assert selected["1%", 120, "2050_SSP2"].effective.initial_loss_mm == pytest.approx(40)
    assert selected["2%", 60, "2050_SSP2"].effective.initial_loss_mm == pytest.approx(40)


@pytest.mark.parametrize("bad_factors", [(nan, 1.0), (0.0, 1.0), (-1.0, 2.0), (1.0, inf)])
def test_invalid_datahub_factors_fail_closed(bad_factors: tuple[float, float]) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        user_loss_adjustments(
            [_event("2050_SSP2")],
            default_losses=LossSpec(30, continuing_loss_mm_per_hour=5),
            losses_by_aep={}, reference_year=1990,
            factor_provider=lambda _year, _ssp: bad_factors, complete_storm=True,
        )


def test_missing_2030_denominator_and_unknown_scenario_fail_closed() -> None:
    with pytest.raises(ValueError, match="unavailable"):
        user_loss_adjustments(
            [_event("2050_SSP2")], default_losses=LossSpec(30, continuing_loss_mm_per_hour=5),
            losses_by_aep={}, reference_year=2030,
            factor_provider=lambda year, _ssp: (0.75, 1.25) if year == 2050 else _missing(),
            complete_storm=True,
        )
    with pytest.raises(ValueError, match="Unsupported ARR climate scenario"):
        user_loss_adjustments(
            [_event("2050_other")], default_losses=LossSpec(30, continuing_loss_mm_per_hour=5),
            losses_by_aep={}, reference_year=1990,
            factor_provider=lambda _year, _ssp: (1, 1), complete_storm=True,
        )


def _missing() -> tuple[float, float]:
    raise ValueError("2030 reference missing")


def test_runoff_coefficient_climate_case_fails_without_inventing_scaling() -> None:
    base = LossSpec(30, runoff_coefficient=0.4)
    only_baseline = user_loss_adjustments(
        [_event(None)], default_losses=base, losses_by_aep={}, reference_year=1990,
        factor_provider=lambda _year, _ssp: (1, 1), complete_storm=False,
    )
    assert only_baseline["1%", 60, None].effective.runoff_coefficient == 0.4
    with pytest.raises(ValueError, match="runoff coefficient"):
        user_loss_adjustments(
            [_event("2050_SSP2")], default_losses=base, losses_by_aep={}, reference_year=1990,
            factor_provider=lambda _year, _ssp: (0.8, 1.1), complete_storm=False,
        )


def test_scenario_user_losses_reach_screening_without_mutating_native_event() -> None:
    events = [_event(None, rain=25), _event("2050_SSP2", rain=45)]
    base = LossSpec(30, continuing_loss_mm_per_hour=0)
    changes = user_loss_adjustments(
        events, default_losses=base, losses_by_aep={}, reference_year=1990,
        factor_provider=lambda _year, _ssp: (0.5, 1.5), complete_storm=False,
    )
    result = assemble_arr_events(
        events, default_losses=base, losses_by_aep={}, event_loss_adjustments=changes,
        complete_storm=False, continuing_loss_timing="full_increment",
    )
    assert [row.status for row in result.patterns] == [ScreeningStatus.ZERO, ScreeningStatus.POSITIVE]
    assert result.patterns[1].excess_mm == pytest.approx(30)
    assert "ILx0.5" in result.rainfall["1%", 60, "2050_SSP2"][-1]
    assert result.loss_adjustments["1%", 60, "2050_SSP2"].effective.initial_loss_mm == 15
    assert events[1].initial_loss == 99.0
