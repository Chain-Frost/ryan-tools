"""Synthetic loss-screening tests; no hydraulic-engine parity is claimed."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from ryan_library.functions.hydrology.rainfall_loss_screening import (
    DesignEvent,
    LossSpec,
    Preburst,
    ScreeningStatus,
    TemporalPattern,
    reduced_burst_initial_loss,
    screen_arr_events,
    screen_bom_ifd,
    screen_event,
    summarize_events,
)


def _pattern(depth_count: int, increments: tuple[float, ...], *, identity: str = "1") -> TemporalPattern:
    return TemporalPattern(identity, 60.0 / depth_count, increments, 1)


def test_rainfall_below_initial_loss_requires_no_temporal_pattern() -> None:
    event = DesignEvent("20%", 60, 23, LossSpec(30, continuing_loss_mm_per_hour=5))
    result = screen_event(event)
    assert len(result) == 1
    assert result[0].status == ScreeningStatus.ZERO
    assert result[0].excess_mm == 0


def test_31mm_without_pattern_is_unresolved_not_falsely_dropped() -> None:
    event = DesignEvent("20%", 60, 31, LossSpec(30, continuing_loss_mm_per_hour=5))
    assert screen_event(event)[0].status == ScreeningStatus.UNRESOLVED


def test_late_concentrated_31mm_can_exceed_losses() -> None:
    pattern = _pattern(10, (0.0,) * 9 + (100.0,))
    event = DesignEvent("20%", 60, 31, LossSpec(30, continuing_loss_mm_per_hour=5), (pattern,))
    screen = screen_event(event)[0]
    assert screen.status == ScreeningStatus.POSITIVE
    # CL acts only for the post-IL portion of the six-minute interval.
    assert screen.excess_mm == pytest.approx(1 - (5 * 0.1 / 31))


def test_long_low_intensity_31mm_is_zero() -> None:
    event = DesignEvent(
        "20%", 1440, 31, LossSpec(30, continuing_loss_mm_per_hour=5),
        (TemporalPattern("uniform", 60, (100 / 24,) * 24),),
    )
    assert screen_event(event)[0].status == ScreeningStatus.ZERO


def test_37mm_temporal_patterns_produce_opposite_results() -> None:
    uniform = TemporalPattern("uniform", 60, (100 / 24,) * 24)
    concentrated = TemporalPattern("burst", 60, (0,) * 23 + (100,))
    event = DesignEvent("10%", 1440, 37, LossSpec(30, continuing_loss_mm_per_hour=5),
                        (uniform, concentrated))
    statuses = [s.status for s in screen_event(event)]
    assert statuses == [ScreeningStatus.ZERO, ScreeningStatus.POSITIVE]
    assert summarize_events(screen_event(event))[0].status == ScreeningStatus.POSITIVE


def test_runoff_coefficient_after_il_and_coefficient_zero() -> None:
    profile = _pattern(1, (100,))
    positive = DesignEvent("1%", 60, 37, LossSpec(30, runoff_coefficient=0.25), (profile,))
    assert screen_event(positive)[0].excess_mm == pytest.approx(1.75)
    zero = DesignEvent("1%", 60, 37, LossSpec(30, runoff_coefficient=0))
    assert screen_event(zero)[0].status == ScreeningStatus.ZERO


def test_explicit_preburst_uses_storm_il_without_double_reduction() -> None:
    burst = _pattern(1, (100,))
    event = DesignEvent("1%", 60, 23, LossSpec(30, continuing_loss_mm_per_hour=0), (burst,),
                        Preburst(10, 60, (100,)))
    result = screen_event(event)[0]
    assert result.excess_mm == pytest.approx(3)
    assert result.status == ScreeningStatus.POSITIVE


def test_preburst_overflow_requires_full_storm() -> None:
    assert reduced_burst_initial_loss(30, 5) == 25
    with pytest.raises(ValueError, match="complete storm"):
        reduced_burst_initial_loss(30, 31)


def test_preburst_per_tp_increments_requires_matching_pattern() -> None:
    event = DesignEvent("1%", 60, 31, LossSpec(30, continuing_loss_mm_per_hour=0),
                        (TemporalPattern("tp1", 60, (100,), 1), TemporalPattern("tp2", 60, (100,), 2)),
                        Preburst(1, 60, (100,), {1: (100,)}))
    zero, unresolved = screen_event(event)
    assert zero.status == ScreeningStatus.POSITIVE
    assert unresolved.status == ScreeningStatus.UNRESOLVED
    assert summarize_events((zero, unresolved))[0].status == ScreeningStatus.UNRESOLVED


def test_invalid_increment_sum_marks_unresolved() -> None:
    event = DesignEvent("1%", 60, 37, LossSpec(30, continuing_loss_mm_per_hour=0),
                        (TemporalPattern("invalid", 60, (80,)),))
    assert screen_event(event)[0].status == ScreeningStatus.UNRESOLVED


def test_invalid_loss_model_fails_at_construction() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        LossSpec(30, continuing_loss_mm_per_hour=5, runoff_coefficient=0.25)
    with pytest.raises(ValueError, match="Runoff coefficient"):
        LossSpec(30, runoff_coefficient=float("nan"))


@dataclass(frozen=True)
class _Probability:
    label: str


class _BoM:
    probabilities = (_Probability("20%"), _Probability("1%"))
    durations_min = (60, 1440)

    def depth_mm_at(self, duration_min: int, probability: _Probability, *, convert_intensity: bool) -> float:
        assert convert_intensity
        assert probability.label
        return 23.0 if duration_min == 60 else 37.0


class _Provider:
    def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
        assert aep_name
        assert output_notation
        if duration == 60:
            return []
        return [SimpleNamespace(source="point", region="west", event_id=100, tp_number=1,
                                timestep=60, increments=[100 / 24] * 24)]


def test_bom_existing_reader_objects_and_aep_duration_schedules() -> None:
    results = screen_bom_ifd(
        _BoM(), temporal_patterns=_Provider(),
        losses_by_aep={
            "20%": {60: LossSpec(30, continuing_loss_mm_per_hour=5),
                    1440: LossSpec(30, continuing_loss_mm_per_hour=5)},
            "1%": LossSpec(20, runoff_coefficient=0.3),
        },
    )
    assert len(results) == 4
    keyed = {(r.aep, r.duration_min): r for r in results}
    assert keyed["20%", 60].status == ScreeningStatus.ZERO
    assert keyed["1%", 60].status == ScreeningStatus.UNRESOLVED
    assert keyed["20%", 1440].status == ScreeningStatus.ZERO
    assert keyed["1%", 1440].status == ScreeningStatus.POSITIVE


def test_arr_pytuflow_event_adapter_reads_preburst_and_arfs() -> None:
    pattern = SimpleNamespace(event_id=17, tp_number=1, timestep=60, increments=[100],
                              source="point", region="West")
    pb = SimpleNamespace(depth=10, timestep=60, increments=[100], per_tp_increments=None)
    event = SimpleNamespace(aep_name="2%", duration=60, depth_areal=23, initial_loss=30,
                            continuing_loss=0, patterns=[pattern], preburst=pb, cc_scenario=None)
    assert screen_arr_events([event])[0].excess_mm == pytest.approx(3)
    assert screen_arr_events([event], runoff_coefficients_by_aep={"2%": 0.25})[0].excess_mm == pytest.approx(.75)


def test_guaranteed_zero_is_independent_of_missing_temporal_patterns() -> None:
    bad = DesignEvent("2EY", 1, 0.1, LossSpec(0.2, continuing_loss_mm_per_hour=10),
                      (TemporalPattern("unavailable", 42, (1,)),))
    assert screen_event(bad)[0].status == ScreeningStatus.ZERO


def test_missing_tp_provider_is_unresolved_except_guaranteed_depth_bound() -> None:
    result = screen_bom_ifd(
        _BoM(), losses_by_aep={"20%": LossSpec(30, continuing_loss_mm_per_hour=5)},
    )
    keyed = {(item.aep, item.duration_min): item for item in result}
    assert keyed["20%", 60].status == ScreeningStatus.ZERO
    assert keyed["20%", 1440].status == ScreeningStatus.UNRESOLVED
    assert keyed["1%", 60].status == ScreeningStatus.UNRESOLVED


class _RejectingProvider:
    def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
        assert duration > 0
        assert aep_name
        assert output_notation
        raise RuntimeError("No temporal pattern for this duration")


def test_provider_error_does_not_convert_unknown_event_to_zero() -> None:
    result = screen_bom_ifd(
        _BoM(), temporal_patterns=_RejectingProvider(),
        losses_by_aep={"1%": LossSpec(30, continuing_loss_mm_per_hour=5)},
    )
    keyed = {(item.aep, item.duration_min): item for item in result}
    assert keyed["1%", 60].status == ScreeningStatus.ZERO
    assert keyed["1%", 1440].status == ScreeningStatus.UNRESOLVED
