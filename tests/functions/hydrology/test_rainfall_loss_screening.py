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
from ryan_library.orchestrators.hydrology.rainfall_loss_workflow import (
    assemble_arr_events,
    assemble_bom_events,
    preflight_temporal_pattern_durations,
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
        "20%",
        1440,
        31,
        LossSpec(30, continuing_loss_mm_per_hour=5),
        (TemporalPattern("uniform", 60, (100 / 24,) * 24),),
    )
    assert screen_event(event)[0].status == ScreeningStatus.ZERO


def test_37mm_temporal_patterns_produce_opposite_results() -> None:
    uniform = TemporalPattern("uniform", 60, (100 / 24,) * 24)
    concentrated = TemporalPattern("burst", 60, (0,) * 23 + (100,))
    event = DesignEvent("10%", 1440, 37, LossSpec(30, continuing_loss_mm_per_hour=5), (uniform, concentrated))
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
    event = DesignEvent("1%", 60, 23, LossSpec(30, continuing_loss_mm_per_hour=0), (burst,), Preburst(10, 60, (100,)))
    result = screen_event(event)[0]
    assert result.excess_mm == pytest.approx(3)
    assert result.status == ScreeningStatus.POSITIVE


def test_preburst_overflow_requires_full_storm() -> None:
    assert reduced_burst_initial_loss(30, 5) == 25
    with pytest.raises(ValueError, match="complete storm"):
        reduced_burst_initial_loss(30, 31)


def test_preburst_per_tp_increments_requires_matching_pattern() -> None:
    event = DesignEvent(
        "1%",
        60,
        31,
        LossSpec(30, continuing_loss_mm_per_hour=0),
        (TemporalPattern("tp1", 60, (100,), 1), TemporalPattern("tp2", 60, (100,), 2)),
        Preburst(1, 60, (100,), {1: (100,)}),
    )
    zero, unresolved = screen_event(event)
    assert zero.status == ScreeningStatus.POSITIVE
    assert unresolved.status == ScreeningStatus.UNRESOLVED
    assert summarize_events((zero, unresolved))[0].status == ScreeningStatus.UNRESOLVED


def test_invalid_increment_sum_marks_unresolved() -> None:
    event = DesignEvent(
        "1%", 60, 37, LossSpec(30, continuing_loss_mm_per_hour=0), (TemporalPattern("invalid", 60, (80,)),)
    )
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

    def depth_mm_at(self, duration_min: int, probability: object, *, convert_intensity: bool) -> float:
        assert convert_intensity
        assert isinstance(probability, _Probability)
        assert probability.label
        return 23.0 if duration_min == 60 else 37.0


class _Provider:
    def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
        assert aep_name
        assert output_notation
        if duration == 60:
            return []
        return [
            SimpleNamespace(
                source="point", region="west", event_id=100, tp_number=1, timestep=60, increments=[100 / 24] * 24
            )
        ]


def test_bom_existing_reader_objects_and_aep_duration_schedules() -> None:
    results = screen_bom_ifd(
        _BoM(),
        temporal_patterns=_Provider(),
        losses_by_aep={
            "20%": {60: LossSpec(30, continuing_loss_mm_per_hour=5), 1440: LossSpec(30, continuing_loss_mm_per_hour=5)},
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
    pattern = SimpleNamespace(event_id=17, tp_number=1, timestep=60, increments=[100], source="point", region="West")
    pb = SimpleNamespace(depth=10, timestep=60, increments=[100], per_tp_increments=None)
    event = SimpleNamespace(
        aep_name="2%",
        duration=60,
        depth_areal=23,
        initial_loss=30,
        continuing_loss=0,
        patterns=[pattern],
        preburst=pb,
        cc_scenario=None,
    )
    assert screen_arr_events([event])[0].excess_mm == pytest.approx(3)
    assert screen_arr_events([event], runoff_coefficients_by_aep={"2%": 0.25})[0].excess_mm == pytest.approx(0.75)


def test_guaranteed_zero_is_independent_of_missing_temporal_patterns() -> None:
    bad = DesignEvent(
        "2EY", 1, 0.1, LossSpec(0.2, continuing_loss_mm_per_hour=10), (TemporalPattern("unavailable", 42, (1,)),)
    )
    assert screen_event(bad)[0].status == ScreeningStatus.ZERO


def test_missing_tp_provider_is_unresolved_except_guaranteed_depth_bound() -> None:
    result = screen_bom_ifd(
        _BoM(),
        losses_by_aep={"20%": LossSpec(30, continuing_loss_mm_per_hour=5)},
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
        msg = "No temporal pattern for this duration"
        raise RuntimeError(msg)


def test_provider_error_does_not_convert_unknown_event_to_zero() -> None:
    result = screen_bom_ifd(
        _BoM(),
        temporal_patterns=_RejectingProvider(),
        losses_by_aep={"1%": LossSpec(30, continuing_loss_mm_per_hour=5)},
    )
    keyed = {(item.aep, item.duration_min): item for item in result}
    assert keyed["1%", 60].status == ScreeningStatus.ZERO
    assert keyed["1%", 1440].status == ScreeningStatus.UNRESOLVED


def test_arr_duration_specific_loss_overrides() -> None:
    pattern = SimpleNamespace(event_id=10, tp_number=1, timestep=60, increments=[100], source="point", region="west")
    event = SimpleNamespace(
        aep_name="1%",
        duration=60,
        depth_areal=37,
        initial_loss=50,
        continuing_loss=0,
        patterns=[pattern],
        preburst=None,
        cc_scenario=None,
    )
    assert screen_arr_events([event])[0].status == ScreeningStatus.ZERO
    custom = screen_arr_events(
        [event],
        losses_by_aep={"1%": {60: LossSpec(30, continuing_loss_mm_per_hour=0)}},
    )
    assert custom[0].excess_mm == pytest.approx(7)
    wrong_duration = screen_arr_events(
        [event],
        losses_by_aep={"1%": {1440: LossSpec(30, continuing_loss_mm_per_hour=0)}},
    )
    assert wrong_duration[0].status == ScreeningStatus.ZERO


def test_maximum_tp_bin_and_maximum_tp_total_are_distinct() -> None:
    events = (
        TemporalPattern("tp01", 60, (50, 50), 1),
        TemporalPattern("tp02", 60, (75, 25), 2),
    )
    event = DesignEvent("1%", 120, 20, LossSpec(0, continuing_loss_mm_per_hour=0), events)
    outcomes = screen_event(event)
    assert [result.excess_mm for result in outcomes] == pytest.approx([20, 20])
    assert [result.max_bin_excess_mm for result in outcomes] == pytest.approx([10, 15])
    total = summarize_events(outcomes)[0]
    assert total.max_tp_total_excess_mm == pytest.approx(20)
    assert total.max_tp_bin_excess_mm == pytest.approx(15)
    assert total.max_total_pattern == "tp01"


def test_full_increment_cl_timing_can_wipe_120_min_storm_where_fractional_cannot() -> None:
    pattern = TemporalPattern("last-bin", 10, (0,) * 11 + (100,), 1)
    preburst = Preburst(0.3926, 10, (100,))
    fractional = DesignEvent("63.2%", 120, 30.2, LossSpec(30, continuing_loss_mm_per_hour=5), (pattern,), preburst)
    full_increment = DesignEvent(
        "63.2%",
        120,
        30.2,
        LossSpec(30, continuing_loss_mm_per_hour=5, continuing_loss_timing="full_increment"),
        (pattern,),
        preburst,
    )
    fractional_outcome = screen_event(fractional)[0]
    full_outcome = screen_event(full_increment)[0]
    assert fractional_outcome.status == ScreeningStatus.POSITIVE
    assert fractional_outcome.excess_mm is not None
    assert fractional_outcome.excess_mm > 0
    assert full_outcome.status == ScreeningStatus.ZERO
    assert full_outcome.excess_mm == pytest.approx(0)


def test_full_increment_is_not_a_guaranteed_zero_for_short_tp_bin() -> None:
    pattern = TemporalPattern("last-bin", 6, (0,) * 19 + (100,), 1)
    event = DesignEvent(
        "63.2%",
        120,
        30.2,
        LossSpec(30, continuing_loss_mm_per_hour=5, continuing_loss_timing="full_increment"),
        (pattern,),
        Preburst(0.3926, 6, (100,)),
    )
    result = screen_event(event)[0]
    assert result.status == ScreeningStatus.POSITIVE
    assert result.excess_mm == pytest.approx(0.0926)


def test_bom_arf_and_manual_cc_multiplier_precede_losses() -> None:
    losses = {"20%": LossSpec(30, continuing_loss_mm_per_hour=0)}
    unadjusted = screen_bom_ifd(_BoM(), temporal_patterns=_Provider(), losses_by_aep=losses)
    adjusted = screen_bom_ifd(
        _BoM(),
        temporal_patterns=_Provider(),
        losses_by_aep=losses,
        arf_by_event={("20%", 60): 1.0, ("20%", 1440): 0.5},
        aeps={"20%"},
    )
    assert {(s.aep, s.duration_min): s.status for s in unadjusted}["20%", 1440] == ScreeningStatus.POSITIVE
    assert {(s.aep, s.duration_min): s.status for s in adjusted}["20%", 1440] == ScreeningStatus.ZERO
    scaled = screen_bom_ifd(
        _BoM(),
        temporal_patterns=_Provider(),
        losses_by_aep=losses,
        arf_by_event={("20%", 60): 1.0, ("20%", 1440): 0.5},
        aeps={"20%"},
        rainfall_multiplier=2.0,
    )
    assert {(s.aep, s.duration_min): s.status for s in scaled}["20%", 1440] == ScreeningStatus.POSITIVE
    with pytest.raises(ValueError, match="invalid ARF"):
        screen_bom_ifd(
            _BoM(),
            losses_by_aep=losses,
            arf_by_event={("20%", 60): 1.0},
            aeps={"20%"},
        )


def test_orchestrator_arr_records_arfs_and_tp_region() -> None:
    pattern = SimpleNamespace(
        source="point",
        region="Rangelands West",
        event_id=17,
        tp_number=1,
        timestep=60,
        increments=[100],
    )
    source = SimpleNamespace(
        aep_name="1%",
        duration=60,
        depth_point=42.0,
        arf=0.8,
        depth_areal=33.6,
        initial_loss=30.0,
        continuing_loss=5.0,
        patterns=[pattern],
        preburst=None,
        cc_scenario="2050_SSP2",
    )
    bundle = assemble_arr_events(
        [source],
        default_losses=None,
        losses_by_aep={},
        continuing_loss_timing="full_increment",
    )
    assert len(bundle.patterns) == 1
    assert bundle.rainfall["1%", 60, "2050_SSP2"] == (42.0, 33.6, 0.8, None, "ARR IL 30; CL 5")
    assert bundle.pattern_sets == ("point: Rangelands West",)


def test_orchestrator_bom_applies_arf_before_screening() -> None:
    bundle = assemble_bom_events(
        _BoM(),
        aeps={"20%"},
        durations={60, 1440},
        temporal_patterns=_Provider(),
        losses_by_aep={"20%": LossSpec(30, continuing_loss_mm_per_hour=0)},
        default_losses=None,
        arf_by_event={("20%", 60): 1.0, ("20%", 1440): 0.5},
        rainfall_multiplier=1,
        preburst_depth_mm=None,
    )
    assert len(bundle.patterns) == 2
    assert all(s.status == ScreeningStatus.ZERO for s in bundle.patterns)
    assert bundle.rainfall["20%", 1440, None] == (37.0, 18.5, 0.5, None, "IL 30; CL 0")


def test_tp_preflight_groups_unsupported_durations_without_inventing_zeroes() -> None:
    """All AEP bands missing 5-min data must be one omitted duration, not 2 DROP rows."""
    from pytuflow.arr.exceptions import ArrError

    class Patterns:
        def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
            assert aep_name
            assert output_notation == "aep"
            if duration == 5:
                msg = "No point temporal pattern available for duration 5.0 min, AEP band 'frequent'."
                raise ArrError(msg)
            if duration == 30:
                return [SimpleNamespace(source="point", region="Rangelands West")]
            return [SimpleNamespace(source="areal", region="Rangelands West")]

    outcome = preflight_temporal_pattern_durations(
        Patterns(),
        durations=[5, 30, 60],
        aeps=["63.2%", "1%"],
        area_km2=350,
        allow_point_fallback=True,
    )
    assert outcome.included_durations == (30, 60)
    assert outcome.omitted_durations == (5,)
    assert outcome.point_fallback_durations == (30,)
    assert outcome.missing_bands[5] == ("frequent", "rare")

    no_fallback = preflight_temporal_pattern_durations(
        Patterns(),
        durations=[5, 30, 60],
        aeps=["63.2%", "1%"],
        area_km2=350,
        allow_point_fallback=False,
    )
    assert no_fallback.included_durations == (60,)
    assert no_fallback.omitted_durations == (5, 30)
    assert no_fallback.point_fallback_durations == ()


def test_tp_preflight_point_only_is_expected_for_small_catchments() -> None:
    class Patterns:
        def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
            assert duration == 60
            assert aep_name == "1%"
            assert output_notation == "aep"
            return [SimpleNamespace(source="point", region="Rangelands West")]

    outcome = preflight_temporal_pattern_durations(
        Patterns(),
        durations=[60],
        aeps=["1%"],
        area_km2=0,
    )
    assert outcome.included_durations == (60,)
    assert outcome.point_fallback_durations == ()


def test_preflight_preserves_deliberate_short_duration_point_tps() -> None:
    """Shorter-than-areal-start point TPs are not an areal fallback."""
    from pytuflow.arr.exceptions import ArrError

    class ArealDurations:
        empty = False

        def __getitem__(self, column: str) -> SimpleNamespace:
            assert column == "duration"
            return SimpleNamespace(min=lambda: 60)

    class Patterns:
        areal_tp = ArealDurations()

        def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> list[object]:
            assert aep_name
            assert output_notation == "aep"
            if duration == 5:
                msg = "No point temporal pattern available for duration 5.0 min, AEP band 'frequent'."
                raise ArrError(msg)
            source = "point" if duration in {30, 120} else "areal"
            return [SimpleNamespace(source=source)]

    provider = Patterns()
    accepted = preflight_temporal_pattern_durations(
        provider,
        durations=[5, 30, 120, 180],
        aeps=["63.2%", "1%"],
        area_km2=350,
        allow_point_fallback=True,
    )
    assert accepted.included_durations == (30, 120, 180)
    assert accepted.omitted_durations == (5,)
    assert accepted.point_fallback_durations == (120,)

    disabled = preflight_temporal_pattern_durations(
        provider,
        durations=[5, 30, 120, 180],
        aeps=["63.2%", "1%"],
        area_km2=350,
        allow_point_fallback=False,
    )
    assert disabled.included_durations == (30, 180)
    assert disabled.omitted_durations == (5, 120)



def test_pattern_independent_zero_reports_all_available_tps_without_inventing_missing_ones() -> None:
    """Every supplied TP is certified zero, but an absent TP set is not one evaluated TP."""
    tp_list = tuple(
        TemporalPattern(f"TP{i:02d}", 999, (0.0,), i)
        for i in range(1, 11)
    )
    with_patterns = DesignEvent(
        "20%", 60, 23, LossSpec(30, continuing_loss_mm_per_hour=5), tp_list
    )
    screens = screen_event(with_patterns)
    assert len(screens) == 10
    assert all(item.status == ScreeningStatus.ZERO for item in screens)
    assert [item.pattern_id for item in screens] == [f"TP{i:02d}" for i in range(1, 11)]
    summary = summarize_events(screens)[0]
    assert summary.zero_patterns == 10
    assert summary.total_patterns == 10
    assert summary.positive_patterns == 0

    no_patterns = DesignEvent("20%", 60, 23, LossSpec(30, continuing_loss_mm_per_hour=5))
    certified = screen_event(no_patterns)
    assert len(certified) == 1
    assert certified[0].pattern_id is None
    summary_empty = summarize_events(certified)[0]
    assert summary_empty.status == ScreeningStatus.ZERO
    assert summary_empty.zero_patterns == 0
