"""Synthetic tests for nested rainfall and explicit loss/adjustment conventions."""

import csv
from dataclasses import dataclass
from pathlib import Path

import pytest

from ryan_library.functions.hydrology.nested_storms import (
    DepthAnchor,
    EventLosses,
    RainfallCurve,
    burst_losses_from_arr,
    curves_from_arr,
    generate_storm,
    write_tuflow_csv,
)
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec


def _curve() -> RainfallCurve:
    return RainfallCurve("1%", "synthetic", (DepthAnchor(1, 4), DepthAnchor(2, 6), DepthAnchor(4, 8)))


def test_loading_and_depth_checks() -> None:
    curve = _curve()
    front = generate_storm(curve, 4, loading="FL")
    middle = generate_storm(curve, 4, loading="ML")
    back = generate_storm(curve, 4, loading="BL")
    assert front.gross_mm == (4, 2, 1, 1)
    assert middle.gross_mm == (1, 2, 4, 1)
    assert back.gross_mm == (1, 1, 2, 4)
    assert [(check.duration_min, check.maximum_rolling_mm) for check in middle.depth_checks] == [
        (1, 4), (2, 6), (4, 8)
    ]


def test_arf_and_rainfall_factors_apply_at_each_anchor() -> None:
    curve = RainfallCurve(
        "1%", "synthetic", (DepthAnchor(1, 4, 0.5, 1.5), DepthAnchor(4, 8, 0.75, 1.25)), "future"
    )
    assert generate_storm(curve, 4).burst_total_mm == pytest.approx(7.5)
    assert generate_storm(curve, 4, arf_override=1).burst_total_mm == pytest.approx(10)


def test_missing_short_duration_and_decreasing_adjusted_depth_fail() -> None:
    curve = RainfallCurve("1%", "synthetic", (DepthAnchor(2, 4), DepthAnchor(4, 8)))
    with pytest.raises(ValueError, match="source coverage"):
        generate_storm(curve, 4)
    assert generate_storm(curve, 4, timestep_min=2).gross_mm == (4, 4)
    decreasing = RainfallCurve("1%", "synthetic", (DepthAnchor(1, 4), DepthAnchor(4, 8, 0.1)))
    with pytest.raises(ValueError, match="decrease"):
        generate_storm(decreasing, 4)


def test_event_specific_preburst_and_cc_losses() -> None:
    curve = _curve()
    spec = LossSpec(6, continuing_loss_mm_per_hour=0)
    reduced = generate_storm(
        curve,
        4,
        loading="FL",
        losses=EventLosses(spec, "storm_reduce", initial_loss_factor=0.5, preburst_ratio=0.25),
    )
    assert reduced.preburst_mm == 2
    assert reduced.applied_losses == LossSpec(1, continuing_loss_mm_per_hour=0)
    assert reduced.excess_mm == (3, 2, 1, 1)
    supplied_burst = generate_storm(curve, 4, loading="FL", losses=EventLosses(spec))
    assert supplied_burst.excess_mm == (0, 0, 1, 1)
    with pytest.raises(ValueError, match="exceeds"):
        generate_storm(curve, 4, losses=EventLosses(spec, "storm_reduce", preburst_depth_mm=7))
    complete = generate_storm(
        curve, 4, loading="FL", losses=EventLosses(spec, "complete", preburst_increments_mm=(7,))
    )
    assert complete.gross_mm == (7, 4, 2, 1, 1)
    assert complete.excess_mm == (1, 4, 2, 1, 1)
    assert complete.burst_total_mm == 8


def test_continuing_loss_adjustment_and_timing() -> None:
    storm = generate_storm(
        _curve(), 4, loading="FL",
        losses=EventLosses(LossSpec(2, continuing_loss_mm_per_hour=60), continuing_loss_factor=0.5),
    )
    assert storm.excess_mm == pytest.approx((1.75, 1.5, 0.5, 0.5))


@dataclass
class _Event:
    aep_name: str
    duration: float
    depth_point: float
    arf: float
    depth_areal: float
    cc_scenario: str | None = None
    initial_loss: float = 2
    continuing_loss: float = 3
    preburst: object | None = None


def test_arr_uses_scenario_point_depth_once_and_retains_identity() -> None:
    events = (_Event("1%", 1, 4, 0.5, 2, "2050_SSP2"), _Event("1%", 4, 8, 0.75, 6, "2050_SSP2"))
    curve, = curves_from_arr(events, source="site A")
    assert curve.scenario == "2050_SSP2"
    assert generate_storm(curve, 4).burst_total_mm == 6
    assert generate_storm(curve, 4, arf_override=1).burst_total_mm == 8
    assert burst_losses_from_arr(events[1]).spec.initial_loss_mm == 2
    with pytest.raises(ValueError, match="unique"):
        curves_from_arr((*events, events[0]), source="site A")
    with pytest.raises(ValueError, match="disagree"):
        curves_from_arr((_Event("1%", 1, 4, 0.5, 4),), source="site A")
    with pytest.raises(ValueError, match="explicit preburst"):
        burst_losses_from_arr(_Event("1%", 4, 8, 1, 8, preburst=object()))


def test_csv_gross_and_excess_are_explicit_and_no_clobber(tmp_path: Path) -> None:
    curve = _curve()
    storm = generate_storm(curve, 4, loading="FL", losses=EventLosses(LossSpec(4, continuing_loss_mm_per_hour=0)))
    short = generate_storm(curve, 2)
    path = tmp_path / "rain.csv"
    write_tuflow_csv(path, {"long": storm, "short": short}, kind="gross")
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))
    assert rows[0] == ["Time", "long", "short"]
    assert [float(row[1]) for row in rows[1:]] == [0, 4, 2, 1, 1, 0]
    assert rows[-1][2] == ""
    assert float(rows[-1][0]) == pytest.approx(5 / 60)
    with pytest.raises(FileExistsError):
        write_tuflow_csv(path, {"long": storm}, kind="gross")
    with pytest.raises(ValueError, match="Missing"):
        write_tuflow_csv(tmp_path / "missing.csv", {"short": short}, kind="excess")
    write_tuflow_csv(tmp_path / "excess.csv", {"long": storm}, kind="excess")
