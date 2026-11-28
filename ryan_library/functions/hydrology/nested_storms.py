"""Experimental nested-frequency storm construction from explicit rainfall-depth curves.

No source download, model execution, or implicit IFD extrapolation. Rainfall
depths are mm, durations and timesteps are integer minutes.
"""

from __future__ import annotations

import csv
from bisect import bisect_left
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import accumulate, pairwise
from math import fsum, isclose, isfinite
from pathlib import Path
from typing import Any, Literal, Protocol

from .rainfall_loss_screening import LossSpec, rainfall_excess_increments, reduced_burst_initial_loss

Loading = Literal["FL", "ML", "BL"]
LossMode = Literal["burst", "storm_reduce", "complete"]
WORKBOOK_DURATIONS_MIN = (60, 180, 360, 720, 1440, 4320)


def _nonnegative(value: float, name: str) -> float:
    if not isfinite(value) or value < 0:
        msg = f"{name} must be finite and nonnegative"
        raise ValueError(msg)
    return value


def _minutes(value: int, name: str) -> int:
    if type(value) is not int or value <= 0:
        msg = f"{name} must be positive integer minutes"
        raise ValueError(msg)
    return value


def _arf(value: float) -> float:
    if not isfinite(value) or not 0 <= value <= 1:
        msg = "ARF must be finite and within [0, 1]"
        raise ValueError(msg)
    return value


@dataclass(frozen=True, slots=True)
class DepthAnchor:
    """Point IFD depth and separate applied factors for this duration.

    For already climate-adjusted upstream point depths use rainfall_factor=1.
    ARF defaults to point rainfall; never supply an already areal depth here.
    """

    duration_min: int
    point_depth_mm: float
    arf: float = 1.0
    rainfall_factor: float = 1.0

    def __post_init__(self) -> None:
        _minutes(self.duration_min, "Anchor duration")
        _nonnegative(self.point_depth_mm, "Point depth")
        _arf(self.arf)
        if not isfinite(self.rainfall_factor) or self.rainfall_factor <= 0:
            msg = "Rainfall factor must be finite and positive"
            raise ValueError(msg)
        _nonnegative(self.scenario_point_mm, "Scenario point depth")

    @property
    def scenario_point_mm(self) -> float:
        return self.point_depth_mm * self.rainfall_factor

    def adjusted_mm(self, arf_override: float | None = None) -> float:
        return self.scenario_point_mm * (self.arf if arf_override is None else _arf(arf_override))


@dataclass(frozen=True, slots=True)
class RainfallCurve:
    """One site, exact source probability label, and climate scenario."""

    aep: str
    source: str
    anchors: tuple[DepthAnchor, ...]
    scenario: str | None = None
    cautions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.aep.strip() or not self.source.strip() or not self.anchors:
            msg = "Rainfall curve requires AEP, source and anchors"
            raise ValueError(msg)
        durations = tuple(anchor.duration_min for anchor in self.anchors)
        if durations != tuple(sorted(set(durations))):
            msg = "Rainfall anchors must be strictly increasing and unique"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class EventLosses:
    """Loss selection for one target event; factors apply BEFORE PB reduction.

    Preburst depth/prefix is already scenario-specific. Alternatively, a ratio
    multiplies the scenario point depth at the full target burst duration.
    The explicit prefix uses the same timestep as the nested burst.
    Proportional loss (fraction, not percent) precedes IL/CL and applies to
    both the prefix and burst, or to the omitted PB used for IL reduction.
    """

    spec: LossSpec
    mode: LossMode = "burst"
    initial_loss_factor: float = 1.0
    continuing_loss_factor: float = 1.0
    preburst_depth_mm: float | None = None
    preburst_ratio: float | None = None
    preburst_increments_mm: tuple[float, ...] = ()
    proportional_loss_fraction: float = 0.0

    def __post_init__(self) -> None:
        _nonnegative(self.initial_loss_factor, "IL factor")
        _nonnegative(self.continuing_loss_factor, "CL factor")
        if not isfinite(self.proportional_loss_fraction) or not 0 <= self.proportional_loss_fraction <= 1:
            msg = "Proportional loss fraction must be within [0, 1]"
            raise ValueError(msg)
        if self.spec.runoff_coefficient is not None and self.proportional_loss_fraction != 0:
            msg = "Do not combine proportional loss with the post-IL runoff coefficient"
            raise ValueError(msg)
        if self.spec.runoff_coefficient is not None and self.continuing_loss_factor != 1:
            msg = "A CL factor is not applicable to runoff coefficient losses"
            raise ValueError(msg)
        for value in self.preburst_increments_mm:
            _nonnegative(value, "Preburst increment")
        supplied = sum((self.preburst_depth_mm is not None, self.preburst_ratio is not None))
        if self.preburst_depth_mm is not None:
            _nonnegative(self.preburst_depth_mm, "Preburst depth")
        if self.preburst_ratio is not None:
            _nonnegative(self.preburst_ratio, "Preburst ratio")
        valid = (
            (self.mode == "burst" and supplied == 0 and not self.preburst_increments_mm)
            or (self.mode == "storm_reduce" and supplied == 1 and not self.preburst_increments_mm)
            or (self.mode == "complete" and supplied == 0 and bool(self.preburst_increments_mm))
        )
        if not valid:
            msg = "Choose exactly one preburst convention: burst, storm_reduce or complete"
            raise ValueError(msg)

    def applied(self, scenario_point_mm: float) -> tuple[LossSpec, float]:
        """Return adopted loss parameters and preburst depth (included or omitted)."""
        initial = self.spec.initial_loss_mm * self.initial_loss_factor
        preburst = fsum(self.preburst_increments_mm)
        if self.mode == "storm_reduce":
            if self.preburst_ratio is not None:
                preburst = self.preburst_ratio * scenario_point_mm
            elif self.preburst_depth_mm is not None:
                preburst = self.preburst_depth_mm
            initial = reduced_burst_initial_loss(initial, preburst * (1 - self.proportional_loss_fraction))
        continuing = self.spec.continuing_loss_mm_per_hour
        return (
            LossSpec(
                initial,
                continuing_loss_mm_per_hour=(
                    continuing * self.continuing_loss_factor if continuing is not None else None
                ),
                runoff_coefficient=self.spec.runoff_coefficient,
                continuing_loss_timing=self.spec.continuing_loss_timing,
            ),
            preburst,
        )


@dataclass(frozen=True, slots=True)
class DepthCheck:
    duration_min: int
    target_mm: float
    maximum_rolling_mm: float


@dataclass(frozen=True, slots=True)
class StormResult:
    curve: RainfallCurve
    duration_min: int
    timestep_min: int
    loading: Loading
    arf_override: float | None
    gross_mm: tuple[float, ...]
    excess_mm: tuple[float, ...] | None
    loss_selection: EventLosses | None
    applied_losses: LossSpec | None
    preburst_mm: float
    preburst_intervals: int
    depth_checks: tuple[DepthCheck, ...]

    @property
    def burst_total_mm(self) -> float:
        return fsum(self.gross_mm[self.preburst_intervals :])


def _interval_order(duration_min: int, timestep_min: int, loading: Loading) -> tuple[int, ...]:
    count = duration_min // timestep_min
    if loading == "FL":
        return tuple(range(count))
    if loading == "BL":
        return tuple(range(count - 1, -1, -1))
    if loading != "ML":
        msg = "Loading must be FL, ML or BL"
        raise ValueError(msg)
    last_odd = count - 1 if count % 2 == 0 else count - 2
    order = (*range(last_odd, 0, -2), *range(0, count, 2))
    # These three workbook sheets place the centre one interval earlier.
    if timestep_min == 1 and duration_min in (360, 1440, 4320):
        return (*order[1:], order[0])
    return order


def _interpolate_increments(
    times: tuple[int, ...], depths: tuple[float, ...], duration_min: int, timestep_min: int
) -> tuple[float, ...]:
    edges = [0.0]
    for minute in range(timestep_min, duration_min + 1, timestep_min):
        index = bisect_left(times, minute)
        if minute == times[index]:
            edges.append(depths[index])
        else:
            fraction = (minute - times[index - 1]) / (times[index] - times[index - 1])
            edges.append(depths[index - 1] + fraction * (depths[index] - depths[index - 1]))
    increments = tuple(right - left for left, right in pairwise(edges))
    if any(value < 0 for value in increments):
        msg = "Interpolation produced negative rainfall"
        raise ArithmeticError(msg)
    return increments


def _burst(
    curve: RainfallCurve, duration_min: int, timestep_min: int, loading: Loading, arf_override: float | None
) -> tuple[tuple[float, ...], tuple[DepthCheck, ...], DepthAnchor]:
    _minutes(duration_min, "Storm duration")
    _minutes(timestep_min, "Timestep")
    if duration_min % timestep_min:
        msg = "Storm duration must be an integer multiple of timestep"
        raise ValueError(msg)
    if arf_override is not None:
        _arf(arf_override)
    anchors = tuple(anchor for anchor in curve.anchors if anchor.duration_min <= duration_min)
    if not anchors or anchors[-1].duration_min != duration_min or anchors[0].duration_min > timestep_min:
        msg = "Require an exact target-duration anchor and source coverage at or below the timestep"
        raise ValueError(msg)
    times = (0, *(anchor.duration_min for anchor in anchors))
    depths = (0.0, *(anchor.adjusted_mm(arf_override) for anchor in anchors))
    if any(right < left for left, right in pairwise(depths)):
        msg = "Adjusted cumulative depths decrease; check ARF/CC factors and input IFD"
        raise ValueError(msg)
    increments = _interpolate_increments(times, depths, duration_min, timestep_min)
    burst = tuple(increments[index] for index in _interval_order(duration_min, timestep_min, loading))
    if not isclose(fsum(burst), depths[-1], rel_tol=1e-12, abs_tol=1e-10):
        msg = "Nested storm mass balance failed"
        raise ArithmeticError(msg)
    prefix = tuple(accumulate(burst, initial=0.0))
    checks: list[DepthCheck] = []
    for anchor, depth in zip(anchors, depths[1:], strict=True):
        if anchor.duration_min % timestep_min == 0:
            width = anchor.duration_min // timestep_min
            maximum = max(prefix[end] - prefix[end - width] for end in range(width, len(prefix)))
            checks.append(DepthCheck(anchor.duration_min, depth, maximum))
    return burst, tuple(checks), anchors[-1]


def generate_storm(
    curve: RainfallCurve,
    duration_min: int,
    *,
    timestep_min: int = 1,
    loading: Loading = "ML",
    arf_override: float | None = None,
    losses: EventLosses | None = None,
) -> StormResult:
    """Generate a nested burst, with optional complete storm or explicit excess.

    Reorder duration-derived increments, not an intensity-ranked substitute.
    Rolling-depth checks are reported rather than silently correcting inputs.
    The source curve and all applied factors remain attached to the result.
    """
    burst, checks, target = _burst(curve, duration_min, timestep_min, loading, arf_override)
    if losses is None:
        return StormResult(curve, duration_min, timestep_min, loading, arf_override, burst, None, None, None, 0.0, 0, checks)
    applied, preburst = losses.applied(target.scenario_point_mm)
    gross = (*losses.preburst_increments_mm, *burst)
    retained = 1 - losses.proportional_loss_fraction
    excess = rainfall_excess_increments(tuple((depth * retained, float(timestep_min)) for depth in gross), applied)
    return StormResult(
        curve,
        duration_min,
        timestep_min,
        loading,
        arf_override,
        gross,
        excess,
        losses,
        applied,
        preburst,
        len(losses.preburst_increments_mm),
        checks,
    )


class BomProbability(Protocol):
    @property
    def label(self) -> str: ...


class BomDepthSource(Protocol):
    @property
    def probabilities(self) -> Sequence[BomProbability]: ...

    @property
    def durations_min(self) -> Sequence[int]: ...

    def depth_mm_at(self, duration_min: int, probability: Any, *, convert_intensity: bool) -> object: ...


def curve_from_bom(
    ifd: BomDepthSource,
    aep: str,
    *,
    source: str,
    arf_by_duration: Mapping[int, float] | None = None,
    rainfall_factors: Mapping[int, float] | None = None,
    scenario: str | None = None,
) -> RainfallCurve:
    """Adapt the existing parsed BoM IFD object; no second CSV parser.

    None means unity factors; a provided factor table must cover every anchor.
    """
    probabilities = tuple(item for item in ifd.probabilities if item.label == aep)
    if len(probabilities) != 1:
        msg = f"Expected exactly one BoM probability matching {aep!r}"
        raise ValueError(msg)
    anchors = tuple(
        DepthAnchor(
            duration,
            float(str(ifd.depth_mm_at(duration, probabilities[0], convert_intensity=True))),
            1.0 if arf_by_duration is None else arf_by_duration[duration],
            1.0 if rainfall_factors is None else rainfall_factors[duration],
        )
        for duration in sorted(ifd.durations_min)
    )
    return RainfallCurve(aep, source, anchors, scenario)


class ArrEvent(Protocol):
    """Structural subset of PyTUFLOW's public EventResult."""

    @property
    def aep_name(self) -> str: ...

    @property
    def duration(self) -> float: ...

    @property
    def depth_point(self) -> float: ...

    @property
    def depth_areal(self) -> float: ...

    @property
    def arf(self) -> float: ...

    @property
    def cc_scenario(self) -> str | None: ...

    @property
    def initial_loss(self) -> float: ...

    @property
    def continuing_loss(self) -> float: ...

    @property
    def preburst(self) -> object | None: ...


def curves_from_arr(events: Iterable[ArrEvent], *, source: str) -> tuple[RainfallCurve, ...]:
    """Adapt already assembled ARR events for ONE site, preserving scenario and ARF.

    Upstream point depths already include scenario rainfall changes. Required
    IFD anchors must be supplied even if no ensemble TP exists at that duration.
    Do not pass a mixture of sites. Inconsistent areal depths are rejected.
    """
    groups: dict[tuple[str, str | None], list[DepthAnchor]] = {}
    for event in events:
        if not isfinite(event.duration) or event.duration != int(event.duration):
            msg = "ARR anchor duration must be integer minutes"
            raise ValueError(msg)
        anchor = DepthAnchor(int(event.duration), event.depth_point, event.arf)
        if not isclose(anchor.adjusted_mm(), event.depth_areal, rel_tol=1e-9, abs_tol=1e-8):
            msg = "ARR point depth, ARF and areal depth disagree"
            raise ValueError(msg)
        groups.setdefault((event.aep_name, event.cc_scenario), []).append(anchor)
    if not groups:
        msg = "No ARR events supplied"
        raise ValueError(msg)
    return tuple(
        RainfallCurve(aep, source, tuple(sorted(anchors, key=lambda anchor: anchor.duration_min)), scenario)
        for (aep, scenario), anchors in groups.items()
    )


def burst_losses_from_arr(event: ArrEvent) -> EventLosses:
    """Reuse an upstream burst IL/CL, with no repeat CC or PB adjustment.

    Complete-storm events require a deliberately selected prefix at the nested
    timestep, not an arbitrary ensemble preburst TP.
    """
    if event.preburst is not None:
        msg = "ARR event has explicit preburst; supply complete-storm EventLosses with a chosen prefix"
        raise ValueError(msg)
    return EventLosses(LossSpec(event.initial_loss, continuing_loss_mm_per_hour=event.continuing_loss))


def write_tuflow_csv(
    path: Path, storms: Mapping[str, StormResult], *, kind: Literal["gross", "excess"]
) -> None:
    """Exclusive-create workbook-shaped candidate RF CSV (not native qualified).

    Time in hours; mm per interval; leading/trailing zero, then blank for
    shorter storms. Gross and excess are deliberately separate choices.
    """
    if not storms or any(not name.strip() or name.strip().casefold() == "time" for name in storms):
        msg = "Nonempty storm names required; Time is reserved"
        raise ValueError(msg)
    if kind not in ("gross", "excess"):
        msg = "Select gross or excess rainfall explicitly"
        raise ValueError(msg)
    steps = {storm.timestep_min for storm in storms.values()}
    if len(steps) != 1:
        msg = "TUFLOW CSV requires a common timestep"
        raise ValueError(msg)
    timestep = next(iter(steps))
    columns: list[tuple[float, ...]] = []
    for storm in storms.values():
        values = storm.gross_mm if kind == "gross" else storm.excess_mm
        if not values or any(not isfinite(value) or value < 0 for value in values):
            msg = "Missing or invalid selected rainfall series"
            raise ValueError(msg)
        columns.append((0.0, *values, 0.0))
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Time", *storms])
        for index in range(max(map(len, columns))):
            writer.writerow([index * timestep / 60, *(col[index] if index < len(col) else "" for col in columns)])
