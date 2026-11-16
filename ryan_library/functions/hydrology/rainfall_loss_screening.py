"""Conservative, rainfall-excess-only screening of design storm temporal patterns.

The input adapters intentionally reuse the BoM reader in 'runoff_routing.bom_ifd'
(if installed) and the ARR 2026 assembly in 'pytuflow.arr'. Neither is copied here.
A zero verdict applies only to the selected patterns and stated loss convention;
it is not a statement about routed model discharge or unexamined patterns.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib import import_module
from math import isclose, isfinite
from pathlib import Path
from typing import Any, Protocol, cast


class ScreeningStatus(StrEnum):
    ZERO = "zero"
    POSITIVE = "positive"
    UNRESOLVED = "unresolved"


def _nonnegative(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result) or result < 0:
        msg = f"{name} must be finite and nonnegative"
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class LossSpec:
    """Exactly one of IL/CL or IL + post-IL runoff coefficient must be specified.

    'initial_loss_mm' is the *burst* IL for a burst-only event, or the
    *storm* IL when 'DesignEvent.preburst' supplies a full preburst prefix.
    No automatic preburst subtraction or double counting occurs.
    """

    initial_loss_mm: float
    continuing_loss_mm_per_hour: float | None = None
    runoff_coefficient: float | None = None

    def __post_init__(self) -> None:
        _nonnegative(self.initial_loss_mm, "initial loss")
        if (self.continuing_loss_mm_per_hour is None) == (self.runoff_coefficient is None):
            msg = "Specify exactly one of continuing loss or runoff coefficient"
            raise ValueError(msg)
        if self.continuing_loss_mm_per_hour is not None:
            _nonnegative(self.continuing_loss_mm_per_hour, "continuing loss")
        if self.runoff_coefficient is not None and (
            not isfinite(self.runoff_coefficient) or not 0 <= self.runoff_coefficient <= 1
        ):
            msg = "Runoff coefficient must be finite and between zero and one"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TemporalPattern:
    identity: str
    timestep_min: float
    increments_percent: tuple[float, ...]
    tp_number: int | None = None


@dataclass(frozen=True, slots=True)
class Preburst:
    """Explicit *complete-storm* prefix. Its depth is additional to burst depth.

    'per_tp_increments' matches PyTUFLOW's per-TP preburst convention. Values
    are percentages, keyed by temporal-pattern number.
    """

    depth_mm: float
    timestep_min: float
    increments_percent: tuple[float, ...]
    per_tp_increments: Mapping[int, tuple[float, ...]] | None = None


@dataclass(frozen=True, slots=True)
class DesignEvent:
    aep: str
    duration_min: float
    depth_mm: float
    losses: LossSpec | None
    patterns: tuple[TemporalPattern, ...] = ()
    preburst: Preburst | None = None
    source: str = "user"
    scenario: str | None = None


@dataclass(frozen=True, slots=True)
class PatternScreen:
    aep: str
    duration_min: float
    pattern_id: str | None
    source: str
    scenario: str | None
    status: ScreeningStatus
    excess_mm: float | None
    max_excess_rate_mm_per_hour: float | None
    reason: str


@dataclass(frozen=True, slots=True)
class EventScreen:
    aep: str
    duration_min: float
    source: str
    scenario: str | None
    status: ScreeningStatus
    total_patterns: int
    zero_patterns: int
    positive_patterns: int
    unresolved_patterns: int


def reduced_burst_initial_loss(storm_initial_loss_mm: float, preburst_depth_mm: float) -> float:
    """Compute burst IL from preburst depth; refuse preburst overflow.

    When preburst depth exceeds storm IL, preburst-period runoff might occur.
    Clamping to zero would incorrectly permit a false 'no-runoff' verdict.
    Model the preburst as a complete storm instead.
    """
    initial = _nonnegative(storm_initial_loss_mm, "storm initial loss")
    depth = _nonnegative(preburst_depth_mm, "preburst depth")
    if depth > initial:
        msg = "Preburst exceeds storm initial loss; screen a complete storm instead"
        raise ValueError(msg)
    return initial - depth


def _fractions(increments: Sequence[float], name: str) -> tuple[float, ...]:
    if not increments:
        msg = f"{name} has no increments"
        raise ValueError(msg)
    values = tuple(_nonnegative(value, f"{name} increment") for value in increments)
    total = sum(values)
    if not 99.5 <= total <= 100.5:
        msg = f"{name} increments must sum to 100% within 0.5 percentage points"
        raise ValueError(msg)
    # ARR temporal-pattern exports can have small decimal-rounding differences.
    # Normalise to avoid screening against rainfall smaller than the design depth.
    return tuple(value / total for value in values)


def _steps(
    depth_mm: float, timestep_min: float, increments: Sequence[float], *, name: str
) -> tuple[tuple[float, float], ...]:
    depth = _nonnegative(depth_mm, f"{name} depth")
    if not isfinite(timestep_min) or timestep_min <= 0:
        msg = f"{name} timestep must be finite and positive"
        raise ValueError(msg)
    return tuple((depth * fraction, float(timestep_min)) for fraction in _fractions(increments, name))


def _rainfall_excess(steps: Sequence[tuple[float, float]], losses: LossSpec) -> tuple[float, float]:
    """Sequential IL, then CL or coefficient; assume uniform rain within each interval.

    In an IL-crossing interval only the post-IL fraction of the time interval
    has continuing loss applied. This is a screening convention, not a claim
    of numeric parity with RORBwin, TUFLOW, or an infiltration model.
    """
    il_remaining = losses.initial_loss_mm
    total_excess = 0.0
    peak_rate = 0.0
    for rain, dt in steps:
        consumed_il = min(il_remaining, rain)
        il_remaining -= consumed_il
        after_il = rain - consumed_il
        if losses.runoff_coefficient is not None:
            excess = after_il * losses.runoff_coefficient
        else:
            continuing_loss = losses.continuing_loss_mm_per_hour
            if continuing_loss is None:
                msg = "Continuing loss must be supplied for IL/CL accounting"
                raise ValueError(msg)
            post_il_fraction = after_il / rain if rain else 0.0
            continuing_capacity = continuing_loss * dt / 60.0 * post_il_fraction
            excess = after_il - min(after_il, continuing_capacity)
        total_excess += excess
        peak_rate = max(peak_rate, excess * 60.0 / dt)
    return total_excess, peak_rate


def _event_excess(event: DesignEvent, pattern: TemporalPattern, losses: LossSpec) -> tuple[float, float]:
    """Assemble a single complete-storm hyetograph for screening."""
    burst = _steps(event.depth_mm, pattern.timestep_min, pattern.increments_percent, name="Burst")
    if not isclose(len(burst) * pattern.timestep_min, event.duration_min, rel_tol=0, abs_tol=1e-6):
        msg = "Temporal pattern timestep/increment count disagrees with design duration"
        raise ValueError(msg)
    preburst_steps: tuple[tuple[float, float], ...] = ()
    if (pb := event.preburst) is not None:
        increments = pb.increments_percent
        if pb.per_tp_increments is not None:
            if pattern.tp_number is None or pattern.tp_number not in pb.per_tp_increments:
                msg = "No matching preburst temporal pattern number"
                raise ValueError(msg)
            increments = pb.per_tp_increments[pattern.tp_number]
        preburst_steps = _steps(pb.depth_mm, pb.timestep_min, increments, name="Preburst")
    return _rainfall_excess((*preburst_steps, *burst), losses)


def screen_event(event: DesignEvent) -> tuple[PatternScreen, ...]:
    """Screen each *selected* temporal pattern; missing inputs are unresolved.

    A pattern-independent zero is allowed only when the *entire* storm depth
    (including explicit preburst, if present) is at most IL, or coefficient=0.
    """
    _nonnegative(event.depth_mm, "design burst depth")
    if not isfinite(event.duration_min) or event.duration_min <= 0:
        msg = "Design duration must be finite and positive"
        raise ValueError(msg)
    if event.preburst is not None:
        _nonnegative(event.preburst.depth_mm, "preburst depth")
    total_depth = event.depth_mm + (event.preburst.depth_mm if event.preburst is not None else 0)

    def make(
        status: ScreeningStatus,
        reason: str,
        pattern: str | None = None,
        excess: float | None = None,
        peak: float | None = None,
    ) -> PatternScreen:
        return PatternScreen(
            event.aep, event.duration_min, pattern, event.source, event.scenario, status, excess, peak, reason
        )

    if event.losses is None:
        return (make(ScreeningStatus.UNRESOLVED, "No losses supplied"),)
    losses = event.losses
    if total_depth <= losses.initial_loss_mm:
        return (make(ScreeningStatus.ZERO, "Entire storm depth does not exceed initial loss", excess=0, peak=0),)
    if losses.runoff_coefficient == 0:
        return (make(ScreeningStatus.ZERO, "Runoff coefficient is zero", excess=0, peak=0),)
    if not event.patterns:
        return (make(ScreeningStatus.UNRESOLVED, "No applicable temporal patterns supplied"),)

    screened: list[PatternScreen] = []
    for pattern in event.patterns:
        try:
            excess, peak = _event_excess(event, pattern, losses)
        except (TypeError, ValueError, OverflowError) as exc:
            screened.append(make(ScreeningStatus.UNRESOLVED, str(exc), pattern.identity))
            continue
        status = ScreeningStatus.POSITIVE if excess > 0 else ScreeningStatus.ZERO
        screened.append(
            make(
                status,
                "Positive rainfall excess" if excess > 0 else "Losses absorb every interval",
                pattern.identity,
                excess,
                peak,
            )
        )
    return tuple(screened)


def summarize_events(screens: Iterable[PatternScreen]) -> tuple[EventScreen, ...]:
    """Aggregate only the *provided* patterns; never infer unprovided coverage."""
    grouped: dict[tuple[str, float, str, str | None], list[PatternScreen]] = {}
    for screen in screens:
        grouped.setdefault((screen.aep, screen.duration_min, screen.source, screen.scenario), []).append(screen)
    results: list[EventScreen] = []
    for (aep, duration, source, scenario), group in grouped.items():
        positives = sum(item.status == ScreeningStatus.POSITIVE for item in group)
        unresolved = sum(item.status == ScreeningStatus.UNRESOLVED for item in group)
        zeros = sum(item.status == ScreeningStatus.ZERO for item in group)
        status = (
            ScreeningStatus.UNRESOLVED
            if unresolved
            else ScreeningStatus.POSITIVE
            if positives
            else ScreeningStatus.ZERO
        )
        results.append(EventScreen(aep, duration, source, scenario, status, len(group), zeros, positives, unresolved))
    return tuple(results)


class _BoMProbability(Protocol):
    @property
    def label(self) -> str: ...


class _BoMSource(Protocol):
    @property
    def probabilities(self) -> Sequence[_BoMProbability]: ...

    @property
    def durations_min(self) -> Sequence[int]: ...

    def depth_mm_at(self, duration_min: int, probability: Any, *, convert_intensity: bool) -> object: ...


class _PatternProvider(Protocol):
    def patterns(self, duration: int, aep_name: str, output_notation: str = "aep") -> Sequence[object]: ...


class _ArrRunner(Protocol):
    def run(self) -> list[object]: ...


def _pytuflow_pattern(pattern: object) -> TemporalPattern:
    """Adapt a selected PyTUFLOW temporal pattern without re-parsing its CSV."""
    # Runtime public dataclasses in PyTUFLOW currently expose these properties.
    item = cast(Any, pattern)
    identity = f"{item.source}:{item.region or 'default'}:{item.event_id}:TP{item.tp_number:02d}"
    return TemporalPattern(
        identity, float(item.timestep), tuple(float(x) for x in item.increments), int(item.tp_number)
    )


def _pytuflow_preburst(preburst: object) -> Preburst:
    item = cast(Any, preburst)
    per = item.per_tp_increments
    return Preburst(
        float(item.depth),
        float(item.timestep),
        tuple(float(x) for x in item.increments),
        {int(k): tuple(float(x) for x in values) for k, values in per.items()} if per else None,
    )


def screen_arr_events(
    events: Iterable[object],
    *,
    runoff_coefficients_by_aep: Mapping[str, float] | None = None,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]] | None = None,
    default_losses: LossSpec | None = None,
) -> tuple[PatternScreen, ...]:
    """Screen already assembled 'pytuflow.arr.engine.EventResult' instances.

    PyTUFLOW owns the ARR 2026 IFD, AEP selection, ARF, preburst, and TP choices.
    In burst-only mode its 'initial_loss' is already preburst-adjusted; in
    complete-storm mode its preburst and full storm IL are used together.
    """
    screens: list[PatternScreen] = []
    for source in events:
        item = cast(Any, source)
        name = str(item.aep_name)
        coeff = runoff_coefficients_by_aep.get(name) if runoff_coefficients_by_aep is not None else None
        selection = losses_by_aep.get(name) if losses_by_aep is not None else None
        selected_losses = selection.get(int(item.duration)) if isinstance(selection, Mapping) else selection
        override = selected_losses if selected_losses is not None else default_losses
        losses = (
            override
            if override is not None
            else LossSpec(float(item.initial_loss), runoff_coefficient=float(coeff))
            if coeff is not None
            else LossSpec(float(item.initial_loss), continuing_loss_mm_per_hour=float(item.continuing_loss))
        )
        event = DesignEvent(
            name,
            float(item.duration),
            float(item.depth_areal),
            losses,
            tuple(_pytuflow_pattern(p) for p in item.patterns),
            _pytuflow_preburst(item.preburst) if item.preburst is not None else None,
            "arr_datahub",
            str(item.cc_scenario) if item.cc_scenario is not None else None,
        )
        screens.extend(screen_event(event))
    return tuple(screens)


def screen_arr_datahub(config: object, response: object) -> tuple[PatternScreen, ...]:
    """Assemble ARR 2026 data using PyTUFLOW, then screen its selected events.

    'response' may be an existing 'ArrApiResponse' or raw downloaded JSON
    mapping; 'config' is a 'pytuflow.arr.config.ArrConfig'. No network access
    occurs here when local temporal-pattern increment CSVs are configured.
    """
    from pytuflow.arr.api_client import ArrApiResponse
    from pytuflow.arr.engine import ArrEngine

    wrapped = response if isinstance(response, ArrApiResponse) else ArrApiResponse(cast(Any, response))
    engine = cast("_ArrRunner", ArrEngine(cast(Any, config), wrapped))
    return screen_arr_events(engine.run())


def screen_bom_ifd(
    imported_ifd: _BoMSource,
    *,
    temporal_patterns: _PatternProvider | None = None,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    default_losses: LossSpec | None = None,
    aeps: Collection[str] | None = None,
    durations_min: Collection[int] | None = None,
) -> tuple[PatternScreen, ...]:
    """Screen existing 'runoff_routing.bom_ifd.BoMIFD' using PyTUFLOW TPs.

    AEP strings remain exact (EY, percentage and '1 in N' are not interchangeable).
    Losses may be a fixed AEP entry or a duration-keyed AEP entry. Burst IL
    must already account for preburst, or derive it explicitly with
    'reduced_burst_initial_loss'; to screen full storms use 'screen_event'.
    """
    result: list[PatternScreen] = []
    for probability in imported_ifd.probabilities:
        name = probability.label
        if aeps is not None and name not in aeps:
            continue
        schedule = losses_by_aep.get(name, default_losses)
        for duration in imported_ifd.durations_min:
            if durations_min is not None and duration not in durations_min:
                continue
            if isinstance(schedule, Mapping):
                losses = schedule.get(duration)
            else:
                losses = schedule
            depth_mm = float(str(imported_ifd.depth_mm_at(duration, probability, convert_intensity=True)))
            # A reader/TP combination can lack patterns for some very short durations.
            # These events remain unresolved unless their depth is provably below IL.
            patterns: tuple[TemporalPattern, ...] = ()
            if (
                temporal_patterns is not None
                and losses is not None
                and depth_mm > losses.initial_loss_mm
                and losses.runoff_coefficient != 0
            ):
                try:
                    patterns = tuple(_pytuflow_pattern(p) for p in temporal_patterns.patterns(duration, name))
                except LookupError, ValueError:
                    pass
                except Exception as exc:  # noqa: BLE001 - isolate external TP provider
                    # A provider may raise its own domain-specific missing-pattern
                    # exception (PyTUFLOW uses ArrError). Preserve the event as
                    # unresolved rather than incorrectly declaring it zero.
                    result.append(
                        PatternScreen(
                            name,
                            duration,
                            None,
                            "bom_ifd",
                            None,
                            ScreeningStatus.UNRESOLVED,
                            None,
                            None,
                            f"Temporal patterns unavailable: {exc}",
                        )
                    )
                    continue
            event = DesignEvent(name, duration, depth_mm, losses, patterns, source="bom_ifd")
            result.extend(screen_event(event))
    return tuple(result)


def screen_bom_ifd_csv(
    path: str | Path,
    *,
    temporal_patterns: _PatternProvider | None = None,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    default_losses: LossSpec | None = None,
    aeps: Collection[str] | None = None,
    durations_min: Collection[int] | None = None,
) -> tuple[PatternScreen, ...]:
    """Load BoM All Design/IFD CSV with the existing runoff-routing reader.

    'python-runoff-routing' is an optional, in-development dependency. It is
    intentionally not made mandatory in 'ryan-tools'; install it in the
    calling environment or use 'screen_bom_ifd' with an imported BoMIFD.
    """
    try:
        reader_name: str = "read_bom_ifd_csv"
        reader = cast(
            "Callable[[Path], _BoMSource]",
            getattr(import_module("runoff_routing.bom_ifd"), reader_name),
        )
    except ImportError as exc:
        msg = (
            "BoM CSV import requires the existing python-runoff-routing BoM reader; "
            "install that package or pass its parsed object to screen_bom_ifd"
        )
        raise ImportError(msg) from exc
    source = reader(Path(path))
    return screen_bom_ifd(
        source,
        temporal_patterns=temporal_patterns,
        losses_by_aep=losses_by_aep,
        default_losses=default_losses,
        aeps=aeps,
        durations_min=durations_min,
    )
