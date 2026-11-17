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
from typing import Any, Literal, Protocol, cast


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
    continuing_loss_timing: Literal["fractional", "full_increment"] = "fractional"

    def __post_init__(self) -> None:
        _nonnegative(self.initial_loss_mm, "initial loss")
        if self.continuing_loss_timing not in ("fractional", "full_increment"):
            msg = "Continuing-loss timing must be fractional or full_increment"
            raise ValueError(msg)
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
    max_bin_excess_mm: float | None = None


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
    max_tp_total_excess_mm: float | None = None
    max_tp_bin_excess_mm: float | None = None
    max_total_pattern: str | None = None


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


def _rainfall_excess(steps: Sequence[tuple[float, float]], losses: LossSpec) -> tuple[float, float, float]:
    """Compute each TP bin's effective rain and aggregate it across the whole storm.

    Fractional CL timing uses the post-IL rainfall fraction of the crossing bin.
    Explicit full_increment timing instead applies the *whole* bin's CL capacity
    when IL is exhausted, following the separately researched experimental
    RORBwin 6.52 candidate. Neither profile has full native-model acceptance.
    """
    il_remaining = losses.initial_loss_mm
    total_excess = 0.0
    peak_rate = 0.0
    max_bin = 0.0
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
            continuing_fraction = 1.0 if losses.continuing_loss_timing == "full_increment" else post_il_fraction
            continuing_capacity = continuing_loss * dt / 60.0 * continuing_fraction
            excess = after_il - min(after_il, continuing_capacity)
        total_excess += excess
        max_bin = max(max_bin, excess)
        peak_rate = max(peak_rate, excess * 60.0 / dt)
    return total_excess, peak_rate, max_bin


def _event_excess(event: DesignEvent, pattern: TemporalPattern, losses: LossSpec) -> tuple[float, float, float]:
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
        max_bin: float | None = None,
    ) -> PatternScreen:
        return PatternScreen(
            event.aep, event.duration_min, pattern, event.source, event.scenario, status, excess, peak, reason, max_bin
        )

    if event.losses is None:
        return (make(ScreeningStatus.UNRESOLVED, "No losses supplied"),)
    losses = event.losses
    if total_depth <= losses.initial_loss_mm or losses.runoff_coefficient == 0:
        reason = (
            "Entire storm depth does not exceed initial loss"
            if total_depth <= losses.initial_loss_mm
            else "Runoff coefficient is zero"
        )
        # The bound certifies *every loaded pattern* without iterating its bins.
        # A missing TP set still yields one pattern-independent certificate;
        # reporting must not interpret that certificate as one evaluated TP.
        if event.patterns:
            return tuple(
                make(ScreeningStatus.ZERO, reason, pattern.identity, excess=0, peak=0, max_bin=0)
                for pattern in event.patterns
            )
        return (make(ScreeningStatus.ZERO, reason, excess=0, peak=0, max_bin=0),)
    if not event.patterns:
        return (make(ScreeningStatus.UNRESOLVED, "No applicable temporal patterns supplied"),)

    screened: list[PatternScreen] = []
    for pattern in event.patterns:
        try:
            excess, peak, max_bin = _event_excess(event, pattern, losses)
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
                max_bin,
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
        # A pattern-independent zero bound has no TP identity and counts as
        # zero evaluated TPs, not as an invented single selected pattern.
        zeros = sum(item.status == ScreeningStatus.ZERO and item.pattern_id is not None for item in group)
        status = (
            ScreeningStatus.UNRESOLVED
            if unresolved
            else ScreeningStatus.POSITIVE
            if positives
            else ScreeningStatus.ZERO
        )
        resolved = [item for item in group if item.excess_mm is not None]
        worst = max(resolved, key=lambda item: item.excess_mm or 0) if resolved else None
        max_bin_values = [item.max_bin_excess_mm for item in resolved if item.max_bin_excess_mm is not None]
        results.append(
            EventScreen(
                aep,
                duration,
                source,
                scenario,
                status,
                len(group),
                zeros,
                positives,
                unresolved,
                worst.excess_mm if worst is not None else None,
                max(max_bin_values) if max_bin_values else None,
                worst.pattern_id if worst is not None else None,
            )
        )
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


def reconcile_arr_full_storm_override(
    event: object,
    override: LossSpec,
    *,
    complete_storm: bool,
    source_preburst_ratio: float | None,
) -> LossSpec | None:
    """Convert a full-storm override to burst IL if PyTUFLOW omits a tiny preburst.

    With a complete preburst period the supplied loss is already full-storm IL.
    The PyTUFLOW engine may omit a preburst when its original ratio is <0.01,
    leaving a burst-only event. Subtract the omitted source preburst depth
    once in that case. An unsupported or indeterminate conversion returns
    None, so screening cannot certify a false zero-excess DROP.
    """
    if not complete_storm or getattr(event, "preburst", None) is not None:
        return override
    if source_preburst_ratio is None or not isfinite(source_preburst_ratio):
        return None
    if not 0 <= source_preburst_ratio < 0.01:
        return None
    point_raw = getattr(event, "depth_point", None)
    if not isinstance(point_raw, int | float) or not isfinite(float(point_raw)) or point_raw < 0:
        return None
    omitted_depth = float(point_raw) * source_preburst_ratio
    if not isfinite(omitted_depth) or omitted_depth > override.initial_loss_mm:
        return None
    return LossSpec(
        initial_loss_mm=max(0.0, override.initial_loss_mm - omitted_depth),
        continuing_loss_mm_per_hour=override.continuing_loss_mm_per_hour,
        runoff_coefficient=override.runoff_coefficient,
        continuing_loss_timing=override.continuing_loss_timing,
    )


def screen_arr_events(
    events: Iterable[object],
    *,
    runoff_coefficients_by_aep: Mapping[str, float] | None = None,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]] | None = None,
    default_losses: LossSpec | None = None,
    continuing_loss_timing: Literal["fractional", "full_increment"] = "fractional",
    complete_storm: bool = False,
    source_preburst_ratios: Mapping[tuple[str, int, str | None], float | None] | None = None,
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
        scenario = str(item.cc_scenario) if item.cc_scenario is not None else None
        if override is not None and complete_storm and item.preburst is None:
            ratio = (
                source_preburst_ratios.get((name, int(item.duration), scenario))
                if source_preburst_ratios is not None
                else None
            )
            adjusted = reconcile_arr_full_storm_override(
                item, override, complete_storm=complete_storm, source_preburst_ratio=ratio
            )
            if adjusted is None:
                screens.append(
                    PatternScreen(
                        name,
                        float(item.duration),
                        None,
                        "arr_datahub",
                        scenario,
                        ScreeningStatus.UNRESOLVED,
                        None,
                        None,
                        "Omitted preburst cannot be reconciled with full-storm IL override",
                    )
                )
                continue
            override = adjusted
        losses = (
            override
            if override is not None
            else LossSpec(
                float(item.initial_loss), runoff_coefficient=float(coeff), continuing_loss_timing=continuing_loss_timing
            )
            if coeff is not None
            else LossSpec(
                float(item.initial_loss),
                continuing_loss_mm_per_hour=float(item.continuing_loss),
                continuing_loss_timing=continuing_loss_timing,
            )
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


def _validate_arf_factor(value: float | None, *, aep: str, duration: int) -> float:
    if value is None or not isfinite(value) or not 0 <= value <= 1:
        msg = f"Missing or invalid ARF for {aep}/{duration} minutes"
        raise ValueError(msg)
    return value


def screen_bom_ifd(
    imported_ifd: _BoMSource,
    *,
    temporal_patterns: _PatternProvider | None = None,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    default_losses: LossSpec | None = None,
    aeps: Collection[str] | None = None,
    durations_min: Collection[int] | None = None,
    arf_by_event: Mapping[tuple[str, int], float] | None = None,
    rainfall_multiplier: float = 1.0,
) -> tuple[PatternScreen, ...]:
    """Screen existing 'runoff_routing.bom_ifd.BoMIFD' using PyTUFLOW TPs.

    AEP strings remain exact (EY, percentage and '1 in N' are not interchangeable).
    Losses may be a fixed AEP entry or a duration-keyed AEP entry. Burst IL
    must already account for preburst, or derive it explicitly with
    'reduced_burst_initial_loss'; to screen full storms use 'screen_event'.
    """
    if not isfinite(rainfall_multiplier) or rainfall_multiplier <= 0:
        msg = "Rainfall multiplier must be finite and positive"
        raise ValueError(msg)
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
            arf = _validate_arf_factor(
                arf_by_event.get((name, duration)) if arf_by_event is not None else 1.0,
                aep=name,
                duration=duration,
            )
            depth_mm = float(str(imported_ifd.depth_mm_at(duration, probability, convert_intensity=True)))
            depth_mm *= arf * rainfall_multiplier
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
    arf_by_event: Mapping[tuple[str, int], float] | None = None,
    rainfall_multiplier: float = 1.0,
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
        arf_by_event=arf_by_event,
        rainfall_multiplier=rainfall_multiplier,
    )
