"""Coordinate BoM/ARR design-event rainfall loss screening and event-level evidence.

The wrapper owns editable settings, user-facing presentation, paths and console
banners. This orchestrator owns event assembly, screening dispatch, and numeric
evidence records used by the console and CSV reports. Source parsers, ARF/TP
selection and loss arithmetic remain in PyTUFLOW, runoff-routing, and shared
hydrology functions respectively. No solver is executed.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias, cast

from ...functions.hydrology.rainfall_loss_screening import (
    LossSpec,
    PatternScreen,
    reconcile_arr_full_storm_override,
    screen_arr_events,
    screen_bom_ifd,
)

RainfallRecord: TypeAlias = tuple[float, float, float, float | None, str]
RainfallRecordKey: TypeAlias = tuple[str, int, str | None]


@dataclass(frozen=True, slots=True)
class ScreeningBundle:
    """Per-pattern outcomes and event evidence used for audit/reporting."""

    patterns: tuple[PatternScreen, ...]
    rainfall: dict[RainfallRecordKey, RainfallRecord]
    pattern_sets: tuple[str, ...]


def _loss_label(loss: LossSpec | None, *, fallback: str) -> str:
    if loss is None:
        return fallback
    if loss.continuing_loss_mm_per_hour is not None:
        return f"IL {loss.initial_loss_mm:g}; CL {loss.continuing_loss_mm_per_hour:g}"
    return f"IL {loss.initial_loss_mm:g}; C {loss.runoff_coefficient:g}"


def _pattern_sets(patterns: Iterable[object]) -> tuple[str, ...]:
    """Read actual TP regions/sources, not inferred climatological region names."""
    found: set[str] = set()
    for source in patterns:
        p = cast(Any, source)
        kind = str(p.source)
        region = str(p.region) if p.region is not None else "unspecified region"
        found.add(f"{kind}: {region}")
    return tuple(sorted(found))


@dataclass(frozen=True, slots=True)
class TemporalPatternAdmission:
    """TP preflight across selected AEP bands, without repeating errors for every AEP."""

    included_durations: tuple[int, ...]
    omitted_durations: tuple[int, ...]
    missing_bands: dict[int, tuple[str, ...]]
    point_fallback_durations: tuple[int, ...]


class _TPSource(Protocol):
    @property
    def source(self) -> str: ...


def _areal_fallback_needed(temporal_patterns: Any, duration: int, area_km2: float) -> bool:
    """Distinguish missing areal TPs from the intended short-duration point TPs.

    PyTUFLOW deliberately uses point TPs below the shortest areal duration,
    even when areal data are present. These are NOT fallback events.
    """
    if area_km2 < 75:
        return False
    areal: object = getattr(temporal_patterns, "areal_tp", None)
    if areal is None:
        return True
    table = cast(Any, areal)
    if bool(table.empty):
        return True
    return duration >= int(str(table["duration"].min()))


def _probe_tp_bands(temporal_patterns: Any, duration: int, bands: Mapping[str, str]) -> tuple[tuple[str, ...], bool]:
    """Check one duration for every relevant AEP band; retain a single warning target."""
    from pytuflow.arr.exceptions import ArrError

    absent: list[str] = []
    used_point = False
    for band, representative in bands.items():
        try:
            candidates = cast("Sequence[object]", temporal_patterns.patterns(duration, representative, "aep"))
        except ArrError as exc:
            if "No point temporal pattern available" not in str(exc):
                raise
            absent.append(band)
            continue
        if not candidates:
            absent.append(band)
            continue
        used_point |= any(cast("_TPSource", item).source == "point" for item in candidates)
    return tuple(sorted(absent)), used_point


def preflight_temporal_pattern_durations(
    temporal_patterns: Any,
    *,
    durations: Collection[int],
    aeps: Collection[str],
    area_km2: float,
    allow_point_fallback: bool = True,
) -> TemporalPatternAdmission:
    """Exclude unscreenable durations across all selected AEPs, never marking them DROP.

    Delegate actual point-versus-areal pattern selection to PyTUFLOW, which
    implements area-bucket and duration fallback decisions.
    """
    from pytuflow.arr.temporal_patterns import aep_band

    bands: dict[str, str] = {}
    for aep in aeps:
        band = aep_band(aep, "aep")
        bands.setdefault(band, aep)
    if not bands:
        msg = "TP preflight requires at least one selected AEP"
        raise ValueError(msg)

    accepted: list[int] = []
    omitted: list[int] = []
    missing: dict[int, tuple[str, ...]] = {}
    fallback: list[int] = []
    for duration in sorted(set(durations)):
        absent, used_point = _probe_tp_bands(temporal_patterns, duration, bands)
        needs_fallback = used_point and _areal_fallback_needed(temporal_patterns, duration, area_km2)
        if absent:
            omitted.append(duration)
            missing[duration] = absent
        elif needs_fallback and not allow_point_fallback:
            omitted.append(duration)
            missing[duration] = ("areal unavailable; point fallback disabled",)
        else:
            accepted.append(duration)
            if needs_fallback:
                fallback.append(duration)
    return TemporalPatternAdmission(
        tuple(accepted),
        tuple(omitted),
        missing,
        tuple(fallback),
    )


def assemble_arr_events(
    events: Iterable[object],
    *,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    default_losses: LossSpec | None,
    continuing_loss_timing: Literal["fractional", "full_increment"],
    complete_storm: bool = False,
    source_preburst_ratios: Mapping[tuple[str, int, str | None], float | None] | None = None,
) -> ScreeningBundle:
    """Coordinate PyTUFLOW event outcomes and preserve point/areal ARF evidence."""
    sources = tuple(events)
    screens = screen_arr_events(
        sources,
        losses_by_aep=losses_by_aep,
        default_losses=default_losses,
        continuing_loss_timing=continuing_loss_timing,
        complete_storm=complete_storm,
        source_preburst_ratios=source_preburst_ratios,
    )
    totals: dict[RainfallRecordKey, RainfallRecord] = {}
    sets: set[str] = set()
    for source in sources:
        event = cast(Any, source)
        aep = str(event.aep_name)
        duration = int(event.duration)
        scenario = str(event.cc_scenario) if event.cc_scenario is not None else None
        overrides = losses_by_aep.get(aep)
        selected = overrides.get(duration) if isinstance(overrides, Mapping) else overrides
        losses = selected if selected is not None else default_losses
        fallback = f"ARR IL {float(event.initial_loss):g}; CL {float(event.continuing_loss):g}"
        pb = event.preburst
        ratio = source_preburst_ratios.get((aep, duration, scenario)) if source_preburst_ratios is not None else None
        applied = (
            reconcile_arr_full_storm_override(event, losses, complete_storm=complete_storm, source_preburst_ratio=ratio)
            if losses is not None
            else None
        )
        loss_label = (
            "UNRESOLVED full-storm IL / omitted preburst"
            if losses is not None and applied is None
            else _loss_label(applied, fallback=fallback)
        )
        if applied is not None and complete_storm and pb is None:
            loss_label += " (burst IL after omitted preburst)"
        totals[(aep, duration, scenario)] = (
            float(event.depth_point),
            float(event.depth_areal),
            float(event.arf),
            float(pb.depth) if pb is not None else None,
            loss_label,
        )
        sets.update(_pattern_sets(cast("Iterable[object]", event.patterns)))
    return ScreeningBundle(screens, totals, tuple(sorted(sets)))


def assemble_bom_events(
    imported_ifd: Any,
    *,
    aeps: Collection[str],
    durations: Collection[int],
    temporal_patterns: Any,
    losses_by_aep: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    default_losses: LossSpec | None,
    arf_by_event: Mapping[tuple[str, int], float],
    rainfall_multiplier: float,
    preburst_depth_mm: float | None,
) -> ScreeningBundle:
    """Apply ARFs and factors before a single pass through each selected BoM TP."""
    screens = screen_bom_ifd(
        imported_ifd,
        temporal_patterns=temporal_patterns,
        losses_by_aep=losses_by_aep,
        default_losses=default_losses,
        aeps=aeps,
        durations_min=durations,
        arf_by_event=arf_by_event,
        rainfall_multiplier=rainfall_multiplier,
    )
    totals: dict[RainfallRecordKey, RainfallRecord] = {}
    for source in cast("Iterable[Any]", imported_ifd.probabilities):
        aep = str(source.label)
        if aep not in aeps:
            continue
        schedule = losses_by_aep.get(aep, default_losses)
        for duration in durations:
            loss = schedule.get(duration) if isinstance(schedule, Mapping) else schedule
            point = float(imported_ifd.depth_mm_at(duration, source, convert_intensity=True))
            arf = arf_by_event[aep, duration]
            totals[(aep, duration, None)] = (
                point,
                point * arf * rainfall_multiplier,
                arf,
                preburst_depth_mm,
                _loss_label(loss, fallback="losses missing"),
            )
    sets = {": ".join(item.pattern_id.split(":", maxsplit=2)[:2]) for item in screens if item.pattern_id}
    return ScreeningBundle(screens, totals, tuple(sorted(sets)))
