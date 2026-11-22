"""Apply Data Hub climate-loss factors to user IL/CL without changing rainfall or preburst.

This module does not interpret ARR source tables or derive its own factors.
The caller supplies the pinned PyTUFLOW response's climate_change_loss_factors
method. Missing or invalid factor data fails closed, never reuses baseline losses.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from math import isfinite
from typing import Any, cast

from .rainfall_loss_screening import LossSpec

type ArrEventKey = tuple[str, int, str | None]
type ArrLossSchedules = Mapping[str, LossSpec | Mapping[int, LossSpec]]
type ArrFactorProvider = Callable[[int, str], tuple[float, float]]


@dataclass(frozen=True, slots=True)
class ArrUserLossAdjustment:
    """Scenario-specific user loss and its unrounded source-year provenance."""

    base: LossSpec
    effective: LossSpec
    reference_year: int
    il_multiplier: float
    cl_multiplier: float
    method: str


def _factor_pair(provider: ArrFactorProvider, year: int, ssp: str) -> tuple[float, float]:
    try:
        il, cl = provider(year, ssp)
        result = float(il), float(cl)
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        msg = f"ARR Data Hub climate loss factors unavailable for {year}_{ssp}: {exc}"
        raise ValueError(msg) from exc
    if any(not isfinite(value) or value <= 0 for value in result):
        msg = f"ARR Data Hub climate loss factors must be finite and positive for {year}_{ssp}: {result}"
        raise ValueError(msg)
    return result


def _scenario_multipliers(
    scenario: str | None,
    reference_year: int,
    provider: ArrFactorProvider,
    cache: dict[tuple[int, str], tuple[float, float]],
) -> tuple[float, float]:
    if scenario is None:
        return 1.0, 1.0
    match = re.fullmatch(r"(\\d{4})_(SSP[1235])", scenario)
    if match is None:
        msg = f"Unsupported ARR climate scenario label {scenario!r}; cannot adjust user losses."
        raise ValueError(msg)
    year, ssp = int(match[1]), match[2]
    for source_year in (year, *((2030,) if reference_year == 2030 else ())):
        key = source_year, ssp
        if key not in cache:
            cache[key] = _factor_pair(provider, source_year, ssp)
    numerator = cache[year, ssp]
    denominator = cache[2030, ssp] if reference_year == 2030 else (1.0, 1.0)
    adjusted = numerator[0] / denominator[0], numerator[1] / denominator[1]
    if not all(isfinite(value) and value > 0 for value in adjusted):
        msg = f"Invalid relative climate loss factors for {scenario} referenced to {reference_year}."
        raise ValueError(msg)
    return adjusted


def user_loss_adjustments(
    events: Iterable[object],
    *,
    default_losses: LossSpec | None,
    losses_by_aep: ArrLossSchedules,
    reference_year: int,
    factor_provider: ArrFactorProvider,
    complete_storm: bool,
) -> dict[ArrEventKey, ArrUserLossAdjustment]:
    """Return manually selected loss overrides per event, including unchanged baseline.

    The ARR Data Hub climate-loss factors are referenced to the historical
    1961-1990 loss estimates. A user input referenced to 2030 therefore uses
    the ratio of scenario-year to 2030 factors for the *same* SSP, not the
    full historical-to-scenario multiplier.

    In main-burst-only mode the supplied initial loss is already a burst loss:
    the IL multiplier is applied directly, not through PyTUFLOW's storm-IL
    minus climate-adjusted preburst derivation. The two methods are different.
    """
    if reference_year not in {1990, 2030}:
        msg = "User LOSS_REFERENCE_YEAR must be 1990 (historical) or 2030 (current)."
        raise ValueError(msg)
    result: dict[ArrEventKey, ArrUserLossAdjustment] = {}
    factor_cache: dict[tuple[int, str], tuple[float, float]] = {}
    for raw in events:
        event = cast(Any, raw)
        aep, duration = str(event.aep_name), int(event.duration)
        scenario = str(event.cc_scenario) if event.cc_scenario is not None else None
        selected = losses_by_aep.get(aep)
        base = selected.get(duration) if isinstance(selected, Mapping) else selected
        base = base if base is not None else default_losses
        if base is None:
            continue
        if scenario is not None and base.runoff_coefficient is not None:
            msg = (
                f"Climate scenario {scenario}: user IL + runoff coefficient has no validated "
                "climate-adjusted runoff-coefficient interpretation; refusing a potentially false DROP."
            )
            raise ValueError(msg)
        il_multiplier, cl_multiplier = _scenario_multipliers(
            scenario, reference_year, factor_provider, factor_cache
        )
        effective = LossSpec(
            initial_loss_mm=base.initial_loss_mm * il_multiplier,
            continuing_loss_mm_per_hour=(
                base.continuing_loss_mm_per_hour * cl_multiplier
                if base.continuing_loss_mm_per_hour is not None
                else None
            ),
            runoff_coefficient=base.runoff_coefficient,
            continuing_loss_timing=base.continuing_loss_timing,
        )
        key = aep, duration, scenario
        if key in result:
            msg = f"Duplicate ARR event with user loss selection: {key!r}"
            raise ValueError(msg)
        result[key] = ArrUserLossAdjustment(
            base, effective, reference_year, il_multiplier, cl_multiplier,
            "full_storm" if complete_storm else "direct_burst",
        )
    return result
