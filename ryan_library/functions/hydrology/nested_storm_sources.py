"""Build IFD curves from saved BoM and ARR inputs without selecting ensemble TPs."""

from __future__ import annotations

import json
from bisect import bisect_left
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from importlib import import_module
from math import isfinite, log10
from pathlib import Path
from typing import Any, Protocol, cast

from .nested_storms import BomDepthSource, DepthAnchor, EventLosses, RainfallCurve, curve_from_bom


class ArrResponse(Protocol):
    """Public PyTUFLOW response interface, independent of TP selection."""

    def ifd_table(self, year: int) -> Mapping[str, Any]: ...

    def layer(self, name: str, required: bool = False) -> Mapping[str, Any] | None: ...

    def climate_change_rainfall_factors(
        self, baseline_year: int, ssp: str
    ) -> tuple[Sequence[float], Sequence[float]]: ...

    def climate_change_loss_factors(self, baseline_year: int, ssp: str) -> tuple[float, float]: ...


def read_arr_response(path: Path) -> ArrResponse:
    """Use PyTUFLOW's response object; never fall back to a network request."""
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = "ARR response must be a JSON object"
        raise ValueError(msg)
    constructor = cast(
        "Callable[[dict[str, Any]], ArrResponse]", import_module("pytuflow.arr.api_client").ArrApiResponse
    )
    return constructor(cast("dict[str, Any]", raw))


def read_bom_source(path: Path) -> BomDepthSource:
    """Use the established BoM CSV reader, not another CSV dialect parser."""
    reader = cast("Callable[[Path], BomDepthSource]", import_module("runoff_routing.bom_ifd").read_bom_ifd_csv)
    return reader(path)


def _exact_column(table: Mapping[str, Any], aep: str) -> dict[int, float]:
    """Select an exact numerical AEP column; never round or clamp probabilities."""
    if not aep.endswith("%"):
        msg = "ARR response AEP must be an exact percentage label, e.g. 1%"
        raise ValueError(msg)
    probability = float(aep[:-1])
    if not isfinite(probability) or not 0 < probability <= 100:
        msg = "Invalid AEP percentage"
        raise ValueError(msg)
    columns = cast("Sequence[object]", table["columns"])
    indices = cast("Sequence[object]", table["index"])
    rows = cast("Sequence[Sequence[object]]", table["data"])
    selected = [index for index, value in enumerate(columns) if float(str(value)) == probability]
    if len(selected) != 1 or len(indices) != len(rows):
        msg = f"Missing or ambiguous ARR AEP/duration table for {aep}"
        raise ValueError(msg)
    result: dict[int, float] = {}
    for raw_duration, row in zip(indices, rows, strict=True):
        duration = float(str(raw_duration))
        if not isfinite(duration) or duration <= 0 or not duration.is_integer() or int(duration) in result:
            msg = "ARR durations must be unique positive integer minutes"
            raise ValueError(msg)
        if len(row) != len(columns):
            msg = "ARR table row and column counts disagree"
            raise ValueError(msg)
        value = float(str(row[selected[0]]))
        if not isfinite(value) or value < 0:
            msg = "Invalid ARR table value"
            raise ValueError(msg)
        result[int(duration)] = value
    if not result:
        msg = "Empty ARR table"
        raise ValueError(msg)
    return result


def _interpolate_factor(value: float, axes: tuple[float, ...], factors: tuple[float, ...]) -> float:
    index = bisect_left(axes, value)
    if index == 0:
        return factors[0]
    if index == len(axes):
        return factors[-1]
    fraction = (value - axes[index - 1]) / (axes[index] - axes[index - 1])
    return factors[index - 1] + fraction * (factors[index] - factors[index - 1])


def _rainfall_factors(response: ArrResponse, durations: Sequence[int], climate: tuple[int, str]) -> dict[int, float]:
    """Match ArrEngine's log-duration interpolation of public source factors."""
    hours, factors = response.climate_change_rainfall_factors(*climate)
    pairs = sorted(zip(hours, factors, strict=True))
    if not pairs or any(
        not isfinite(hour) or hour <= 0 or not isfinite(factor) or factor <= 0 for hour, factor in pairs
    ):
        msg = "Invalid climate rainfall factors"
        raise ValueError(msg)
    axes = tuple(log10(hour) for hour, _ in pairs)
    values = tuple(factor for _, factor in pairs)
    if len(set(axes)) != len(axes):
        msg = "Duplicate climate factor duration"
        raise ValueError(msg)
    return {duration: _interpolate_factor(log10(duration / 60), axes, values) for duration in durations}


def arf_for_curve(
    response: ArrResponse | None,
    *,
    aep: str,
    durations: Sequence[int],
    area_km2: float,
    override: float | None = None,
) -> dict[int, float]:
    """Use PyTUFLOW ARF calculations, or explicitly override all factors."""
    if not isfinite(area_km2) or area_km2 < 0:
        msg = "Catchment area must be finite and nonnegative"
        raise ValueError(msg)
    if override is not None:
        if not isfinite(override) or not 0 <= override <= 1:
            msg = "ARF override must be within [0, 1]"
            raise ValueError(msg)
        return dict.fromkeys(durations, override)
    if area_km2 <= 1:
        return dict.fromkeys(durations, 1.0)
    if response is None:
        msg = "Calculated ARF for area >1 km² requires an ARR response"
        raise ValueError(msg)
    calculate = cast("Callable[..., Any]", import_module("pytuflow.arr.arf").arf_factors)
    params = response.layer("ARFParams", required=True)
    if params is None:
        msg = "ARR response lacks ARFParams"
        raise ValueError(msg)
    table = calculate(area_km2, durations, [aep], dict(params), arf_frequent=False, min_arf=0)
    return {duration: float(str(table.loc[duration, aep])) for duration in durations}


def _arf_notes(area_km2: float, factors: Mapping[int, float], override: float | None) -> tuple[str, ...]:
    notes: list[str] = []
    if override is not None:
        notes.append(f"Explicit ARF override: {override:g}")
    elif area_km2 > 1:
        notes.append("Calculated ARFs: verify AEP applicability (0.05%-50%) and unity fallbacks")
        if any(duration < 30 for duration in factors):
            notes.append("ARF anchors below 30 min are extrapolated")
        if area_km2 > 30000 or any(duration > 10080 for duration in factors):
            notes.append("ARF area/duration exceeds published generalised limits")
        if area_km2 > 1000 and any(duration <= 720 for duration in factors):
            notes.append("ARF short-duration area exceeds 1000 km²")
    if any(value == 0 for value in factors.values()):
        notes.append("Zero ARF: review physical plausibility")
    return tuple(notes)


def curve_from_arr_response(
    response: ArrResponse,
    aep: str,
    *,
    source: str,
    baseline_year: int = 2030,
    climate: tuple[int, str] | None = None,
    area_km2: float = 0,
    arf_override: float | None = None,
) -> RainfallCurve:
    """Use exact IFD rows without requiring TPs. CC factors multiply the 1990 IFD.

    Only exact source AEP/duration anchors are admitted. No TP downloads or
    event assembly are performed; loss selection is separate.
    """
    depths = _exact_column(response.ifd_table(1990 if climate is not None else baseline_year), aep)
    durations = tuple(sorted(depths))
    rain_factors = dict.fromkeys(durations, 1.0) if climate is None else _rainfall_factors(response, durations, climate)
    arfs = arf_for_curve(response, aep=aep, durations=durations, area_km2=area_km2, override=arf_override)
    anchors = tuple(
        DepthAnchor(duration, depths[duration], arfs[duration], rain_factors[duration]) for duration in durations
    )
    scenario = None if climate is None else f"{climate[0]}_{climate[1]}"
    return RainfallCurve(aep, source, anchors, scenario, _arf_notes(area_km2, arfs, arf_override))


def curve_from_bom_file(
    path: Path,
    aep: str,
    *,
    response: ArrResponse | None = None,
    area_km2: float = 0,
    arf_override: float | None = None,
    rainfall_factors: Mapping[int, float] | None = None,
    scenario: str | None = None,
) -> RainfallCurve:
    """Reuse the established BoM reader and the existing ARR ARF calculation."""
    ifd = read_bom_source(path)
    factors = arf_for_curve(response, aep=aep, durations=ifd.durations_min, area_km2=area_km2, override=arf_override)
    curve = curve_from_bom(
        ifd, aep, source=str(path), arf_by_duration=factors, rainfall_factors=rainfall_factors, scenario=scenario
    )
    return replace(curve, cautions=_arf_notes(area_km2, factors, arf_override))


def arr_preburst_ratio(response: ArrResponse, aep: str, duration_min: int, *, percentile: str) -> float:
    """Read an exact PB ratio to point depth; no extrapolation/interpolation.

    Recommended uses RecPreburst if present, otherwise Preburst50.
    """
    if percentile not in {"recommended", "10%", "25%", "50%", "75%", "90%"}:
        msg = "Unsupported ARR preburst percentile"
        raise ValueError(msg)
    layer = "RecPreburst" if percentile == "recommended" else f"Preburst{percentile[:-1]}"
    table = response.layer(layer)
    if table is None and percentile == "recommended":
        table = response.layer("Preburst50", required=True)
    if table is None:
        msg = f"ARR response lacks {layer}"
        raise ValueError(msg)
    return _exact_column(table, aep)[duration_min]


def apply_arr_loss_factors(response: ArrResponse, losses: EventLosses, climate: tuple[int, str]) -> EventLosses:
    """Apply source factors to baseline user-supplied IL/CL before PB reduction."""
    if losses.initial_loss_factor != 1 or losses.continuing_loss_factor != 1:
        msg = "Do not combine ARR climate loss factors with additional loss factors"
        raise ValueError(msg)
    il_factor, cl_factor = response.climate_change_loss_factors(*climate)
    return replace(losses, initial_loss_factor=il_factor, continuing_loss_factor=cl_factor)
