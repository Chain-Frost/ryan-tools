r"""Screen BoM/ARR design rainfall for loss-wiped events without running a hydraulic model.

Edit settings below or provide CLI overrides. This prints an AEP/duration
screening table, including how many selected temporal patterns are zero,
positive, or unresolved. It does not claim that positive rainfall excess
produces flow at an outlet, or that unexamined patterns can be omitted.

Examples:
    py -3.14 screen_rainfall_losses.py --source arr --latitude -23.08632 --longitude 119.2755 --view zero --no-pause
    py -3.14 screen_rainfall_losses.py --source bom --storm-mode main-burst-only --ifd "depths_hd4_temp_all_design.csv" --il 30 --cl 5 --no-pause
    py -3.14 screen_rainfall_losses.py --source arr --offline --arr-response site.json --point-tp point_Increments.csv --areal-tp areal_Increments.csv --no-pause
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-17.11"

WORKING_DIR: Path = Path(__file__).absolute().parent
SOURCE = "arr"  # "arr" downloads ARR Data Hub 2026; "bom" uses the original BoM IFD CSV.
IFD_CSV: Path | None = None
ARR_RESPONSE_JSON: Path | None = None  # Saved ARR Data Hub JSON overrides online API retrieval when set.
POINT_TP_CSV: Path | None = None  # PyTUFLOW ARR point *_Increments.csv; local TP overrides API download.
AREAL_TP_CSV: Path | None = None  # PyTUFLOW ARR areal *_Increments.csv; required offline if response has ArealTP URL.
OFFLINE = False  # True: disallow ALL Data Hub/API/TP downloads; require files needed for the chosen mode.
LATITUDE: float | None = None  # E.g. -23.08632
LONGITUDE: float | None = None  # E.g. 119.2755
SITE_NAME = "Rainfall screening"
CATCHMENT_AREA_KM2 = 0.0  # Zero uses point rainfall/TP. Set actual area for areal analysis.
IFD_BASELINE_YEAR = 2030  # ARR Data Hub: 1990 historical or 2030 current baseline.
LOSS_REFERENCE_YEAR = 1990  # Year of USER-supplied loss values: 1990 historical or 2030 current.
AEP_MODE = "common"  # "common" or "all"; AEP_SELECTION overrides either mode.
COMMON_AEPS: list[str] = [
    "50%",
    "20%",
    "10%",
    "5%",
    "2%",
    "1%",
    "0.5%",
    "0.2%",
    "1 in 200",
    "1 in 500",
]
AEP_SELECTION: list[str] = []  # Exact source labels; e.g. ["20%", "1%", "1 in 200"].
DURATION_SELECTION_MIN: list[int] = []  # Empty: every available duration from BoM CSV or ARR Data Hub IFD table.
ARR_DURATIONS_MIN: list[int] = []  # Optional ARR-only explicit override; empty uses Data Hub's IFD duration rows.
INITIAL_LOSS_MM: float | None = None  # None uses Data Hub's losses for ARR; BoM is unresolved without losses.
CONTINUING_LOSS_MM_PER_HOUR: float | None = None
RUNOFF_COEFFICIENT: float | None = None  # Post-IL fraction; mutually exclusive with continuing loss.
LOSS_TIMING = "full_increment"  # Experimental RORBwin 6.52 candidate; not TUFLOW-materials verified.
ARF_OVERRIDE: float | None = None  # Explicit constant ARF; None uses ARR/PyTUFLOW duration/AEP-specific ARFs.
RAINFALL_FACTOR = 1.0  # BoM-only manual rainfall multiplier (e.g. 1.15); not a Data Hub SSP climate scenario.
CLIMATE_SCENARIOS: list[str] = []  # ARR only: ["2050:SSP2", "2090:SSP5"]. Baseline is also calculated.
SHOW_MATRIX = True  # Print duration rows by AEP columns, separated by climate scenario.
SHOW_TP_DETAILS = False  # True prints one numerical excess row per evaluated temporal pattern.
# Per-AEP losses may also be duration-keyed, e.g. {"1%": {"60": {"il_mm": 25, "cl_mm_per_hour": 5}}}.
LOSS_OVERRIDES_BY_AEP: dict[str, object] = {}
LOSS_OVERRIDES_JSON: Path | None = None  # JSON object using the same per-AEP format as above.
# "complete" (default): preburst rainfall + main design burst; apply full storm IL once.
# "main-burst-only": omit explicit preburst; use an already preburst-adjusted burst IL.
STORM_MODE = "complete"
PREBURST_PERCENTILE = "recommended"  # "recommended", "10%", "25%", "50%", "75%", "90%".
BOM_PREBURST_DEPTH_MM: float | None = None  # Only explicit subtraction from a storm IL, NOT complete storm.
FETCH_TPS_FOR_BOM = True  # Fetch ARR point/areal patterns using BoM file's requested coordinates.
ALLOW_POINT_TP_FALLBACK = True  # Warn and use point TP if no matching areal TP exists.
VIEW = "all"  # "all", "zero" (only excluded), "keep" (positive or unresolved).
CSV_REPORT: Path | None = None  # Exclusive-create output; existing reports are never overwritten.
CONSOLE_LOG_LEVEL = "WARNING"
PAUSE_ON_FINISH = True

import argparse
import csv
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from importlib import import_module
from math import isfinite
from typing import Any, cast

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ryan_library.functions.hydrology.arr_user_climate_losses import ArrUserLossAdjustment, user_loss_adjustments
from ryan_library.functions.hydrology.rainfall_loss_screening import (
    LossSpec,
    PatternScreen,
    ScreeningStatus,
    reduced_burst_initial_loss,
    summarize_events,
)
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.hydrology.rainfall_loss_workflow import (
    TemporalPatternAdmission,
    assemble_arr_events,
    assemble_bom_events,
    preflight_temporal_pattern_durations,
)


def _path(value: Path | None, directory: Path) -> Path | None:
    if value is None:
        return None
    return (value if value.is_absolute() else directory / value).absolute()


def _error(message: str) -> ValueError:
    return ValueError(message)


def _choice(name: str, value: str, allowed: set[str]) -> str:
    if value not in allowed:
        message = f"{name}={value!r} must be one of {sorted(allowed)}."
        raise _error(message)
    return value


def _aep_selection(available: Sequence[str], chosen: Sequence[str], mode: str) -> list[str]:
    if len(set(available)) != len(available):
        message = "Source contains duplicate AEP labels."
        raise _error(message)
    if len(set(chosen)) != len(chosen):
        message = "AEP_SELECTION / --aep includes duplicate labels."
        raise _error(message)
    if chosen:
        unknown = [a for a in chosen if a not in available]
        if unknown:
            message = f"Requested AEP(s) not in the source: {unknown}. Available: {list(available)}"
            raise _error(message)
        return list(chosen)
    _choice("AEP_MODE", mode, {"common", "all"})
    matched = list(available) if mode == "all" else [a for a in available if a in COMMON_AEPS]
    if not matched:
        message = f"No AEPs matched AEP_MODE={mode!r}; use --aep-mode all or --aep."
        raise _error(message)
    return matched


def _loss_from_mapping(entry: object, *, loss_timing: str = "fractional") -> LossSpec:
    if not isinstance(entry, dict):
        message = "Each loss entry must be an object, e.g. {'il_mm':30,'cl_mm_per_hour':5}."
        raise _error(message)
    values = cast("dict[str, object]", entry)
    valid = {"il_mm", "cl_mm_per_hour", "runoff_coefficient"}
    if not set(values) <= valid or "il_mm" not in values:
        message = f"Loss entries require 'il_mm' and 'cl_mm_per_hour' or 'runoff_coefficient': {values}"
        raise _error(message)
    cl = values.get("cl_mm_per_hour")
    coefficient = values.get("runoff_coefficient")
    return LossSpec(
        float(str(values["il_mm"])),
        continuing_loss_mm_per_hour=float(str(cl)) if cl is not None else None,
        runoff_coefficient=float(str(coefficient)) if coefficient is not None else None,
        continuing_loss_timing=cast(Any, loss_timing),
    )


def _loss_schedules(raw: object, *, loss_timing: str = "fractional") -> dict[str, LossSpec | dict[int, LossSpec]]:
    if not isinstance(raw, dict):
        message = "LOSS_OVERRIDES_BY_AEP / --losses-json must contain a JSON object."
        raise _error(message)
    items = cast("dict[str, object]", raw)
    schedules: dict[str, LossSpec | dict[int, LossSpec]] = {}
    for aep, entry in items.items():
        if not aep.strip():
            message = "Loss AEP keys must be nonempty source labels."
            raise _error(message)
        if not isinstance(entry, dict):
            message = f"AEP {aep!r} must contain a loss object or duration-keyed loss objects."
            raise _error(message)
        data = cast("dict[str, object]", entry)
        if "il_mm" in data:
            schedules[aep] = _loss_from_mapping(data, loss_timing=loss_timing)
        else:
            durations: dict[int, LossSpec] = {}
            for key, value in data.items():
                if not key.isdecimal() or int(key) <= 0 or str(int(key)) != key:
                    message = f"Invalid duration key {key!r} for {aep!r}; use e.g. '60' in minutes."
                    raise _error(message)
                durations[int(key)] = _loss_from_mapping(value, loss_timing=loss_timing)
            if not durations:
                message = f"No losses specified for {aep!r}."
                raise _error(message)
            schedules[aep] = durations
    return schedules


def _default_loss(
    il: float | None, cl: float | None, coefficient: float | None, *, loss_timing: str = "fractional"
) -> LossSpec | None:
    if il is None:
        if cl is not None or coefficient is not None:
            message = "INITIAL_LOSS_MM / --il is required if --cl or --coefficient is supplied."
            raise _error(message)
        return None
    return LossSpec(
        il,
        continuing_loss_mm_per_hour=cl,
        runoff_coefficient=coefficient,
        continuing_loss_timing=cast(Any, loss_timing),
    )


def _apply_bom_preburst(
    default: LossSpec | None,
    schedules: Mapping[str, LossSpec | Mapping[int, LossSpec]],
    depth: float | None,
) -> tuple[LossSpec | None, dict[str, LossSpec | dict[int, LossSpec]]]:
    if depth is None:
        return default, {key: dict(value) if isinstance(value, Mapping) else value for key, value in schedules.items()}

    def adjust(loss: LossSpec) -> LossSpec:
        burst_il = reduced_burst_initial_loss(loss.initial_loss_mm, depth)
        return LossSpec(
            burst_il,
            continuing_loss_mm_per_hour=loss.continuing_loss_mm_per_hour,
            runoff_coefficient=loss.runoff_coefficient,
            continuing_loss_timing=loss.continuing_loss_timing,
        )

    selected: dict[str, LossSpec | dict[int, LossSpec]] = {}
    for aep, schedule in schedules.items():
        selected[aep] = (
            {duration: adjust(loss) for duration, loss in schedule.items()}
            if isinstance(schedule, Mapping)
            else adjust(schedule)
        )
    if default is None and not selected:
        message = "BOM_PREBURST_DEPTH_MM requires storm IL/CL or IL+coefficient inputs."
        raise _error(message)
    return adjust(default) if default is not None else None, selected


def _durations(available: Sequence[int], chosen: Sequence[int], *, arr: bool) -> list[int]:
    selected = list(chosen) if chosen else list(ARR_DURATIONS_MIN) if arr and ARR_DURATIONS_MIN else list(available)
    if not selected or len(selected) != len(set(selected)) or any(t <= 0 for t in selected):
        message = "Duration selection must have unique positive integer minutes."
        raise _error(message)
    unknown = [t for t in selected if t not in available]
    if unknown:
        label = "ARR IFD" if arr else "BoM file"
        message = f"Duration(s) not in {label}: {unknown}. Available: {list(available)}"
        raise _error(message)
    return selected


def _arr_config(
    latitude: float | None,
    longitude: float | None,
    area: float,
    site: str,
    aeps: list[str],
    durations: list[int],
    baseline: int,
    mode: str,
    percentile: str,
    directory: Path,
    json_path: Path | None,
    *,
    point_tp_path: Path | None = None,
    areal_tp_path: Path | None = None,
    climate_scenarios: Sequence[tuple[int, str]] = (),
    arf_override: float | None = None,
    user_losses: LossSpec | None = None,
) -> Any:
    from pytuflow.arr.config import ArrConfig

    _choice("STORM_MODE", mode, {"complete", "main-burst-only"})
    if baseline not in {1990, 2030}:
        message = "IFD_BASELINE_YEAR must be 1990 or 2030."
        raise _error(message)
    if not isfinite(area) or area < 0:
        message = "CATCHMENT_AREA_KM2 must be finite and nonnegative."
        raise _error(message)
    if (latitude is None) != (longitude is None):
        message = "LATITUDE and LONGITUDE must either both be specified or both omitted."
        raise _error(message)
    if latitude is not None and (not isfinite(latitude) or not -90 <= latitude <= 90):
        message = "LATITUDE must be between -90 and 90."
        raise _error(message)
    if longitude is not None and (not isfinite(longitude) or not -180 <= longitude <= 180):
        message = "LONGITUDE must be between -180 and 180."
        raise _error(message)
    data: dict[str, object] = {
        "site": {"name": site, "latitude": latitude, "longitude": longitude, "catchment_area": area},
        "ifd": {"baseline_year": baseline},
        "events": {"aep": aeps, "duration": durations, "output_notation": "aep"},
        "preburst": {"percentile": percentile, "pattern_method": "recommended"},
        "complete_storm": mode == "complete",
        "climate_change": {
            "enabled": bool(climate_scenarios),
            "scenarios": [{"baseline_year": year, "ssp": ssp} for year, ssp in climate_scenarios],
        },
        "arf": {"arf_override": arf_override},
        "output": {"path": str(directory)},
        "temporal_patterns": {
            "point_tp_csv": str(point_tp_path) if point_tp_path is not None else None,
            "areal_tp_csv": str(areal_tp_path) if areal_tp_path is not None else None,
        },
    }
    if user_losses is not None:
        if user_losses.continuing_loss_mm_per_hour is None:
            message = "PyTUFLOW native user losses require IL/CL, not a runoff coefficient."
            raise _error(message)
        data["losses"] = {
            "user_initial_loss": user_losses.initial_loss_mm,
            "user_continuing_loss": user_losses.continuing_loss_mm_per_hour,
            "climate_change_method": "storm",
        }
    if json_path is not None:
        data["response_json"] = str(json_path)
    method_name: str = "from_dict"
    return getattr(ArrConfig, method_name)(data)


def _validate_local_sources(
    *,
    source: str,
    offline: bool,
    response_path: Path | None,
    point_tp_path: Path | None,
    areal_tp_path: Path | None,
    fetch_bom_tps: bool,
) -> None:
    """Validate source precedence and fail before PyTUFLOW can download in offline mode."""
    for label, path in (
        ("ARR_RESPONSE_JSON", response_path),
        ("POINT_TP_CSV", point_tp_path),
        ("AREAL_TP_CSV", areal_tp_path),
    ):
        if path is not None and not path.is_file():
            message = f"{label} requires an existing readable file: {path}"
            raise _error(message)
    if not offline:
        return
    if source == "arr":
        if response_path is None:
            message = "OFFLINE ARR mode requires --arr-response / ARR_RESPONSE_JSON containing saved Data Hub JSON."
            raise _error(message)
        if point_tp_path is None:
            message = "OFFLINE ARR mode requires --point-tp / POINT_TP_CSV (local point increments CSV)."
            raise _error(message)
    elif point_tp_path is None and fetch_bom_tps:
        message = (
            "OFFLINE BoM mode cannot fetch ARR temporal patterns: supply --point-tp "
            "or --no-fetch-tps (the latter leaves events above IL unresolved)."
        )
        raise _error(message)


def _validate_offline_arr_patterns(
    response: Any,
    *,
    offline: bool,
    areal_tp_path: Path | None,
    allow_point_fallback: bool = False,
) -> bool:
    """Indicate if an advertised areal TP URL must be suppressed in offline mode."""
    if not offline or areal_tp_path is not None:
        return False
    areal_layer = response.layer("ArealTP")
    if not areal_layer or not areal_layer.get("url"):
        return False
    if allow_point_fallback:
        return True
    message = (
        "OFFLINE ARR response advertises an areal TP download URL; supply a local "
        "--areal-tp file or enable --point-tp-fallback."
    )
    raise _error(message)


def _response_without_areal_tp(response: Any) -> Any:
    """Copy the Data Hub response without its areal TP download link; do not edit source JSON."""
    from pytuflow.arr.api_client import ArrApiResponse

    raw = cast("dict[str, Any]", response.raw)
    layers = cast("dict[str, Any]", raw["layers"])
    retained = dict(layers)
    retained.pop("ArealTP", None)
    return ArrApiResponse({**raw, "layers": retained})


def _announce_tp_admission(admission: TemporalPatternAdmission, *, area: float) -> None:
    """Emit one warning for all unsupported durations and another for point fallback."""
    console = Console(stderr=True)
    if admission.omitted_durations:
        durations = ", ".join(str(value) for value in admission.omitted_durations)
        console.print(f"WARNING: Missing TPs for {durations} min; skipped for all AEPs (NOT DROP).")
    if admission.point_fallback_durations:
        durations = ", ".join(str(value) for value in admission.point_fallback_durations)
        console.print(
            f"WARNING: Areal TPs unavailable for {durations} min at {area:g} km²; "
            "using point TPs instead (review suitability)."
        )


def _arr_response(config: Any, json_path: Path | None) -> Any:
    from pytuflow.arr.api_client import ArrApiClient, ArrApiResponse

    if json_path is not None:
        raw: object = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            message = "ARR response JSON must have a JSON object at its root."
            raise _error(message)
        return ArrApiResponse(raw)
    return ArrApiClient().fetch(config)


def _datahub_aeps(response: Any, baseline: int) -> list[str]:
    table = cast("dict[str, object]", response.ifd_table(baseline))
    columns = table.get("columns")
    if not isinstance(columns, list):
        message = "ARR response has no RecIFD AEP columns for selected baseline."
        raise _error(message)
    return [f"{float(str(column)):g}%" for column in cast("list[object]", columns)]


def _datahub_durations(response: Any, baseline: int) -> list[int]:
    """Preserve source-provided IFD duration rows rather than a fixed curated list."""
    table = cast("dict[str, object]", response.ifd_table(baseline))
    raw = table.get("index")
    if not isinstance(raw, list) or not raw:
        message = "ARR response RecIFD has no duration rows."
        raise _error(message)
    durations: list[int] = []
    for item in cast("list[object]", raw):
        numeric = float(str(item))
        if not isfinite(numeric) or int(numeric) != numeric or numeric <= 0:
            message = f"Invalid ARR IFD duration in minutes: {item!r}"
            raise _error(message)
        durations.append(int(numeric))
    return _durations(durations, [], arr=False)


def _parse_climate_scenarios(values: Sequence[str]) -> tuple[tuple[int, str], ...]:
    """Delegate real SSP rainfall/loss adjustments to PyTUFLOW's climate engine."""
    parsed: list[tuple[int, str]] = []
    for item in values:
        try:
            year_text, ssp_text = item.split(":", maxsplit=1)
            year = int(year_text)
            ssp = ssp_text.upper()
        except ValueError as exc:
            message = f"Climate scenario {item!r} must look like 2050:SSP2."
            raise _error(message) from exc
        if year not in range(2030, 2101, 10) or ssp not in {"SSP1", "SSP2", "SSP3", "SSP5"}:
            message = f"Unsupported climate scenario {item!r}; use 2030..2100 by 10 and SSP1/2/3/5."
            raise _error(message)
        if (year, ssp) in parsed:
            message = f"Duplicate climate scenario {item!r}."
            raise _error(message)
        parsed.append((year, ssp))
    return tuple(parsed)


def _source_decimals(values: Sequence[float]) -> int:
    """Use the maximum meaningful decimal precision in the selected source depths."""
    if not values:
        return 0
    try:
        counts = [max(0, -int(Decimal(str(value)).normalize().as_tuple().exponent)) for value in values]
    except InvalidOperation as exc:
        message = "Rainfall depths must be valid finite decimals."
        raise _error(message) from exc
    return min(6, max(counts))


def _region_label(screens: Sequence[PatternScreen]) -> str:
    """Use the pattern loader's exact source/region label; never infer from coordinates."""
    found = sorted(
        {
            ":".join(item.pattern_id.split(":", maxsplit=2)[:2])
            for item in screens
            if item.pattern_id is not None and ":" in item.pattern_id
        }
    )
    return ", ".join(found) if found else "Not resolved (pattern-independent bound or missing TP data)"


def _decision_cell(status: ScreeningStatus) -> Text:
    """Use the same visible decision colours in detailed and matrix summaries."""
    if status == ScreeningStatus.ZERO:
        return Text("DROP", style="bold green")
    if status == ScreeningStatus.POSITIVE:
        return Text("KEEP", style="bold yellow")
    return Text("UNRESOLVED", style="bold red")


def _arf_cell(arf: float, *, flagged: bool = False) -> Text:
    """Grey-to-white ARF intensity: stronger area reduction is darker."""
    if not isfinite(arf) or not 0 <= arf <= 1:
        return Text("—", style="grey50")
    shade = round(110 + 145 * arf)
    suffix = "*" if flagged else ""
    return Text(f"{arf:.4f}{suffix}", style=f"#{shade:02x}{shade:02x}{shade:02x}")


def _arf_area_duration_cautions(area: float, duration: int) -> tuple[str, ...]:
    """Separate published area/duration applicability from AEP handling."""
    notes: list[str] = []
    if area > 1000 and duration <= 720:
        notes.append("SHORT_AREA_GT_1000_UNSUPPORTED")
    if area > 30000:
        notes.append("AREA_GT_30000_UNSUPPORTED")
    if duration > 10080:
        notes.append("DURATION_GT_168_HOURS_UNSUPPORTED")
    return tuple(notes)


def _arf_policy_cautions(
    totals: Mapping[tuple[str, int, str | None], tuple[float, float, float, float | None, str]],
    *,
    area: float,
    override: float | None,
) -> dict[tuple[str, int, str | None], tuple[str, ...]]:
    """Identify unsupported ARF inputs without silently changing PyTUFLOW factors."""
    from pytuflow.arr.arf import aep_name_to_pct

    cautions: dict[tuple[str, int, str | None], tuple[str, ...]] = {}
    for key, (_, _, arf, _, _) in totals.items():
        aep, duration, _scenario = key
        notes: list[str] = []
        if area > 1 and override is None:
            percent = float(aep_name_to_pct(aep))
            if percent > 50:
                notes.append("AEP_GT_50_UNITY" if arf == 1 else "AEP_GT_50_UNSUPPORTED")
            elif percent < 0.05:
                notes.append("AEP_LT_0_05_UNITY" if arf == 1 else "AEP_LT_0_05_UNSUPPORTED")
            elif duration < 30:
                notes.append("DURATION_LT_30_EXTRAPOLATED")
            notes.extend(_arf_area_duration_cautions(area, duration))
        if arf == 0:
            notes.append("ZERO_ARF_REVIEW")
        if notes:
            cautions[key] = tuple(notes)
    return cautions


def _show_arf_policy_warnings(cautions: Mapping[tuple[str, int, str | None], tuple[str, ...]]) -> None:
    """Consolidate Ladson/ARR cautions before the table, rather than per row."""
    console = Console()
    frequent = sorted({a for (a, _, _), flags in cautions.items() if "AEP_GT_50_UNITY" in flags})
    if frequent:
        console.print(
            "WARNING: ARF not applied for "
            + ", ".join(frequent)
            + " (>50% AEP); 1.0000 is a unity fallback, NOT a calculated factor. "
            "Ladson recommends checking any extension rather than assuming validity."
        )
    rare = sorted({a for (a, _, _), flags in cautions.items() if "AEP_LT_0_05_UNITY" in flags})
    if rare:
        console.print("WARNING: ARF not applied below 0.05% AEP for " + ", ".join(rare) + "; 1.0000 is a fallback.")
    unsupported = sorted(
        {
            a
            for (a, _, _), flags in cautions.items()
            if "AEP_GT_50_UNSUPPORTED" in flags or "AEP_LT_0_05_UNSUPPORTED" in flags
        }
    )
    if unsupported:
        console.print("WARNING: AEP outside ARR ARF equation limits with a non-unity factor: " + ", ".join(unsupported))
    short = sorted({d for (_, d, _), flags in cautions.items() if "DURATION_LT_30_EXTRAPOLATED" in flags})
    if short:
        console.print(
            "WARNING: ARF at "
            + ", ".join(str(d) for d in short)
            + " minutes is extrapolated below the 30-minute derivation limit. "
            "Check realism; do NOT automatically exclude flagged DROP events."
        )
    big_short = sorted({d for (_, d, _), flags in cautions.items() if "SHORT_AREA_GT_1000_UNSUPPORTED" in flags})
    if big_short:
        console.print(
            "WARNING: short-duration ARF outside the 1000-km² area limit at "
            + ", ".join(str(d) for d in big_short)
            + " minutes."
        )
    if any("AREA_GT_30000_UNSUPPORTED" in flags for flags in cautions.values()):
        console.print("WARNING: catchment area exceeds the 30,000-km² ARF equation limit.")
    if any("DURATION_GT_168_HOURS_UNSUPPORTED" in flags for flags in cautions.values()):
        console.print("WARNING: duration exceeds the 168-hour ARF equation limit.")
    zero = sorted({f"{a}/{d}min" for (a, d, _), flags in cautions.items() if "ZERO_ARF_REVIEW" in flags})
    if zero:
        console.print(
            "WARNING: ARF = 0 for "
            + ", ".join(zero)
            + ". PyTUFLOW floors negative ARF values at zero (or zero may be explicitly selected). "
            "Check physical realism and downstream zero-rainfall handling before using DROP."
        )


def _arr_source_preburst_ratios(
    response: Any, events: Sequence[object], percentile: str
) -> dict[tuple[str, int, str | None], float | None]:
    """Read the source ARR ratio using PyTUFLOW's existing interpolation code.

    The helper is version-bound and internal to PyTUFLOW, as used by its own
    complete-storm engine. Missing source values are not silently zero-filled.
    """
    from pytuflow.arr.arf import aep_name_to_pct
    from pytuflow.arr.exceptions import ArrError

    method = "_preburst_ratio"
    calculator = cast("Callable[..., Any]", getattr(import_module("pytuflow.arr.complete_storm"), method))
    ratios: dict[tuple[str, int, str | None], float | None] = {}
    for item in events:
        event = cast(Any, item)
        aep = str(event.aep_name)
        duration = int(event.duration)
        scenario = str(event.cc_scenario) if event.cc_scenario is not None else None
        try:
            ratio = float(calculator(response, percentile, duration, float(aep_name_to_pct(aep))))
        except ArrError:
            ratios[aep, duration, scenario] = None
            continue
        if not isfinite(ratio) or ratio < 0:
            message = f"Invalid ARR source preburst ratio for {aep}/{duration} min: {ratio}"
            raise _error(message)
        ratios[aep, duration, scenario] = ratio
    return ratios


def _show_matrix(screens: Sequence[PatternScreen], *, enabled: bool) -> None:
    """Summarise keep/drop decisions by duration and AEP, one matrix per scenario."""
    if not enabled:
        return
    all_events = summarize_events(screens)
    if not all_events:
        return
    labels = list(dict.fromkeys(event.aep for event in all_events))
    scenarios = list(dict.fromkeys(event.scenario for event in all_events))
    console = Console()
    for scenario in scenarios:
        group = [event for event in all_events if event.scenario == scenario]
        durations = sorted({int(event.duration_min) for event in group})
        indexed = {(int(event.duration_min), event.aep): event.status for event in group}
        matrix = Table(title=f"Duration by AEP event status — {scenario or 'baseline'}")
        matrix.add_column("Duration\n(min)", justify="right")
        for label in labels:
            matrix.add_column(label, justify="center")
        for duration in durations:
            cells: list[str | Text] = []
            for aep in labels:
                value = indexed.get((duration, aep))
                cells.append(
                    Text("?", style="bold red")
                    if value == ScreeningStatus.UNRESOLVED
                    else _decision_cell(value)
                    if value is not None
                    else Text("—", style="grey50")
                )
            matrix.add_row(str(duration), *cells)
        console.print(matrix)
    console.print("Matrix: [green]DROP[/green] = zero excess; [yellow]KEEP[/yellow] = positive excess; ? = unresolved.")


def _bom_patterns(
    source: Any,
    latitude: float | None,
    longitude: float | None,
    area: float,
    fetch_tps: bool,
    point_csv: Path | None,
    areal_csv: Path | None,
    directory: Path,
    response: Any | None = None,
    allow_point_fallback: bool = True,
) -> Any:
    from pytuflow.arr.temporal_patterns import TemporalPatternSet

    if point_csv is not None:
        if area >= 75 and areal_csv is None and not allow_point_fallback:
            message = "No local areal TP increments CSV and point TP fallback disabled."
            raise _error(message)
        return TemporalPatternSet.from_files(
            str(point_csv), str(areal_csv) if areal_csv is not None else None, catchment_area=area
        )
    if not fetch_tps:
        return None
    lat = latitude if latitude is not None else float(source.latitude) if source.latitude is not None else None
    lon = longitude if longitude is not None else float(source.longitude) if source.longitude is not None else None
    if lat is None or lon is None:
        message = "BoM IFD has no requested coordinates; specify --latitude and --longitude or --no-fetch-tps."
        raise _error(message)
    config = _arr_config(
        lat, lon, area, SITE_NAME, ["1%"], [60], 2030, "main-burst-only", "recommended", directory, None
    )
    selected_response = response if response is not None else _arr_response(config, None)
    point = selected_response.layer("PointTP", required=True)
    areal = selected_response.layer("ArealTP")
    areal_url = areal["url"] if areal else None
    try:
        return TemporalPatternSet.from_sources(
            None,
            str(areal_csv) if areal_csv is not None else None,
            point["url"],
            areal_url,
            catchment_area=area,
        )
    except Exception as exc:
        if (
            not allow_point_fallback
            or areal_csv is not None
            or areal_url is None
            or "Failed to download temporal pattern zip" not in str(exc)
            or areal_url not in str(exc)
        ):
            raise
        Console(stderr=True).print("WARNING: Could not download areal TP increments. Using point TPs instead.")
        return TemporalPatternSet.from_sources(None, None, point["url"], None, catchment_area=area)


def _bom_arf_factors(
    *,
    area: float,
    durations: Sequence[int],
    aeps: Sequence[str],
    response: Any | None,
    override: float | None,
) -> dict[tuple[str, int], float]:
    """Use the existing PyTUFLOW ARR ARF implementation on gross BoM point depths."""
    if not isfinite(area) or area < 0:
        message = "CATCHMENT_AREA_KM2 must be finite and >=0; 0 means point rainfall only."
        raise _error(message)
    if override is not None and (not isfinite(override) or not 0 <= override <= 1):
        message = "ARF_OVERRIDE / --arf-override must be within [0, 1]."
        raise _error(message)
    if override is not None or area <= 1:
        value = override if override is not None else 1.0
        return {(aep, duration): value for aep in aeps for duration in durations}
    if response is None:
        message = "BoM ARF for area >1 km² requires ARR ARFParams from a saved JSON or a Data Hub request."
        raise _error(message)
    # PyTUFLOW's legacy ARF callable has untyped dict annotations; cast at the
    # external boundary rather than importing a partially unknown signature.
    arf_method: str = "arf_factors"
    arf_calc = cast("Callable[..., Any]", getattr(import_module("pytuflow.arr.arf"), arf_method))
    params = response.layer("ARFParams", required=True)
    factors = arf_calc(area, durations, aeps, params, arf_frequent=False, min_arf=0)
    return {(aep, duration): float(str(factors.loc[duration, aep])) for aep in aeps for duration in durations}


def _print_provenance(
    *,
    latitude: float | None,
    longitude: float | None,
    site: str,
    area: float,
    arf_source: str,
    screens: Sequence[PatternScreen],
    loss_timing: str,
    climate: str,
    pattern_sets: Sequence[str] = (),
    arf_cautions: Mapping[tuple[str, int, str | None], tuple[str, ...]] | None = None,
) -> None:
    console = Console()
    console.print(f"Site: {site}")
    if latitude is not None and longitude is not None:
        console.print(f"Requested coordinates (WGS84): {latitude:.6f}, {longitude:.6f}")
    else:
        console.print("Requested coordinates: not provided; using the selected saved source")
    console.print(f"Catchment area: {area:g} km²; ARF: {arf_source}; loss timing: {loss_timing}; climate: {climate}")
    if arf_cautions is not None:
        _show_arf_policy_warnings(arf_cautions)
    elif area > 1 and not arf_source.startswith("explicit factor") and any(item.aep == "63.2%" for item in screens):
        console.print("WARNING: ARF not applied at 63.2% AEP (>50%); ARF 1.0000 is a unity fallback, not calculated.")
    if area > 1 and not arf_source.startswith("explicit factor"):
        if any(720 < item.duration_min < 1440 for item in screens):
            console.print("ARF duration interpolation (12-24 h): ARR Eq. 2.4.3 (Ladson Table 2.4.1 correction).")
        if area < 10:
            console.print("ARF area interpolation (1-10 km²): ARR Eq. 2.4.4.")
    console.print(f"Temporal pattern set(s): {', '.join(pattern_sets) if pattern_sets else _region_label(screens)}")


def _print_tp_details(screens: Sequence[PatternScreen], *, enabled: bool) -> None:
    if not enabled:
        return
    table = Table(title="Per-pattern applied excess rainfall")
    for heading in ("AEP", "Duration", "Scenario", "TP identity", "Decision", "Total excess (mm)", "Max bin (mm)"):
        table.add_column(heading)
    for item in screens:
        table.add_row(
            item.aep,
            f"{item.duration_min:g}",
            item.scenario or "baseline",
            item.pattern_id or "pattern-independent bound",
            _decision_cell(item.status),
            _excess_cell(item.excess_mm),
            _excess_cell(item.max_bin_excess_mm),
        )
    Console().print(table)


def _excess_cell(value: float | None, *, places: int = 4) -> Text:
    """Grey only for exact zero; a rounded small positive remains visibly nonzero."""
    if value is None or not isfinite(value):
        return Text("—", style="grey50")
    if value == 0:
        return Text(f"{0:.{places}f}", style="grey50")
    if 0 < value < 0.5 * 10 ** (-places):
        return Text(f"<{10 ** (-places):.{places}f}", style="white")
    return Text(f"{value:.{places}f}", style="white")


def _print_report(
    screens: Sequence[PatternScreen],
    totals: Mapping[tuple[str, int, str | None], tuple[float, float, float, float | None, str]],
    view: str,
    csv_report: Path | None,
    *,
    source_decimal_places: int | None = None,
    arf_cautions: Mapping[tuple[str, int, str | None], tuple[str, ...]] | None = None,
    source_preburst_ratios: Mapping[tuple[str, int, str | None], float | None] | None = None,
    loss_adjustments: Mapping[tuple[str, int, str | None], ArrUserLossAdjustment] | None = None,
    native_user_loss: bool = False,
    source_kind: str = "unknown",
) -> None:
    """Compact decision report with source-adjusted depths and separated AEP groups."""
    _choice("VIEW", view, {"all", "zero", "keep"})
    events = summarize_events(screens)
    selected = [
        event
        for event in events
        if view == "all"
        or (view == "zero" and event.status == ScreeningStatus.ZERO)
        or (view == "keep" and event.status != ScreeningStatus.ZERO)
    ]
    source_places = (
        source_decimal_places
        if source_decimal_places is not None
        else _source_decimals([item[0] for item in totals.values()])
    )
    adjusted_places = max(
        source_places,
        2 if any(abs(item[1] - item[0]) > 1e-9 for item in totals.values()) else 0,
    )
    show_unknown = any(event.unresolved_patterns > 0 for event in selected)
    show_scenario = any(event.scenario is not None for event in selected)
    table = Table(
        title="Design rainfall loss screening (selected temporal patterns)",
        box=box.SQUARE,
        show_lines=False,
        padding=(0, 1),
    )
    table.add_column("Decision", no_wrap=True)
    table.add_column("AEP", no_wrap=True)
    table.add_column("Dur\n(min)", justify="right")
    if show_scenario:
        table.add_column("CC", no_wrap=True)
    table.add_column("Point\nIFD mm", justify="right")
    table.add_column("ARF", justify="right")
    table.add_column("Rain\nmm", justify="right")
    table.add_column("IL / CL", no_wrap=True)
    table.add_column("PB\nmm", justify="right")
    table.add_column("PB\nratio", justify="right")
    table.add_column("Zero\nTP", justify="right")
    table.add_column("Pos\nTP", justify="right")
    if show_unknown:
        table.add_column("Unknown\nTP", justify="right")
    table.add_column("Max TP\nsum mm", justify="right")
    table.add_column("Max\nbin mm", justify="right")

    csv_records: list[list[str]] = []
    for i, event in enumerate(selected):
        point, used, arf, preburst, loss_label = totals.get(
            (event.aep, int(event.duration_min), event.scenario),
            (float("nan"), float("nan"), float("nan"), None, "source data unavailable"),
        )
        decision = (
            "DROP"
            if event.status == ScreeningStatus.ZERO
            else "KEEP"
            if event.status == ScreeningStatus.POSITIVE
            else "UNRESOLVED"
        )
        key = (event.aep, int(event.duration_min), event.scenario)
        loss_audit = loss_adjustments.get(key) if loss_adjustments is not None else None
        if loss_audit is not None and native_user_loss:
            loss_label += f" [PyTUFLOW user {loss_audit.reference_year}; SSP loss factors]"
        loss_origin = (
            ("user_pytuflow_native" if native_user_loss else "user_scenario_override")
            if loss_audit is not None
            else ("arr_datahub" if source_kind == "arr" else source_kind)
        )
        caution_codes = arf_cautions.get(key, ()) if arf_cautions is not None else ()
        if source_preburst_ratios is not None:
            pb_ratio = source_preburst_ratios.get(key)
            ratio_basis = "arr_selected_source_ratio" if pb_ratio is not None else "source_ratio_unavailable"
        else:
            pb_ratio = preburst / point if preburst is not None and point > 0 else None
            ratio_basis = "applied_depth_over_point_ifd" if pb_ratio is not None else "not_applicable"
        pb_display = Text("—", style="grey50") if preburst is None else Text(f"{preburst:.4g}")
        ratio_display = Text("—", style="grey50") if pb_ratio is None else Text(f"{100 * pb_ratio:.2f}%")
        zero_label = (
            Text("bound", style="grey70")
            if event.status == ScreeningStatus.ZERO and event.zero_patterns == 0
            else Text(str(event.zero_patterns))
        )
        row: list[str | Text] = [
            _decision_cell(event.status),
            event.aep,
            f"{event.duration_min:g}",
            *([event.scenario or "base"] if show_scenario else []),
            f"{point:.{source_places}f}",
            _arf_cell(arf, flagged=bool(caution_codes)),
            f"{used:.{adjusted_places}f}",
            loss_label,
            pb_display,
            ratio_display,
            zero_label,
            str(event.positive_patterns),
            *([str(event.unresolved_patterns)] if show_unknown else []),
            _excess_cell(event.max_tp_total_excess_mm),
            _excess_cell(event.max_tp_bin_excess_mm),
        ]
        ends_group = i + 1 < len(selected) and selected[i + 1].aep != event.aep
        table.add_row(*row, end_section=ends_group)
        csv_records.append(
            [
                decision,
                event.aep,
                f"{event.duration_min:g}",
                event.scenario or "baseline",
                f"{point:.{source_places}f}",
                f"{arf:.4f}",
                f"{used:.{adjusted_places}f}",
                loss_label,
                "" if preburst is None else f"{preburst:.12g}",
                "" if pb_ratio is None else f"{pb_ratio:.12g}",
                ratio_basis,
                ";".join(caution_codes),
                str(event.zero_patterns),
                str(event.positive_patterns),
                *([str(event.unresolved_patterns)] if show_unknown else []),
                "" if event.max_tp_total_excess_mm is None else f"{event.max_tp_total_excess_mm:.12g}",
                "" if event.max_tp_bin_excess_mm is None else f"{event.max_tp_bin_excess_mm:.12g}",
                loss_origin,
                str(loss_audit.reference_year) if loss_audit is not None else "",
                f"{loss_audit.base.initial_loss_mm:.12g}" if loss_audit is not None else "",
                (
                    f"{loss_audit.base.continuing_loss_mm_per_hour:.12g}"
                    if loss_audit is not None and loss_audit.base.continuing_loss_mm_per_hour is not None
                    else ""
                ),
                f"{loss_audit.il_multiplier:.12g}" if loss_audit is not None else "",
                f"{loss_audit.cl_multiplier:.12g}" if loss_audit is not None else "",
                f"{loss_audit.effective.initial_loss_mm:.12g}" if loss_audit is not None else "",
                (
                    f"{loss_audit.effective.continuing_loss_mm_per_hour:.12g}"
                    if loss_audit is not None and loss_audit.effective.continuing_loss_mm_per_hour is not None
                    else ""
                ),
                ("pytuflow_native_storm" if native_user_loss else loss_audit.method) if loss_audit is not None else "",
            ]
        )
    console = Console()
    console.print(table)
    wiped = sum(e.status == ScreeningStatus.ZERO for e in events)
    unknown = sum(e.status == ScreeningStatus.UNRESOLVED for e in events)
    console.print(
        f"{len(events)} AEP/duration/scenario combinations screened: "
        f"{wiped} DROP; {len(events) - wiped - unknown} KEEP; {unknown} unresolved."
    )
    console.print(
        "ARF shade: darker grey = stronger reduction; white = 1.0; ARF * denotes a policy caution. "
        "Grey excess 0.0000 is exactly zero; white <0.0001 is positive."
    )
    console.print(
        "Max TP sum = greatest total excess depth in one TP; "
        "Max bin = greatest single time-bin excess depth across all TPs. "
        "These maxima may belong to different TPs."
    )
    console.print(
        "ARR PB ratio = original selected/interpolated Data Hub ratio (not climate- or ARF-adjusted); "
        "PB mm = applied preburst depth, which changes with point IFD. "
        "BoM PB ratio is inferred from manual preburst depth / point IFD."
    )
    if source_preburst_ratios is not None and any(
        source_preburst_ratios.get((event.aep, int(event.duration_min), event.scenario)) is None for event in selected
    ):
        console.print("WARNING: Selected ARR preburst source ratios unavailable for some events; blank is not zero.")
    console.print(
        "DROP and KEEP depend on the selected patterns and loss profile. "
        "Native-model parity and complete TP coverage are not established."
    )
    if csv_report is not None:
        csv_report.parent.mkdir(parents=True, exist_ok=True)
        with csv_report.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(
                [
                    "decision",
                    "aep",
                    "duration_min",
                    "climate_scenario",
                    "point_ifd_mm",
                    "arf",
                    "applied_rain_mm",
                    "loss_parameters",
                    "preburst_mm",
                    "preburst_ratio_fraction",
                    "preburst_ratio_basis",
                    "arf_cautions",
                    "zero_tp",
                    "positive_tp",
                    *(["unknown_tp"] if show_unknown else []),
                    "max_tp_total_excess_mm",
                    "max_tp_bin_excess_mm",
                    "loss_source",
                    "user_loss_reference_year",
                    "user_base_il_mm",
                    "user_base_cl_mm_per_hour",
                    "user_il_multiplier",
                    "user_cl_multiplier",
                    "adjusted_user_il_mm",
                    "adjusted_user_cl_mm_per_hour",
                    "loss_adjustment_method",
                ]
            )
            writer.writerows(csv_records)
        console.print(f"CSV report created: {csv_report}")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Screen design rainfall AEP/duration/temporal-pattern events for zero rainfall excess.",
        epilog=r"""Examples:
  py -3.14 screen_rainfall_losses.py --source arr --latitude -23.08632 --longitude 119.2755 --no-pause
  py -3.14 screen_rainfall_losses.py --source arr --offline --arr-response site.json --point-tp point_Increments.csv --areal-tp areal_Increments.csv --no-pause
  py -3.14 screen_rainfall_losses.py --source bom --storm-mode main-burst-only --ifd "D:\Model\depths_all_design.csv" --il 30 --cl 5 --view zero --no-pause
  py -3.14 screen_rainfall_losses.py --source bom --storm-mode main-burst-only --ifd "D:\Model\depths_all_design.csv" --aep 20% 10% 1% --no-pause""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", choices=("arr", "bom"), help="Rainfall backend (ARR 2026 API or BoM CSV).")
    parser.add_argument("--working-directory", type=Path)
    parser.add_argument("--ifd", type=Path, help="Original BoM IFD/All Design rainfall CSV.")
    parser.add_argument("--arr-response", type=Path, help="Saved ARR 2026 JSON; overrides a live API response.")
    parser.add_argument(
        "--offline",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Never access the network; require all necessary local ARR JSON and TP CSV files.",
    )
    parser.add_argument("--point-tp", type=Path, help="Local ARR point temporal pattern increments CSV.")
    parser.add_argument("--areal-tp", type=Path, help="Local ARR areal temporal pattern increments CSV.")
    parser.add_argument("--fetch-tps", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument(
        "--point-tp-fallback",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use point TPs if no areal TP is available, with a grouped warning.",
    )
    parser.add_argument("--latitude", type=float)
    parser.add_argument("--longitude", type=float)
    parser.add_argument("--site", help="Site identifier.")
    parser.add_argument("--catchment-area", type=float, metavar="KM2")
    parser.add_argument("--baseline-year", type=int, choices=(1990, 2030))
    parser.add_argument(
        "--loss-reference-year",
        type=int,
        choices=(1990, 2030),
        help="Reference year for user IL/CL (independent of the IFD baseline year).",
    )
    parser.add_argument("--aep-mode", choices=("all", "common"))
    parser.add_argument("--aep", nargs="+", action="append", help="Exact AEP labels, e.g. 20%% 1%% '1 in 200'.")
    parser.add_argument("--duration", nargs="+", type=int, help="Selected durations in minutes.")
    parser.add_argument("--il", type=float, help="Initial loss in mm.")
    parser.add_argument("--cl", type=float, help="Continuing loss in mm/hour.")
    parser.add_argument("--coefficient", type=float, help="Runoff coefficient after initial loss.")
    parser.add_argument(
        "--loss-timing",
        choices=("fractional", "full_increment"),
        help="How CL applies in the IL-exhaustion TP bin; full_increment is an experimental RORBwin 6.52 candidate.",
    )
    parser.add_argument("--arf-override", type=float, help="Explicit ARF [0..1] for every AEP/duration.")
    parser.add_argument(
        "--rainfall-factor", type=float, help="Manual multiplier for original BoM IFD rainfall (e.g. 1.15)."
    )
    parser.add_argument(
        "--cc",
        action="append",
        metavar="YEAR:SSP",
        help="ARR Data Hub climate scenario, e.g. --cc 2050:SSP2 --cc 2090:SSP5; baseline also reported.",
    )
    parser.add_argument("--matrix", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--tp-details", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--losses-json", type=Path, help="Optional per-AEP or per-duration loss overrides JSON.")
    parser.add_argument(
        "--storm-mode",
        choices=("complete", "main-burst-only"),
        help=(
            "complete (default): prepend preburst and use full storm IL; "
            "main-burst-only: use design burst with preburst-adjusted burst IL."
        ),
    )
    parser.add_argument("--preburst-percentile", choices=("recommended", "10%", "25%", "50%", "75%", "90%"))
    parser.add_argument(
        "--bom-preburst-depth", type=float, help="Explicit BoM preburst depth to subtract from storm IL."
    )
    parser.add_argument("--view", choices=("all", "zero", "keep"))
    parser.add_argument("--csv-report", type=Path, help="New report CSV; existing files are not overwritten.")
    parser.add_argument("--console-log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"))
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


def main(args: argparse.Namespace | None = None) -> int:  # noqa: C901 - coordinates two distinct data sources
    """Run read-only design event screening; return 1 on validation/processing errors."""
    print_wrapper_banner(
        wrapper_source="hydrology-python/screen_rainfall_losses.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    args = args if args is not None else _arguments()
    directory = (args.working_directory if args.working_directory is not None else WORKING_DIR).absolute()
    if not change_working_directory(target_dir=directory):
        return 1
    try:
        source = args.source if args.source is not None else SOURCE
        _choice("SOURCE", source, {"arr", "bom"})
        il = args.il if args.il is not None else INITIAL_LOSS_MM
        cl = args.cl if args.cl is not None else CONTINUING_LOSS_MM_PER_HOUR
        coefficient = args.coefficient if args.coefficient is not None else RUNOFF_COEFFICIENT
        loss_timing = args.loss_timing if args.loss_timing is not None else LOSS_TIMING
        _choice("LOSS_TIMING", loss_timing, {"fractional", "full_increment"})
        loss_default = _default_loss(il, cl, coefficient, loss_timing=loss_timing)
        losses_json = _path(args.losses_json if args.losses_json is not None else LOSS_OVERRIDES_JSON, directory)
        raw_losses: object = (
            json.loads(losses_json.read_text(encoding="utf-8")) if losses_json is not None else LOSS_OVERRIDES_BY_AEP
        )
        overrides = _loss_schedules(raw_losses, loss_timing=loss_timing)
        aep_chosen = [aep for group in args.aep for aep in group] if args.aep is not None else list(AEP_SELECTION)
        aep_mode = args.aep_mode if args.aep_mode is not None else AEP_MODE
        duration_chosen = args.duration if args.duration is not None else list(DURATION_SELECTION_MIN)
        latitude = args.latitude if args.latitude is not None else LATITUDE
        longitude = args.longitude if args.longitude is not None else LONGITUDE
        area = args.catchment_area if args.catchment_area is not None else CATCHMENT_AREA_KM2
        arf_override = args.arf_override if args.arf_override is not None else ARF_OVERRIDE
        rainfall_factor = args.rainfall_factor if args.rainfall_factor is not None else RAINFALL_FACTOR
        climate_values = args.cc if args.cc is not None else list(CLIMATE_SCENARIOS)
        climate_scenarios = _parse_climate_scenarios(climate_values)
        show_matrix = args.matrix if args.matrix is not None else SHOW_MATRIX
        show_tp_details = args.tp_details if args.tp_details is not None else SHOW_TP_DETAILS
        if not isfinite(rainfall_factor) or rainfall_factor <= 0:
            message = "RAINFALL_FACTOR must be finite and > 0."
            raise _error(message)
        if arf_override is not None and (not isfinite(arf_override) or not 0 <= arf_override <= 1):
            message = "ARF_OVERRIDE must be finite and in [0, 1]."
            raise _error(message)
        site = args.site if args.site is not None else SITE_NAME
        baseline = args.baseline_year if args.baseline_year is not None else IFD_BASELINE_YEAR
        loss_reference_year = args.loss_reference_year if args.loss_reference_year is not None else LOSS_REFERENCE_YEAR
        if loss_reference_year not in {1990, 2030}:
            message = "LOSS_REFERENCE_YEAR must be 1990 or 2030."
            raise _error(message)
        storm_mode = args.storm_mode if args.storm_mode is not None else STORM_MODE
        percentile = args.preburst_percentile if args.preburst_percentile is not None else PREBURST_PERCENTILE
        _choice("STORM_MODE", storm_mode, {"complete", "main-burst-only"})
        _choice("PREBURST_PERCENTILE", percentile, {"recommended", "10%", "25%", "50%", "75%", "90%"})
        view = args.view if args.view is not None else VIEW
        report_path = _path(args.csv_report if args.csv_report is not None else CSV_REPORT, directory)
        log_level = args.console_log_level if args.console_log_level is not None else CONSOLE_LOG_LEVEL
        _choice("CONSOLE_LOG_LEVEL", log_level, {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
        logging.basicConfig(level=getattr(logging, log_level))
        offline = args.offline if args.offline is not None else OFFLINE
        response_path = _path(args.arr_response if args.arr_response is not None else ARR_RESPONSE_JSON, directory)
        point_path = _path(args.point_tp if args.point_tp is not None else POINT_TP_CSV, directory)
        areal_path = _path(args.areal_tp if args.areal_tp is not None else AREAL_TP_CSV, directory)
        fetch_tps = args.fetch_tps if args.fetch_tps is not None else FETCH_TPS_FOR_BOM
        allow_point_fallback = args.point_tp_fallback if args.point_tp_fallback is not None else ALLOW_POINT_TP_FALLBACK
        _validate_local_sources(
            source=source,
            offline=offline,
            response_path=response_path,
            point_tp_path=point_path,
            areal_tp_path=areal_path,
            fetch_bom_tps=fetch_tps,
        )
        if report_path is not None and (report_path.exists() or report_path.is_symlink()):
            message = f"CSV_REPORT already exists; choose a new report path: {report_path}"
            raise _error(message)
        totals: dict[tuple[str, int, str | None], tuple[float, float, float, float | None, str]] = {}
        report_loss_adjustments: dict[tuple[str, int, str | None], ArrUserLossAdjustment] = {}
        native_user_losses: LossSpec | None = None
        source_pb_ratios: dict[tuple[str, int, str | None], float | None] | None = None
        source_places: int | None = None
        provenance_latitude = latitude
        provenance_longitude = longitude
        provenance_arf = (
            "Unity / point rainfall (catchment area <= 1 km²)" if area <= 1 else "Calculated per duration/AEP"
        )
        climate_note = ", ".join(climate_values) if climate_values else "baseline only"
        if source == "arr":
            if rainfall_factor != 1.0:
                message = "Manual RAINFALL_FACTOR applies to BoM IFD only; use ARR --cc for Data Hub SSP scenarios."
                raise _error(message)
            if args.ifd is not None or IFD_CSV is not None:
                message = "ARR mode uses the ARR 2026 RecIFD depths, not the BoM --ifd CSV; choose --source bom."
                raise _error(message)
            if response_path is None and (latitude is None or longitude is None):
                message = "Live ARR mode requires --latitude and --longitude, e.g. -23.08632 119.2755."
                raise _error(message)
            initial_cfg = _arr_config(
                latitude,
                longitude,
                area,
                site,
                ["1%"],
                [60],
                baseline,
                storm_mode,
                percentile,
                directory,
                response_path,
                point_tp_path=point_path,
                areal_tp_path=areal_path,
                climate_scenarios=climate_scenarios,
                arf_override=arf_override,
            )
            response = _arr_response(initial_cfg, response_path)
            strip_areal = _validate_offline_arr_patterns(
                response,
                offline=offline,
                areal_tp_path=areal_path,
                allow_point_fallback=allow_point_fallback,
            )
            if strip_areal:
                response = _response_without_areal_tp(response)
                Console(stderr=True).print(
                    "WARNING: Strict offline ARR response contains an areal TP URL but no local areal "
                    "increments CSV. Using point TPs instead; no network request was made."
                )
            available = _datahub_aeps(response, baseline)
            aeps = _aep_selection(available, aep_chosen, aep_mode)
            durations = _durations(_datahub_durations(response, baseline), duration_chosen, arr=True)
            source_data = cast("dict[str, object]", response.ifd_table(baseline)).get("data")
            if isinstance(source_data, list):
                source_rows = cast("list[object]", source_data)
                values = [
                    float(str(value))
                    for row in source_rows
                    if isinstance(row, list)
                    for value in cast("list[object]", row)
                ]
                source_places = _source_decimals(values)
            native_user_losses = (
                loss_default
                if climate_scenarios
                and storm_mode == "complete"
                and loss_reference_year == 1990
                and loss_default is not None
                and loss_default.continuing_loss_mm_per_hour is not None
                and not overrides
                else None
            )
            config = _arr_config(
                latitude,
                longitude,
                area,
                site,
                aeps,
                durations,
                baseline,
                storm_mode,
                percentile,
                directory,
                response_path,
                point_tp_path=point_path,
                areal_tp_path=areal_path,
                climate_scenarios=climate_scenarios,
                arf_override=arf_override,
                user_losses=native_user_losses,
            )
            Console().print(
                f"ARR inputs: {'offline' if offline else 'hybrid/local' if response_path is not None else 'online'}; "
                f"response={response_path or 'live API'}; "
                f"point TP={point_path or 'Data Hub URL'}; "
                f"areal TP={areal_path or 'Data Hub URL (if supplied)'}"
            )
            from pytuflow.arr.engine import ArrEngine, EventResult

            engine = ArrEngine(config, response)
            tp_method: str = "_tp_set_for_config"
            try:
                tp_set = getattr(engine, tp_method)()
            except Exception as exc:
                layer = response.layer("ArealTP")
                url = layer.get("url") if layer else None
                if (
                    not allow_point_fallback
                    or offline
                    or areal_path is not None
                    or url is None
                    or "Failed to download temporal pattern zip" not in str(exc)
                    or url not in str(exc)
                ):
                    raise
                Console(stderr=True).print("WARNING: Areal TP download failed; falling back to point TPs.")
                engine.response = _response_without_areal_tp(response)
                tp_set = getattr(engine, tp_method)()

            admission = preflight_temporal_pattern_durations(
                tp_set,
                durations=durations,
                aeps=aeps,
                area_km2=area,
                allow_point_fallback=allow_point_fallback,
            )
            _announce_tp_admission(admission, area=area)
            if not admission.included_durations:
                message = "No selected durations have applicable TPs. No events were screened or classified DROP."
                raise _error(message)
            cast(Any, engine).config.events.duration = list(admission.included_durations)
            run_method: str = "run"
            runner = cast("Callable[[], list[EventResult]]", getattr(engine, run_method))
            events: list[EventResult] = runner()
            source_pb_ratios = _arr_source_preburst_ratios(engine.response, events, percentile)
            if loss_default is not None or overrides:
                report_loss_adjustments = user_loss_adjustments(
                    events,
                    default_losses=loss_default,
                    losses_by_aep=overrides,
                    reference_year=loss_reference_year,
                    factor_provider=engine.response.climate_change_loss_factors,
                    complete_storm=storm_mode == "complete",
                )
            scenario_adjustments = report_loss_adjustments if climate_scenarios and native_user_losses is None else None
            bundle = assemble_arr_events(
                events,
                losses_by_aep={} if native_user_losses is not None else overrides,
                default_losses=None if native_user_losses is not None else loss_default,
                event_loss_adjustments=scenario_adjustments,
                continuing_loss_timing=cast(Any, loss_timing),
                complete_storm=storm_mode == "complete",
                source_preburst_ratios=source_pb_ratios,
            )
            screens = bundle.patterns
            totals = bundle.rainfall
            pattern_sets = bundle.pattern_sets
            provenance_arf = (
                f"explicit factor {arf_override:g} (all events)"
                if arf_override is not None
                else "PyTUFLOW ARR2019 Book 2 ARF, per AEP/duration (frequent AEP may be unity)"
            )
        else:
            if climate_scenarios:
                message = "ARR SSP climate scenarios require --source arr; for BoM use --rainfall-factor."
                raise _error(message)
            if storm_mode != "main-burst-only":
                message = (
                    "BoM CSV mode requires --storm-mode main-burst-only: the BoM IFD file "
                    "does not contain a complete preburst hyetograph. "
                    "Use --source arr for the default complete storm."
                )
                raise _error(message)
            ifd_path = _path(args.ifd if args.ifd is not None else IFD_CSV, directory)
            if ifd_path is None or not ifd_path.is_file():
                message = "BoM mode needs a readable --ifd depths_all_design.csv."
                raise _error(message)
            reader_name: str = "read_bom_ifd_csv"
            reader = cast("Callable[[Path], Any]", getattr(import_module("runoff_routing.bom_ifd"), reader_name))
            imported: Any = reader(ifd_path)
            aeps = _aep_selection([p.label for p in imported.probabilities], aep_chosen, aep_mode)
            durations = _durations(list(imported.durations_min), duration_chosen, arr=False)
            if latitude is None and longitude is None:
                provenance_latitude = float(imported.latitude) if imported.latitude is not None else None
                provenance_longitude = float(imported.longitude) if imported.longitude is not None else None
            source_places = _source_decimals(
                [
                    float(imported.depth_mm_at(duration, p, convert_intensity=True))
                    for p in imported.probabilities
                    if p.label in aeps
                    for duration in durations
                ]
            )
            pb_depth = args.bom_preburst_depth if args.bom_preburst_depth is not None else BOM_PREBURST_DEPTH_MM
            bom_default, bom_overrides = _apply_bom_preburst(loss_default, overrides, pb_depth)
            response_for_bom: Any | None = None
            if (area > 1 and arf_override is None) or (fetch_tps and point_path is None and response_path is not None):
                query_latitude = provenance_latitude
                query_longitude = provenance_longitude
                if response_path is None and offline:
                    message = (
                        "Strict offline BoM ARF requires --arr-response saved Data Hub JSON "
                        "containing ARFParams, or an explicit --arf-override."
                    )
                    raise _error(message)
                if response_path is None and (query_latitude is None or query_longitude is None):
                    message = "BoM ARF requires ARR Data Hub coordinates or a saved --arr-response JSON."
                    raise _error(message)
                config_for_bom = _arr_config(
                    query_latitude,
                    query_longitude,
                    area,
                    site,
                    ["1%"],
                    [60],
                    baseline,
                    "main-burst-only",
                    percentile,
                    directory,
                    response_path,
                )
                response_for_bom = _arr_response(config_for_bom, response_path)
                _validate_offline_arr_patterns(
                    response_for_bom,
                    offline=offline,
                    areal_tp_path=areal_path,
                    allow_point_fallback=allow_point_fallback,
                )
            factors = _bom_arf_factors(
                area=area,
                durations=durations,
                aeps=aeps,
                response=response_for_bom,
                override=arf_override,
            )
            patterns = _bom_patterns(
                imported,
                latitude,
                longitude,
                area,
                fetch_tps,
                point_path,
                areal_path,
                directory,
                response=response_for_bom,
                allow_point_fallback=allow_point_fallback,
            )
            if patterns is not None:
                admission = preflight_temporal_pattern_durations(
                    patterns,
                    durations=durations,
                    aeps=aeps,
                    area_km2=area,
                    allow_point_fallback=allow_point_fallback,
                )
                _announce_tp_admission(admission, area=area)
                if not admission.included_durations:
                    message = "No selected durations have applicable TPs. No events were screened or classified DROP."
                    raise _error(message)
                durations = list(admission.included_durations)
            bundle = assemble_bom_events(
                imported,
                aeps=set(aeps),
                durations=set(durations),
                temporal_patterns=patterns,
                losses_by_aep=bom_overrides,
                default_losses=bom_default,
                arf_by_event=factors,
                rainfall_multiplier=rainfall_factor,
                preburst_depth_mm=pb_depth,
            )
            screens = bundle.patterns
            totals = bundle.rainfall
            pattern_sets = bundle.pattern_sets
            provenance_arf = (
                f"explicit factor {arf_override:g} (all events)"
                if arf_override is not None
                else "PyTUFLOW ARR ARF from Data Hub coefficients"
                if area > 1
                else "unity (area <= 1 km²)"
            )
        arf_cautions = _arf_policy_cautions(totals, area=area, override=arf_override)
        _print_provenance(
            latitude=provenance_latitude,
            longitude=provenance_longitude,
            site=site,
            area=area,
            arf_source=provenance_arf,
            screens=screens,
            loss_timing=loss_timing,
            climate=climate_note,
            pattern_sets=pattern_sets,
            arf_cautions=arf_cautions,
        )
        _print_report(
            screens,
            totals,
            view,
            report_path,
            source_decimal_places=source_places,
            arf_cautions=arf_cautions,
            source_preburst_ratios=source_pb_ratios,
            loss_adjustments=report_loss_adjustments,
            native_user_loss=native_user_losses is not None,
            source_kind=source,
        )
        _show_matrix(screens, enabled=show_matrix)
        _print_tp_details(screens, enabled=show_tp_details)
        Console().print(
            "TP counts: Zero TP = zero-excess TP results; Positive TP = nonzero-excess TP results; "
            "Unknown TP = unresolvable calculations (hidden when all zero). "
            "A single zero with no TP identity can be a guaranteed bound, not one evaluated TP."
        )
        Console().print(
            "DROP = no rainfall excess under this loss profile for every considered TP; "
            "KEEP = at least one TP produces rainfall excess (not necessarily material flow); "
            "UNRESOLVED = insufficient data, so never drop."
        )
        Console().print(
            "CL = mm/hour and is applied per TP time bin. Fractional mode prorates CL only "
            "in the IL-exhaustion bin; full_increment mode (experimental RORBwin 6.52 candidate) "
            "allows a full bin's CL capacity. Neither guarantees TUFLOW soil-infiltration equivalence."
        )
        Console().print(
            "Storm mode: complete (default) = explicit preburst + main burst with full storm IL once; "
            "main-burst-only = main design burst only, with preburst-adjusted burst IL. "
            "These are alternatives, not additive loss deductions."
        )
        Console().print(
            "TUFLOW material losses remove rainfall BEFORE it reaches cells and stepped "
            "rainfall holds the input RATE constant per interval. This tool evaluates CL on "
            "TP bins. TUFLOW's exact IL-crossing-bin CL treatment is NOT verified: "
            "neither fractional nor full_increment is certified as materials-file parity."
        )
    except Exception as exc:  # noqa: BLE001 - entry-point boundary: fail closed, preserve input
        Console(stderr=True).print(f"Rainfall screening failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    arguments = _arguments()
    exit_code = main(arguments)
    print_wrapper_banner(
        wrapper_source="hydrology-python/screen_rainfall_losses.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not (arguments.no_pause or not PAUSE_ON_FINISH):
        pause_console()
    raise SystemExit(exit_code)
