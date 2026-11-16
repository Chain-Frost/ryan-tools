r"""Screen BoM/ARR design rainfall for loss-wiped events without running a hydraulic model.

Edit settings below or provide CLI overrides. This prints an AEP/duration
screening table, including how many selected temporal patterns are zero,
positive, or unresolved. It does not claim that positive rainfall excess
produces flow at an outlet, or that unexamined patterns can be omitted.

Examples:
    py -3.14 screen_rainfall_losses.py --source arr --latitude -23.08632 --longitude 119.2755 --view zero --no-pause
    py -3.14 screen_rainfall_losses.py --source bom --ifd "depths_hd4_temp_all_design.csv" --il 30 --cl 5 --no-pause
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-16.1"

WORKING_DIR: Path = Path(__file__).absolute().parent
SOURCE = "arr"  # "arr" downloads ARR Data Hub 2026; "bom" uses the original BoM IFD CSV.
IFD_CSV: Path | None = None
ARR_RESPONSE_JSON: Path | None = None  # Optional offline JSON previously obtained from ARR Data Hub.
POINT_TP_CSV: Path | None = None  # Optional PyTUFLOW ARR *_Increments.csv; primarily for offline BoM runs.
AREAL_TP_CSV: Path | None = None
LATITUDE: float | None = None  # E.g. -23.08632
LONGITUDE: float | None = None  # E.g. 119.2755
SITE_NAME = "Rainfall screening"
CATCHMENT_AREA_KM2 = 0.0  # Zero uses point rainfall/TP. Set actual area for areal analysis.
IFD_BASELINE_YEAR = 2030  # ARR Data Hub: 1990 historical or 2030 current baseline.
AEP_MODE = "common"  # "common" or "all"; AEP_SELECTION overrides either mode.
COMMON_AEPS: list[str] = [
    "63.2%",
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
DURATION_SELECTION_MIN: list[int] = []  # Empty: every BoM duration; standard ARR durations below.
ARR_DURATIONS_MIN: list[int] = [30, 45, 60, 90, 120, 180, 360, 720, 1440, 2880, 4320]
INITIAL_LOSS_MM: float | None = None  # None uses Data Hub's losses for ARR; BoM is unresolved without losses.
CONTINUING_LOSS_MM_PER_HOUR: float | None = None
RUNOFF_COEFFICIENT: float | None = None  # Post-IL fraction; mutually exclusive with continuing loss.
# Per-AEP losses may also be duration-keyed, e.g. {"1%": {"60": {"il_mm": 25, "cl_mm_per_hour": 5}}}.
LOSS_OVERRIDES_BY_AEP: dict[str, object] = {}
LOSS_OVERRIDES_JSON: Path | None = None  # JSON object using the same per-AEP format as above.
STORM_MODE = "burst"  # ARR "burst" uses preburst-adjusted burst IL; "complete" prepends preburst.
PREBURST_PERCENTILE = "recommended"  # "recommended", "10%", "25%", "50%", "75%", "90%".
BOM_PREBURST_DEPTH_MM: float | None = None  # Only explicit subtraction from a storm IL, NOT complete storm.
FETCH_TPS_FOR_BOM = True  # Fetch ARR point/areal patterns using BoM file's requested coordinates.
VIEW = "all"  # "all", "zero" (only excluded), "keep" (positive or unresolved).
CSV_REPORT: Path | None = None  # Exclusive-create output; existing reports are never overwritten.
CONSOLE_LOG_LEVEL = "WARNING"
PAUSE_ON_FINISH = True

import argparse
import csv
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from importlib import import_module
from math import isfinite
from typing import Any, cast

from rich.console import Console
from rich.table import Table

from ryan_library.functions.hydrology.rainfall_loss_screening import (
    LossSpec,
    PatternScreen,
    ScreeningStatus,
    reduced_burst_initial_loss,
    screen_arr_events,
    screen_bom_ifd,
    summarize_events,
)
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


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


def _loss_from_mapping(entry: object) -> LossSpec:
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
    )


def _loss_schedules(raw: object) -> dict[str, LossSpec | dict[int, LossSpec]]:
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
            schedules[aep] = _loss_from_mapping(data)
        else:
            durations: dict[int, LossSpec] = {}
            for key, value in data.items():
                if not key.isdecimal() or int(key) <= 0 or str(int(key)) != key:
                    message = f"Invalid duration key {key!r} for {aep!r}; use e.g. '60' in minutes."
                    raise _error(message)
                durations[int(key)] = _loss_from_mapping(value)
            if not durations:
                message = f"No losses specified for {aep!r}."
                raise _error(message)
            schedules[aep] = durations
    return schedules


def _default_loss(il: float | None, cl: float | None, coefficient: float | None) -> LossSpec | None:
    if il is None:
        if cl is not None or coefficient is not None:
            message = "INITIAL_LOSS_MM / --il is required if --cl or --coefficient is supplied."
            raise _error(message)
        return None
    return LossSpec(il, continuing_loss_mm_per_hour=cl, runoff_coefficient=coefficient)


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
    selected = list(chosen) if chosen else list(ARR_DURATIONS_MIN) if arr else list(available)
    if not selected or len(selected) != len(set(selected)) or any(t <= 0 for t in selected):
        message = "Duration selection must have unique positive integer minutes."
        raise _error(message)
    if not arr:
        unknown = [t for t in selected if t not in available]
        if unknown:
            message = f"Duration(s) not in BoM file: {unknown}. Available: {list(available)}"
            raise _error(message)
    return selected


def _arr_config(
    latitude: float,
    longitude: float,
    area: float,
    site: str,
    aeps: list[str],
    durations: list[int],
    baseline: int,
    mode: str,
    percentile: str,
    directory: Path,
    json_path: Path | None,
) -> Any:
    from pytuflow.arr.config import ArrConfig

    if baseline not in {1990, 2030}:
        message = "IFD_BASELINE_YEAR must be 1990 or 2030."
        raise _error(message)
    if not isfinite(area) or area < 0:
        message = "CATCHMENT_AREA_KM2 must be finite and nonnegative."
        raise _error(message)
    if not isfinite(latitude) or not -90 <= latitude <= 90:
        message = "LATITUDE must be between -90 and 90."
        raise _error(message)
    if not isfinite(longitude) or not -180 <= longitude <= 180:
        message = "LONGITUDE must be between -180 and 180."
        raise _error(message)
    data: dict[str, object] = {
        "site": {"name": site, "latitude": latitude, "longitude": longitude, "catchment_area": area},
        "ifd": {"baseline_year": baseline},
        "events": {"aep": aeps, "duration": durations, "output_notation": "aep"},
        "preburst": {"percentile": percentile, "pattern_method": "recommended"},
        "complete_storm": mode == "complete",
        "output": {"path": str(directory)},
    }
    if json_path is not None:
        data["response_json"] = str(json_path)
    method_name: str = "from_dict"
    return getattr(ArrConfig, method_name)(data)


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


def _bom_patterns(
    source: Any,
    latitude: float | None,
    longitude: float | None,
    area: float,
    fetch_tps: bool,
    point_csv: Path | None,
    areal_csv: Path | None,
    directory: Path,
) -> Any:
    from pytuflow.arr.temporal_patterns import TemporalPatternSet

    if point_csv is not None:
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
    config = _arr_config(lat, lon, area, SITE_NAME, ["1%"], [60], 2030, "burst", "recommended", directory, None)
    response = _arr_response(config, None)
    point = response.layer("PointTP", required=True)
    areal = response.layer("ArealTP")
    return TemporalPatternSet.from_api_response(point["url"], areal["url"] if areal else None, catchment_area=area)


def _print_report(
    screens: Sequence[PatternScreen],
    totals: Mapping[tuple[str, int, str | None], tuple[float, float | None, str]],
    view: str,
    csv_report: Path | None,
) -> None:
    _choice("VIEW", view, {"all", "zero", "keep"})
    events = summarize_events(screens)
    table = Table(title="Design rainfall loss screening (selected temporal patterns)", show_lines=False)
    table.add_column("Decision", no_wrap=True)
    for header in (
        "AEP",
        "Duration (min)",
        "Rain (mm)",
        "Loss / preburst",
        "Zero TP",
        "Positive TP",
        "Unknown TP",
    ):
        table.add_column(header, justify="right" if "TP" in header or "min" in header or "mm" in header else "left")
    records: list[list[str]] = []
    for event in events:
        if view == "zero" and event.status != ScreeningStatus.ZERO:
            continue
        if view == "keep" and event.status == ScreeningStatus.ZERO:
            continue
        rainfall, preburst, losses = totals.get(
            (event.aep, int(event.duration_min), event.scenario), (float("nan"), None, "unknown")
        )
        decision = (
            "WIPE OUT"
            if event.status == ScreeningStatus.ZERO
            else "KEEP"
            if event.status == ScreeningStatus.POSITIVE
            else "UNRESOLVED"
        )
        details = f"{losses}; PB {preburst:g} mm" if preburst is not None else losses
        row = [
            event.aep,
            f"{event.duration_min:g}",
            f"{rainfall:.3f}",
            details,
            str(event.zero_patterns),
            str(event.positive_patterns),
            str(event.unresolved_patterns),
            decision,
        ]
        records.append(row)
        table.add_row(row[-1], *row[:-1])
    console = Console()
    console.print(table)
    wiped = sum(e.status == ScreeningStatus.ZERO for e in events)
    unknown = sum(e.status == ScreeningStatus.UNRESOLVED for e in events)
    console.print(
        f"{len(events)} AEP/duration/scenario combinations screened: "
        f"{wiped} wiped out; {len(events) - wiped - unknown} positive; {unknown} unresolved."
    )
    console.print(
        "A zero result applies to these assumptions and supplied patterns only; native-model parity is unverified."
    )
    if csv_report is not None:
        csv_report.parent.mkdir(parents=True, exist_ok=True)
        with csv_report.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(
                ["AEP", "duration_min", "rain_mm", "loss_preburst", "zero_tp", "positive_tp", "unknown_tp", "decision"]
            )
            writer.writerows(records)
        console.print(f"CSV report created: {csv_report}")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Screen design rainfall AEP/duration/temporal-pattern events for zero rainfall excess.",
        epilog=r"""Examples:
  py -3.14 screen_rainfall_losses.py --source arr --latitude -23.08632 --longitude 119.2755 --no-pause
  py -3.14 screen_rainfall_losses.py --source bom --ifd "D:\Model\depths_all_design.csv" --il 30 --cl 5 --view zero --no-pause
  py -3.14 screen_rainfall_losses.py --source bom --ifd "D:\Model\depths_all_design.csv" --aep 20% 10% 1% --no-pause""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", choices=("arr", "bom"), help="Rainfall backend (ARR 2026 API or BoM CSV).")
    parser.add_argument("--working-directory", type=Path)
    parser.add_argument("--ifd", type=Path, help="Original BoM IFD/All Design rainfall CSV.")
    parser.add_argument("--arr-response", type=Path, help="Local ARR Data Hub JSON (offline ARR mode).")
    parser.add_argument("--point-tp", type=Path, help="Local ARR point temporal pattern increments CSV.")
    parser.add_argument("--areal-tp", type=Path, help="Local ARR areal temporal pattern increments CSV.")
    parser.add_argument("--fetch-tps", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--latitude", type=float)
    parser.add_argument("--longitude", type=float)
    parser.add_argument("--site", help="Site identifier.")
    parser.add_argument("--catchment-area", type=float, metavar="KM2")
    parser.add_argument("--baseline-year", type=int, choices=(1990, 2030))
    parser.add_argument("--aep-mode", choices=("all", "common"))
    parser.add_argument("--aep", nargs="+", action="append", help="Exact AEP labels, e.g. 20%% 1%% '1 in 200'.")
    parser.add_argument("--duration", nargs="+", type=int, help="Selected durations in minutes.")
    parser.add_argument("--il", type=float, help="Initial loss in mm.")
    parser.add_argument("--cl", type=float, help="Continuing loss in mm/hour.")
    parser.add_argument("--coefficient", type=float, help="Runoff coefficient after initial loss.")
    parser.add_argument("--losses-json", type=Path, help="Optional per-AEP or per-duration loss overrides JSON.")
    parser.add_argument("--storm-mode", choices=("burst", "complete"), help="ARR burst-IL or complete preburst storm.")
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
        loss_default = _default_loss(il, cl, coefficient)
        losses_json = _path(args.losses_json if args.losses_json is not None else LOSS_OVERRIDES_JSON, directory)
        raw_losses: object = (
            json.loads(losses_json.read_text(encoding="utf-8")) if losses_json is not None else LOSS_OVERRIDES_BY_AEP
        )
        overrides = _loss_schedules(raw_losses)
        aep_chosen = [aep for group in args.aep for aep in group] if args.aep is not None else list(AEP_SELECTION)
        aep_mode = args.aep_mode if args.aep_mode is not None else AEP_MODE
        duration_chosen = args.duration if args.duration is not None else list(DURATION_SELECTION_MIN)
        latitude = args.latitude if args.latitude is not None else LATITUDE
        longitude = args.longitude if args.longitude is not None else LONGITUDE
        area = args.catchment_area if args.catchment_area is not None else CATCHMENT_AREA_KM2
        site = args.site if args.site is not None else SITE_NAME
        baseline = args.baseline_year if args.baseline_year is not None else IFD_BASELINE_YEAR
        storm_mode = args.storm_mode if args.storm_mode is not None else STORM_MODE
        percentile = args.preburst_percentile if args.preburst_percentile is not None else PREBURST_PERCENTILE
        _choice("STORM_MODE", storm_mode, {"burst", "complete"})
        _choice("PREBURST_PERCENTILE", percentile, {"recommended", "10%", "25%", "50%", "75%", "90%"})
        view = args.view if args.view is not None else VIEW
        report_path = _path(args.csv_report if args.csv_report is not None else CSV_REPORT, directory)
        log_level = args.console_log_level if args.console_log_level is not None else CONSOLE_LOG_LEVEL
        _choice("CONSOLE_LOG_LEVEL", log_level, {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
        logging.basicConfig(level=getattr(logging, log_level))
        if report_path is not None and (report_path.exists() or report_path.is_symlink()):
            message = f"CSV_REPORT already exists; choose a new report path: {report_path}"
            raise _error(message)
        totals: dict[tuple[str, int, str | None], tuple[float, float | None, str]] = {}
        if source == "arr":
            if args.ifd is not None or IFD_CSV is not None:
                message = "ARR mode uses the ARR 2026 RecIFD depths, not the BoM --ifd CSV; choose --source bom."
                raise _error(message)
            if latitude is None or longitude is None:
                message = "ARR mode requires --latitude and --longitude, e.g. -23.08632 119.2755."
                raise _error(message)
            response_path = _path(args.arr_response if args.arr_response is not None else ARR_RESPONSE_JSON, directory)
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
            )
            response = _arr_response(initial_cfg, response_path)
            available = _datahub_aeps(response, baseline)
            aeps = _aep_selection(available, aep_chosen, aep_mode)
            durations = _durations([], duration_chosen, arr=True)
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
            )
            from pytuflow.arr.engine import ArrEngine, EventResult

            run_method: str = "run"
            runner = cast("Callable[[], list[EventResult]]", getattr(ArrEngine(config, response), run_method))
            events: list[EventResult] = runner()
            screens = screen_arr_events(events, losses_by_aep=overrides, default_losses=loss_default)
            for event in events:
                preburst = cast(Any, event).preburst
                name = str(event.aep_name)
                schedule = overrides.get(name)
                effective = schedule.get(int(event.duration)) if isinstance(schedule, Mapping) else schedule
                loss = effective if effective is not None else loss_default
                text = (
                    f"IL {loss.initial_loss_mm:g}; CL {loss.continuing_loss_mm_per_hour:g}"
                    if loss is not None and loss.continuing_loss_mm_per_hour is not None
                    else f"IL {loss.initial_loss_mm:g}; C {loss.runoff_coefficient:g}"
                    if loss is not None and loss.runoff_coefficient is not None
                    else f"ARR IL {event.initial_loss:g}; CL {event.continuing_loss:g}"
                )
                totals[(name, int(event.duration), str(event.cc_scenario) if event.cc_scenario else None)] = (
                    float(event.depth_areal),
                    float(preburst.depth) if preburst is not None else None,
                    text,
                )
        else:
            if storm_mode != "burst":
                message = "BoM CSV mode only screens burst rainfall; use --source arr for complete preburst."
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
            pb_depth = args.bom_preburst_depth if args.bom_preburst_depth is not None else BOM_PREBURST_DEPTH_MM
            bom_default, bom_overrides = _apply_bom_preburst(loss_default, overrides, pb_depth)
            point_path = _path(args.point_tp if args.point_tp is not None else POINT_TP_CSV, directory)
            areal_path = _path(args.areal_tp if args.areal_tp is not None else AREAL_TP_CSV, directory)
            fetch_tps = args.fetch_tps if args.fetch_tps is not None else FETCH_TPS_FOR_BOM
            patterns = _bom_patterns(imported, latitude, longitude, area, fetch_tps, point_path, areal_path, directory)
            screens = screen_bom_ifd(
                imported,
                temporal_patterns=patterns,
                losses_by_aep=bom_overrides,
                default_losses=bom_default,
                aeps=set(aeps),
                durations_min=set(durations),
            )
            for probability in imported.probabilities:
                if probability.label not in aeps:
                    continue
                for duration in durations:
                    schedule = bom_overrides.get(probability.label, bom_default)
                    loss = schedule.get(duration) if isinstance(schedule, Mapping) else schedule
                    label = (
                        f"IL {loss.initial_loss_mm:g}; CL {loss.continuing_loss_mm_per_hour:g}"
                        if loss is not None and loss.continuing_loss_mm_per_hour is not None
                        else f"IL {loss.initial_loss_mm:g}; C {loss.runoff_coefficient:g}"
                        if loss is not None and loss.runoff_coefficient is not None
                        else "losses missing"
                    )
                    totals[(probability.label, duration, None)] = (
                        float(imported.depth_mm_at(duration, probability, convert_intensity=True)),
                        pb_depth,
                        label,
                    )
        _print_report(screens, totals, view, report_path)
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
