"""TUFLOW rainfall CSV → single-pluviograph RORB DESIGN storm conversion.

The TUFLOW STEPPED rainfall convention assigns the depth at time t to the
preceding interval. RORB's first rainfall ordinate is the *first interval
after burst start*, not the zero-time placeholder.

References:
- RORB Version 6 User Manual (2010), §3.2 p.28 and Table 5-2 pp.56–57.
- RORBwin 6.52 release notes, §1.8 (2000-increment limit).
- TUFLOW Classic/HPC 2026.0 manual, §8.4.3.1.1.
"""

import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.tuflow.tuflow_result_naming import validate_output_filename

type TerminalZeroPolicy = Literal["auto", "keep", "drop"]

MAX_RORB_INCREMENTS = 2000
DEFAULT_RUN_INCREMENTS = 200  # RORBwin 6.52 default; may be too short for long routing tails.

_CSV_NAME_RE = re.compile(
    r"^(?P<catchment>.+?)_RF_(?P<aep>\d+(?:\.\d+)?)p(?P<duration>\d+)m\.csv$",
    re.IGNORECASE,
)
_TP_COLUMN_RE = re.compile(r"^TP(?P<digits>\d{1,2})(?:_|$)", re.IGNORECASE)
_AEP_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)p$", re.IGNORECASE)


class ConversionError(ValueError):
    """Invalid rainfall data or RORB storm configuration."""


@dataclass(frozen=True, slots=True)
class RainfallSeries:
    column_name: str
    timestep_hours: float
    values: tuple[float, ...]  # Actual interval depths, *excluding* TUFLOW time-zero placeholder.

    @property
    def depth_mm(self) -> float:
        return math.fsum(self.values)


def normalise_aep(text: str) -> str:
    """Render TUFLOW AEP 01.00p as 01p without losing significant decimals."""
    match = _AEP_RE.fullmatch(text.strip())
    if match is None:
        raise ConversionError(f"Invalid TUFLOW AEP: {text!r}")
    token = re.sub(r"\.0+$", "", match["value"])
    return f"{token}p"


def normalise_tp(text: str) -> str:
    """Normalise the TP component using the library's existing TUFLOW parser."""
    match = _TP_COLUMN_RE.match(text.strip())
    if match is None:
        raise ConversionError(f"Expected TP## at the start of {text!r}.")
    canonical = TuflowStringParser.normalize_tp_label(f"TP{match['digits']}")
    if canonical is None:
        raise ConversionError(f"Invalid temporal pattern: {text!r}.")
    return canonical


def infer_output_details(csv_path: Path, column_name: str) -> tuple[str, str, str, str]:
    """Extract catchment, AEP, duration in minutes and TP from a standard filename."""
    match = _CSV_NAME_RE.fullmatch(csv_path.name)
    if match is None:
        raise ConversionError(
            f"Cannot infer filename from {csv_path.name!r}; use --output-name."
        )
    return (
        match["catchment"],
        normalise_aep(f"{match['aep']}p"),
        str(int(match["duration"])),
        normalise_tp(column_name).lower(),
    )


def infer_model_name(csv_path: Path) -> str:
    """Find a model identifier in an ancestor such as TUFLOW_MLGD."""
    for ancestor in (csv_path, *csv_path.parents):
        if ancestor.name.upper().startswith("TUFLOW_") and len(ancestor.name) > 7:
            return ancestor.name[7:]
    raise ConversionError(
        "Cannot infer the STM model name from a TUFLOW_* ancestor; set --model-name."
    )


def _clean_cell(cell: str) -> str:
    return cell.strip().lstrip("\ufeff")


def _nominal_duration_hours(csv_path: Path) -> float | None:
    match = _CSV_NAME_RE.fullmatch(csv_path.name)
    return int(match["duration"]) / 60 if match is not None else None


def read_rainfall_series(
    csv_path: Path,
    column_name: str,
    *,
    terminal_zero: TerminalZeroPolicy = "auto",
) -> RainfallSeries:
    """Read uniform-step TUFLOW mm-per-interval values and map to RORB intervals.

    Auto strips a *single* terminal zero only when the standard input filename
    declares a storm duration exactly one step shorter than the CSV timespan.
    `keep` preserves all non-initial ordinates for non-standard event layouts.
    `drop` explicitly removes one terminal zero regardless of filename.
    Interior dry increments are never discarded.
    """
    if terminal_zero not in {"auto", "keep", "drop"}:
        raise ConversionError(f"Unknown terminal zero policy: {terminal_zero!r}")
    try:
        with csv_path.open(encoding="utf-8-sig", newline="") as stream:
            rows = [[_clean_cell(cell) for cell in row] for row in csv.reader(stream)]
    except OSError as exc:
        raise ConversionError(f"Could not read input CSV {csv_path}: {exc}") from exc

    header_pos = next(
        (i for i, row in enumerate(rows) if row and row[0].casefold() in {"time (hour)", "time (hours)"}),
        None,
    )
    if header_pos is None:
        raise ConversionError("No rainfall header beginning 'Time (hour)' was found.")
    header = rows[header_pos]
    column_indices = [i for i, name in enumerate(header) if name.casefold() == column_name.casefold()]
    if not column_indices:
        raise ConversionError(f"Rainfall column {column_name!r} not found; available: {header[1:]}")
    if len(column_indices) != 1 or column_indices[0] == 0:
        raise ConversionError(f"Ambiguous or invalid rainfall column {column_name!r}.")
    column_index = column_indices[0]

    times: list[float] = []
    depths: list[float] = []
    for line, row in enumerate(rows[header_pos + 1 :], start=header_pos + 2):
        if not row or not any(row):
            continue
        if len(row) <= column_index or not row[0] or not row[column_index]:
            raise ConversionError(f"CSV line {line}: missing time or rainfall depth.")
        try:
            time, depth = float(row[0]), float(row[column_index])
        except ValueError as exc:
            raise ConversionError(f"CSV line {line}: non-numeric time or depth.") from exc
        if not math.isfinite(time) or not math.isfinite(depth):
            raise ConversionError(f"CSV line {line}: NaN/infinity is not allowed.")
        if depth < 0:
            raise ConversionError(f"CSV line {line}: negative rainfall depth {depth:g} mm.")
        times.append(time)
        depths.append(depth)

    if len(times) < 2:
        raise ConversionError("At least two timestamps are required for one rainfall interval.")
    if not math.isclose(times[0], 0.0, abs_tol=1e-9):
        raise ConversionError(f"First TUFLOW rainfall timestamp must be zero; found {times[0]:g}.")
    if not math.isclose(depths[0], 0.0, abs_tol=1e-12):
        raise ConversionError("TUFLOW time-zero rainfall must be zero; it is not a RORB rainfall interval.")

    timestep = times[1] - times[0]
    if timestep <= 0:
        raise ConversionError("Rainfall times must increase.")
    for line, (before, after) in enumerate(zip(times, times[1:]), start=header_pos + 3):
        if not math.isclose(after - before, timestep, rel_tol=1e-8, abs_tol=1e-9):
            raise ConversionError(f"Irregular rainfall timestep at CSV line {line}.")

    # TUFLOW assigns value at t_i to (t_(i-1), t_i]; t=0 is only a placeholder.
    interval_depths = depths[1:]
    duration = _nominal_duration_hours(csv_path)
    if terminal_zero == "drop":
        if not math.isclose(interval_depths[-1], 0, abs_tol=1e-12):
            raise ConversionError("Cannot drop a non-zero terminal rainfall interval.")
        interval_depths = interval_depths[:-1]
    elif terminal_zero == "auto" and duration is not None:
        if math.isclose(times[-1], duration + timestep, rel_tol=1e-8, abs_tol=1e-9):
            if not math.isclose(interval_depths[-1], 0, abs_tol=1e-12):
                raise ConversionError("Extra terminal interval after named storm duration is non-zero.")
            interval_depths = interval_depths[:-1]
        elif not math.isclose(times[-1], duration, rel_tol=1e-8, abs_tol=1e-9):
            raise ConversionError(
                f"CSV duration {times[-1]:g} h differs from filename duration {duration:g} h; "
                "use --terminal-zero keep for an intentional extended series."
            )
    if not interval_depths:
        raise ConversionError("No rainfall intervals remain after removing boundary placeholders.")

    return RainfallSeries(
        column_name=header[column_index],
        timestep_hours=timestep,
        values=tuple(interval_depths),
    )


def _format_number(value: float) -> str:
    return "0" if value == 0 else format(value, ".10g")


def build_stm(model_name: str, series: RainfallSeries, run_increments: int = DEFAULT_RUN_INCREMENTS) -> str:
    """Write the supported one-burst, one-pluviograph, uniform spatial storm format."""
    if not model_name.strip() or "\n" in model_name or "\r" in model_name or len(model_name) > 68:
        raise ConversionError("STM storm identification must be a non-empty single line of at most 68 characters.")
    if not 1 <= run_increments <= MAX_RORB_INCREMENTS:
        raise ConversionError(f"RORB run increments must be 1–{MAX_RORB_INCREMENTS} (RORBwin 6.52).")
    if run_increments < len(series.values):
        raise ConversionError(
            f"Run length ({run_increments}) is shorter than rainfall ({len(series.values)} intervals); "
            "increase --run-increments and allow for the runoff tail."
        )
    if not series.values or series.timestep_hours <= 0 or not math.isfinite(series.timestep_hours):
        raise ConversionError("Rainfall series must have a positive finite timestep and at least one interval.")
    if any(not math.isfinite(v) or v < 0 for v in series.values):
        raise ConversionError("Rainfall series contains a negative or non-finite depth.")
    dt = _format_number(series.timestep_hours)
    if series.timestep_hours.is_integer():
        dt = f"{series.timestep_hours:.1f}"
    lines = [
        model_name,
        "DESIGN",
        "C time increment (hours), run increments, bursts, pluviographs, uniform flag",
        f"{dt},{run_increments},1,1,0,-99",
        "C start and finish increment of the single rainfall burst",
        f"0,{len(series.values)}",
        "Pluviograph 1",
        *(_format_number(value) for value in series.values),
        "-99",
    ]
    return "\n".join(lines) + "\n"


def output_filename(csv_path: Path, column_name: str, *, override: str | None = None) -> str:
    """Make a safe STM filename; accept project-specific names by explicit override."""
    if override is not None:
        name = override if override.lower().endswith(".stm") else f"{override}.stm"
    else:
        catchment, aep, duration, tp = infer_output_details(csv_path, column_name)
        name = f"Storm_{catchment}_{aep}_{duration}m_{tp}.stm"
    try:
        return validate_output_filename(name)
    except ValueError as exc:
        raise ConversionError(str(exc)) from exc


def write_stm(path: Path, content: str, *, overwrite: bool = False) -> None:
    """Create a storm file without racing past the no-overwrite option."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w" if overwrite else "x", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
    except FileExistsError as exc:
        raise ConversionError(f"Output exists: {path}; pass --overwrite to replace it.") from exc
    except OSError as exc:
        raise ConversionError(f"Could not write STM {path}: {exc}") from exc
