# ryan_library/functions/tuflow/po_timeseries_checks.py
"""Helpers for analyzing PO timeseries CSV outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from ryan_library.classes import tuflow_string_classes as tsc

# TODO: make it more like the other orchestrators for options etc. I don't think it is using the common pipeline either.


@dataclass(frozen=True, slots=True)
class PeakCheckConfig:
    datatype_include: Sequence[str]
    datatype_case_sensitive: bool
    location_include: Sequence[str]
    location_exclude: Sequence[str]
    location_case_sensitive: bool
    warn_2hours: float
    warn_1hour: float
    flat_tol: float


@dataclass(frozen=True, slots=True)
class StabilityCheckConfig:
    datatype_include: Sequence[str]
    datatype_case_sensitive: bool
    location_include: Sequence[str]
    location_exclude: Sequence[str]
    location_case_sensitive: bool
    flat_tol: float
    diff_rel_tol: float
    diff_abs_tol: float
    max_sign_changes: int
    min_points: int


@dataclass(slots=True)
class PeakCheckResult:
    file: str
    run_code: str
    run_meta: dict[str, str]
    datatype: str
    location: str
    peak_kind: str | None
    peak_value: float | None
    peak_time: float | None
    end_time: float | None
    hours_from_end: float | None
    start_value: float | None
    end_value: float | None
    end_minus_start: float | None
    peak_above_start: float | None
    end_pct_of_peak: float | None
    status: str


@dataclass(slots=True)
class StabilityCheckResult:
    file: str
    run_code: str
    run_meta: dict[str, str]
    datatype: str
    location: str
    status: str
    points: int | None
    sign_changes: int | None
    nonzero_steps: int | None
    delta_tol: float | None
    value_min: float | None
    value_max: float | None
    value_range: float | None
    start_time: float | None
    end_time: float | None
    time_span: float | None
    start_value: float | None
    end_value: float | None
    max_abs_step: float | None
    mean_abs_step: float | None


@dataclass(slots=True)
class PoCsvData:
    df: DataFrame
    time_hours: Series
    end_row_idx: int
    end_hours: float


@dataclass(slots=True)
class QCsvData:
    df: DataFrame
    time_hours: Series


def parse_run_meta_from_filename(path: Path) -> dict[str, str]:
    p = tsc.TuflowStringParser(file_path=path)
    meta: dict[str, str] = {
        "data_type": str(p.data_type) if p.data_type is not None else "",
        "raw_run_code": p.raw_run_code,
        "trim_run_code": p.trim_run_code,
    }
    if p.tp:
        meta["TP"] = p.tp.text_repr
        if p.tp.numeric_value is not None:
            meta["TP_num"] = str(p.tp.numeric_value)
    if p.duration:
        meta["Duration"] = p.duration.text_repr
        if p.duration.numeric_value is not None:
            meta["Duration_m"] = str(p.duration.numeric_value)
    if p.aep:
        meta["AEP"] = p.aep.text_repr
        if p.aep.numeric_value is not None:
            meta["AEP_value"] = str(p.aep.numeric_value)
    for k, v in p.run_code_parts.items():
        meta[k] = v
    return meta


def _normalize_value(value: str, case_sensitive: bool) -> str:
    cleaned = value.strip()
    return cleaned if case_sensitive else cleaned.lower()


def _normalize_filter(values: Sequence[str], case_sensitive: bool) -> set[str]:
    return {_normalize_value(value=value, case_sensitive=case_sensitive) for value in values if value.strip()}


def _datatype_allowed(dtype_name: str, include_set: set[str], case_sensitive: bool) -> bool:
    if not dtype_name:
        return False
    if not include_set:
        return False
    key = _normalize_value(value=dtype_name, case_sensitive=case_sensitive)
    return key in include_set


def _location_allowed(loc_name: str, include_set: set[str], exclude_set: set[str], case_sensitive: bool) -> bool:
    if not loc_name:
        return False
    key = _normalize_value(value=loc_name, case_sensitive=case_sensitive)
    inc_ok = (not include_set) or (key in include_set)
    exc_hit = key in exclude_set if exclude_set else False
    return inc_ok and not exc_hit


def _parse_po_csv(path: Path) -> tuple[PoCsvData | None, str | None, bool]:
    try:
        if path.stat().st_size == 0:
            return None, "EMPTY_FILE", True
    except Exception:
        pass

    try:
        df: DataFrame = pd.read_csv(  # type: ignore
            filepath_or_buffer=path,
            header=[0, 1],
            low_memory=False,
            dtype=str,
            on_bad_lines="skip",
            engine="c",
        )
    except pd.errors.EmptyDataError:
        return None, "NO_COLUMNS", True
    except UnicodeDecodeError:
        return None, "DECODE_FAIL", True
    except Exception:
        return None, "CSV_PARSE_FAIL", True

    if not isinstance(df.columns, pd.MultiIndex):
        return None, "BAD_HEADER", True

    keep: Series[bool] = ~df.columns.to_frame().isna().all(axis=1)
    df = df.loc[:, keep]

    if df.shape[1] == 0:
        return None, None, False
    df = df.iloc[:, 1:]
    if df.shape[1] < 1:
        return None, None, False

    time_hours = pd.to_numeric(df.iloc[:, 0], errors="coerce")  # type: ignore
    time_valid = time_hours.dropna()
    if time_valid.empty:
        return None, "TIME_PARSE_FAIL", True

    end_row_idx = int(time_valid.index[-1])
    end_hours = float(time_valid.iloc[-1])
    return PoCsvData(df=df, time_hours=time_hours, end_row_idx=end_row_idx, end_hours=end_hours), None, False


def _parse_q_csv(path: Path) -> tuple[QCsvData | None, str | None, bool]:
    try:
        if path.stat().st_size == 0:
            return None, "EMPTY_FILE", True
    except Exception:
        pass

    try:
        df: DataFrame = pd.read_csv(  # type: ignore
            filepath_or_buffer=path,
            header=0,
            low_memory=False,
            dtype=str,
            skipinitialspace=True,
            encoding="utf-8",
            on_bad_lines="skip",
            engine="c",
        )
    except pd.errors.EmptyDataError:
        return None, "NO_COLUMNS", True
    except UnicodeDecodeError:
        return None, "DECODE_FAIL", True
    except Exception:
        return None, "CSV_PARSE_FAIL", True

    if df.empty:
        return None, "NO_DATA", True

    df = df.dropna(axis=1, how="all")
    if df.shape[1] == 0:
        return None, "NO_COLUMNS", True

    first_column = str(df.columns[0]).strip()
    time_aliases: set[str] = {"Time", "Time (h)", "Time(h)"}
    if first_column not in time_aliases and df.shape[1] > 1:
        df = df.iloc[:, 1:].copy()

    rename_columns: dict[object, str] = {
        column: "Time" for column in df.columns if str(column).strip() in {"Time (h)", "Time(h)"}
    }
    if rename_columns:
        df.rename(columns=rename_columns, inplace=True)

    if "Time" not in df.columns:
        return None, "TIME_PARSE_FAIL", True

    cleaned_columns: list[str] = []
    for column in df.columns:
        column_text = str(column).strip()
        if column_text == "Time":
            cleaned_columns.append("Time")
            continue
        if column_text.startswith("Q "):
            column_text = column_text[2:].strip()
        if "[" in column_text and "]" in column_text:
            column_text = column_text.split("[", maxsplit=1)[0].strip()
        cleaned_columns.append(column_text)
    df.columns = cleaned_columns

    time_hours = pd.to_numeric(df["Time"], errors="coerce")  # type: ignore
    if time_hours.dropna().empty:
        return None, "TIME_PARSE_FAIL", True

    value_columns = [column for column in df.columns if column != "Time"]
    if not value_columns:
        return None, None, False

    return QCsvData(df=df, time_hours=time_hours), None, False


def _build_empty_stability_result(
    *,
    path: Path,
    run_code: str,
    run_meta: dict[str, str],
    status: str,
    datatype: str = "",
    location: str = "",
    points: int | None = None,
) -> StabilityCheckResult:
    return StabilityCheckResult(
        file=str(path),
        run_code=run_code,
        run_meta=run_meta,
        datatype=datatype,
        location=location,
        status=status,
        points=points,
        sign_changes=None,
        nonzero_steps=None,
        delta_tol=None,
        value_min=None,
        value_max=None,
        value_range=None,
        start_time=None,
        end_time=None,
        time_span=None,
        start_value=None,
        end_value=None,
        max_abs_step=None,
        mean_abs_step=None,
    )


def _evaluate_stability_series(
    *,
    path: Path,
    run_code: str,
    run_meta: dict[str, str],
    datatype: str,
    location: str,
    values_raw: Series,
    time_hours: Series,
    config: StabilityCheckConfig,
) -> StabilityCheckResult:
    valid_mask = values_raw.notna() & time_hours.notna()
    points = int(valid_mask.sum())

    if points == 0:
        return _build_empty_stability_result(
            path=path,
            run_code=run_code,
            run_meta=run_meta,
            status="NO_DATA",
            datatype=datatype,
            location=location,
            points=0,
        )

    values_valid = values_raw.loc[valid_mask].astype("float64")
    time_valid = time_hours.loc[valid_mask].astype("float64")
    start_time = float(time_valid.iloc[0])
    end_time = float(time_valid.iloc[-1])
    start_value = float(values_valid.iloc[0])
    end_value = float(values_valid.iloc[-1])
    value_min = float(values_valid.min())
    value_max = float(values_valid.max())
    value_range = value_max - value_min

    if points < max(config.min_points, 2):
        return StabilityCheckResult(
            file=str(path),
            run_code=run_code,
            run_meta=run_meta,
            datatype=datatype,
            location=location,
            status="INSUFFICIENT_POINTS",
            points=points,
            sign_changes=None,
            nonzero_steps=None,
            delta_tol=None,
            value_min=value_min,
            value_max=value_max,
            value_range=value_range,
            start_time=start_time,
            end_time=end_time,
            time_span=end_time - start_time,
            start_value=start_value,
            end_value=end_value,
            max_abs_step=None,
            mean_abs_step=None,
        )

    if value_range <= config.flat_tol:
        return StabilityCheckResult(
            file=str(path),
            run_code=run_code,
            run_meta=run_meta,
            datatype=datatype,
            location=location,
            status="FLAT",
            points=points,
            sign_changes=0,
            nonzero_steps=0,
            delta_tol=None,
            value_min=value_min,
            value_max=value_max,
            value_range=value_range,
            start_time=start_time,
            end_time=end_time,
            time_span=end_time - start_time,
            start_value=start_value,
            end_value=end_value,
            max_abs_step=None,
            mean_abs_step=None,
        )

    delta_tol: float = max(config.diff_abs_tol, config.diff_rel_tol * value_range)
    diffs = np.diff(values_valid.to_numpy(dtype=float))
    abs_diffs = np.abs(diffs)
    max_abs_step: float | None = float(abs_diffs.max()) if abs_diffs.size else None
    mean_abs_step: float | None = float(abs_diffs.mean()) if abs_diffs.size else None

    if abs_diffs.size == 0:
        sign_changes = 0
        nonzero_steps = 0
    else:
        signs = np.sign(diffs)
        signs[abs_diffs <= delta_tol] = 0
        nonzero = signs[signs != 0]
        nonzero_steps = int(nonzero.size)
        if nonzero.size < 2:
            sign_changes = 0
        else:
            sign_changes = int(np.sum(nonzero[1:] * nonzero[:-1] < 0))

    status: str = "UNSTABLE" if sign_changes > config.max_sign_changes else "OK"

    return StabilityCheckResult(
        file=str(path),
        run_code=run_code,
        run_meta=run_meta,
        datatype=datatype,
        location=location,
        status=status,
        points=points,
        sign_changes=sign_changes,
        nonzero_steps=nonzero_steps,
        delta_tol=delta_tol,
        value_min=value_min,
        value_max=value_max,
        value_range=value_range,
        start_time=start_time,
        end_time=end_time,
        time_span=end_time - start_time,
        start_value=start_value,
        end_value=end_value,
        max_abs_step=max_abs_step,
        mean_abs_step=mean_abs_step,
    )


def analyze_peak_csv(path: Path, config: PeakCheckConfig) -> list[PeakCheckResult]:
    results: list[PeakCheckResult] = []

    run_meta: dict[str, str] = parse_run_meta_from_filename(path)
    run_code: str = run_meta.get("trim_run_code", path.stem)

    parsed, status, emit_row = _parse_po_csv(path=path)
    if parsed is None:
        if status and emit_row:
            results.append(
                PeakCheckResult(
                    file=str(path),
                    run_code=run_code,
                    run_meta=run_meta,
                    datatype="",
                    location="",
                    peak_kind=None,
                    peak_value=None,
                    peak_time=None,
                    end_time=None,
                    hours_from_end=None,
                    start_value=None,
                    end_value=None,
                    end_minus_start=None,
                    peak_above_start=None,
                    end_pct_of_peak=None,
                    status=status,
                )
            )
        return results

    dtype_include = _normalize_filter(config.datatype_include, config.datatype_case_sensitive)
    loc_include = _normalize_filter(config.location_include, config.location_case_sensitive)
    loc_exclude = _normalize_filter(config.location_exclude, config.location_case_sensitive)

    df = parsed.df
    time_hours = parsed.time_hours
    end_row_idx = parsed.end_row_idx
    end_hours = parsed.end_hours

    time_col_idx = 0
    for j, (dtype_name, loc_name) in enumerate(df.columns):
        if j == time_col_idx:
            continue

        dtype_str = "" if pd.isna(dtype_name) else str(dtype_name).strip()
        loc_str = "" if pd.isna(loc_name) else str(loc_name).strip()

        if not _datatype_allowed(dtype_str, dtype_include, config.datatype_case_sensitive):
            continue
        if not _location_allowed(loc_str, loc_include, loc_exclude, config.location_case_sensitive):
            continue

        values_raw = pd.to_numeric(df.iloc[:, j], errors="coerce")  # type: ignore
        if values_raw.notna().sum() == 0:
            results.append(
                PeakCheckResult(
                    file=str(path),
                    run_code=run_code,
                    run_meta=run_meta,
                    datatype=dtype_str,
                    location=loc_str,
                    peak_kind=None,
                    peak_value=None,
                    peak_time=None,
                    end_time=end_hours,
                    hours_from_end=None,
                    start_value=None,
                    end_value=None,
                    end_minus_start=None,
                    peak_above_start=None,
                    end_pct_of_peak=None,
                    status="NO_DATA",
                )
            )
            continue

        first_valid = values_raw.dropna()
        start_value = float(first_valid.iloc[0])

        end_cell = values_raw.iloc[end_row_idx]
        end_value: float | None = float(end_cell) if pd.notna(end_cell) else None
        end_minus_start: float | None = (end_value - start_value) if end_value is not None else None

        values_rel = values_raw - start_value
        abs_rel = values_rel.abs()
        try:
            peak_idx = int(abs_rel.idxmax(skipna=True))
        except Exception:
            peak_idx = int(np.nanargmax(abs_rel.to_numpy()))  # type: ignore

        peak_rel = values_rel.iloc[peak_idx]
        peak_value = values_raw.iloc[peak_idx]
        peak_time_val = time_hours.iloc[peak_idx]

        peak_rel_f: float | None = float(peak_rel) if pd.notna(peak_rel) else None
        peak_value_f: float | None = float(peak_value) if pd.notna(peak_value) else None
        peak_hours_f: float | None = float(peak_time_val) if pd.notna(peak_time_val) else None

        if peak_rel_f is None or abs(peak_rel_f) <= config.flat_tol:
            peak_kind = "flat"
        else:
            peak_kind = "max" if peak_rel_f > 0.0 else "min"

        denom: float | None = abs(peak_value_f - start_value) if (peak_value_f is not None) else None
        numer: float | None = abs(end_value - start_value) if (end_value is not None) else None
        if denom is not None and denom > config.flat_tol and numer is not None:
            end_pct_of_peak: float | None = 100.0 * (numer / denom)
        elif denom is not None and denom <= config.flat_tol:
            end_pct_of_peak = 0.0 if (numer is not None and numer <= config.flat_tol) else None
        else:
            end_pct_of_peak = None

        peak_above_start: float | None = (peak_value_f - start_value) if (peak_value_f is not None) else None

        if peak_hours_f is None:
            status = "TIME_PARSE_FAIL"
            hours_from_end: float | None = None
        else:
            hours_from_end = end_hours - peak_hours_f
            status = (
                "WARN_1H"
                if hours_from_end < config.warn_1hour
                else ("WARN_2H" if hours_from_end < config.warn_2hours else "OK")
            )

        results.append(
            PeakCheckResult(
                file=str(path),
                run_code=run_code,
                run_meta=run_meta,
                datatype=dtype_str,
                location=loc_str,
                peak_kind=peak_kind,
                peak_value=peak_value_f,
                peak_time=peak_hours_f,
                end_time=end_hours,
                hours_from_end=hours_from_end,
                start_value=start_value,
                end_value=end_value,
                end_minus_start=end_minus_start,
                peak_above_start=peak_above_start,
                end_pct_of_peak=end_pct_of_peak,
                status=status,
            )
        )

    return results


def analyze_stability_csv(path: Path, config: StabilityCheckConfig) -> list[StabilityCheckResult]:
    results: list[StabilityCheckResult] = []

    run_meta: dict[str, str] = parse_run_meta_from_filename(path)
    run_code: str = run_meta.get("trim_run_code", path.stem)

    parsed, status, emit_row = _parse_po_csv(path=path)
    if parsed is None:
        if status and emit_row:
            results.append(
                _build_empty_stability_result(
                    path=path,
                    run_code=run_code,
                    run_meta=run_meta,
                    status=status,
                )
            )
        return results

    dtype_include: set[str] = _normalize_filter(config.datatype_include, config.datatype_case_sensitive)
    loc_include: set[str] = _normalize_filter(config.location_include, config.location_case_sensitive)
    loc_exclude: set[str] = _normalize_filter(config.location_exclude, config.location_case_sensitive)

    df: DataFrame = parsed.df
    time_hours = parsed.time_hours

    time_col_idx = 0
    for j, (dtype_name, loc_name) in enumerate(df.columns):
        if j == time_col_idx:
            continue

        dtype_str = "" if pd.isna(dtype_name) else str(dtype_name).strip()
        loc_str = "" if pd.isna(loc_name) else str(loc_name).strip()

        if not _datatype_allowed(dtype_str, dtype_include, config.datatype_case_sensitive):
            continue
        if not _location_allowed(loc_str, loc_include, loc_exclude, config.location_case_sensitive):
            continue

        values_raw = pd.to_numeric(df.iloc[:, j], errors="coerce")  # type: ignore
        results.append(
            _evaluate_stability_series(
                path=path,
                run_code=run_code,
                run_meta=run_meta,
                datatype=dtype_str,
                location=loc_str,
                values_raw=values_raw,
                time_hours=time_hours,
                config=config,
            )
        )

    return results


def analyze_stability_q_csv(path: Path, config: StabilityCheckConfig) -> list[StabilityCheckResult]:
    results: list[StabilityCheckResult] = []

    run_meta: dict[str, str] = parse_run_meta_from_filename(path)
    run_code: str = run_meta.get("trim_run_code", path.stem)

    parsed, status, emit_row = _parse_q_csv(path=path)
    if parsed is None:
        if status and emit_row:
            results.append(
                _build_empty_stability_result(
                    path=path,
                    run_code=run_code,
                    run_meta=run_meta,
                    status=status,
                    datatype="Q",
                )
            )
        return results

    dtype_include: set[str] = _normalize_filter(config.datatype_include, config.datatype_case_sensitive)
    loc_include: set[str] = _normalize_filter(config.location_include, config.location_case_sensitive)
    loc_exclude: set[str] = _normalize_filter(config.location_exclude, config.location_case_sensitive)

    if not _datatype_allowed("Q", dtype_include, config.datatype_case_sensitive):
        return results

    for column in parsed.df.columns:
        if column == "Time":
            continue

        loc_str = str(column).strip()
        if not _location_allowed(loc_str, loc_include, loc_exclude, config.location_case_sensitive):
            continue

        values_raw = pd.to_numeric(parsed.df[column], errors="coerce")  # type: ignore
        results.append(
            _evaluate_stability_series(
                path=path,
                run_code=run_code,
                run_meta=run_meta,
                datatype="Q",
                location=loc_str,
                values_raw=values_raw,
                time_hours=parsed.time_hours,
                config=config,
            )
        )

    return results


def flatten_peak_results(results: Sequence[PeakCheckResult]) -> list[dict[str, object]]:
    return [_flatten_result(result) for result in results]


def flatten_stability_results(results: Sequence[StabilityCheckResult]) -> list[dict[str, object]]:
    return [_flatten_result(result) for result in results]


def _flatten_result(result: PeakCheckResult | StabilityCheckResult) -> dict[str, object]:
    base: dict[str, Any] = asdict(result)
    meta = base.pop("run_meta", {})
    row: dict[str, object] = base
    for k, v in meta.items():
        if k in row:
            row[f"{k}_meta"] = v
        else:
            row[k] = v
    return row
