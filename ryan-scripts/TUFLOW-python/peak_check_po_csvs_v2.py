# peak_check_po_csvs.py


from dataclasses import dataclass, asdict
from pathlib import Path
import concurrent.futures as cf
import os

import numpy as np
import pandas as pd

from pandas import DataFrame, Series
from ryan_library.classes import tuflow_string_classes as tsc
from ryan_library.functions.misc_functions import ExcelExporter  # hard requirement
from ryan_library.scripts.wrapper_utils import print_library_version

# -------------------------
# Configuration
# -------------------------
WORKING_DIR: Path = Path(__file__).resolve().parent
CSV_GLOB: str = "**/*_PO.csv"

# Datatype filter (include-list). Default: only 'Flow'.
DATATYPE_INCLUDE: list[str] = ["Flow"]
DATATYPE_CASE_SENSITIVE: bool = False

# Location filter (exact include-list). Empty => no include filter.
LOCATION_INCLUDE: list[str] = []  # e.g. ["PO-009", "PO-010", "PO-011"]
LOCATION_EXCLUDE: list[str] = []
LOCATION_CASE_SENSITIVE: bool = False

# Warning thresholds (hours from end)
WARN_2HOURS: float = 2.0
WARN_1HOUR: float = 1.0

# Treat deviations <= this as "flat"
FLAT_TOL: float = 1e-6

# Multiprocessing
n: int | None = os.cpu_count()
MAX_WORKERS: int | None = (max(n - 1, 1)) if n is not None else None
CHUNKSIZE: int = 1


# -------------------------
# Result schema
# -------------------------
@dataclass(slots=True)
class PeakCheckResult:
    file: str
    run_code: str
    run_meta: dict[str, str]  # flattened later (no prefix)
    datatype: str
    location: str

    # Peak info (relative to start baseline)
    peak_kind: str | None  # "max" | "min" | "flat" | None
    peak_value: float | None  # signed value at peak index
    peak_time: float | None  # hours at peak index

    # End-of-series timing
    end_time: float | None  # last valid time (hours)
    hours_from_end: float | None

    # Baseline-corrected metrics
    start_value: float | None  # signed
    end_value: float | None  # signed
    end_minus_start: float | None  # signed (end - start)
    peak_above_start: float | None  # signed (peak - start)
    end_pct_of_peak: float | None  # % based on absolute magnitudes

    status: str  # "OK", "WARN_2H", "WARN_1H", "NO_DATA", "TIME_PARSE_FAIL", etc.


# -------------------------
# Filters
# -------------------------
def _norm(vals: list[str], case_sensitive: bool) -> list[str]:
    return vals if case_sensitive else [v.lower() for v in vals]


_DT_INC: list[str] = _norm(DATATYPE_INCLUDE, DATATYPE_CASE_SENSITIVE)
_LOC_INC: list[str] = _norm(LOCATION_INCLUDE, LOCATION_CASE_SENSITIVE)
_LOC_EXC: list[str] = _norm(LOCATION_EXCLUDE, LOCATION_CASE_SENSITIVE)


def _datatype_allowed(dtype_name: str) -> bool:
    if not dtype_name:
        return False
    key: str = dtype_name if DATATYPE_CASE_SENSITIVE else dtype_name.strip().lower()
    return key in _DT_INC


def _location_allowed(loc_name: str) -> bool:
    if not loc_name:
        return False
    key: str = loc_name if LOCATION_CASE_SENSITIVE else loc_name.strip().lower()
    inc_ok: bool = (not _LOC_INC) or (key in _LOC_INC)
    exc_hit: bool = (key in _LOC_EXC) if _LOC_EXC else False
    return inc_ok and not exc_hit


# -------------------------
# Filename metadata
# -------------------------
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
    for k, v in p.run_code_parts.items():  # e.g. R01, R02, ...
        meta[k] = v
    return meta


# -------------------------
# Core analysis (hardened)
# -------------------------
def analyze_csv(path: Path) -> list[PeakCheckResult]:
    """
    CSV layout:
      - Row 1: datatype (header level 0)
      - Row 2: location (header level 1)
      - Row 3+: data
      - Column A: ignore
      - Column B: time in DECIMAL HOURS
    """
    results: list[PeakCheckResult] = []

    run_meta: dict[str, str] = parse_run_meta_from_filename(path)
    run_code: str = run_meta.get("trim_run_code", path.stem)

    # Quick empty-file guard
    try:
        if path.stat().st_size == 0:
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
                    status="EMPTY_FILE",
                )
            )
            return results
    except Exception:
        pass  # let read_csv try

    # Read with guards
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
                status="NO_COLUMNS",
            )
        )
        return results
    except UnicodeDecodeError:
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
                status="DECODE_FAIL",
            )
        )
        return results
    except Exception:
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
                status="CSV_PARSE_FAIL",
            )
        )
        return results

    if not isinstance(df.columns, pd.MultiIndex):
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
                status="BAD_HEADER",
            )
        )
        return results

    # Drop blank header columns
    keep: Series[bool] = ~df.columns.to_frame().isna().all(axis=1)
    df = df.loc[:, keep]

    # Drop Column A unconditionally
    if df.shape[1] == 0:
        return results
    df = df.iloc[:, 1:]
    if df.shape[1] < 1:
        return results

    # Time column (decimal hours)
    time_col_idx = 0
    time_hours = pd.to_numeric(df.iloc[:, time_col_idx], errors="coerce")  # type: ignore
    time_valid = time_hours.dropna()

    if time_valid.empty:
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
                status="TIME_PARSE_FAIL",
            )
        )
        return results

    end_row_idx = int(time_valid.index[-1])
    end_hours = float(time_valid.iloc[-1])

    # Iterate over data columns (skip time column 0)
    for j, (dtype_name, loc_name) in enumerate(df.columns):
        if j == time_col_idx:
            continue

        dtype_str = "" if pd.isna(dtype_name) else str(dtype_name).strip()
        loc_str = "" if pd.isna(loc_name) else str(loc_name).strip()

        if not _datatype_allowed(dtype_str):
            continue
        if not _location_allowed(loc_str):
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

        # Baseline = first valid value
        first_valid = values_raw.dropna()
        start_value = float(first_valid.iloc[0])

        # End value aligned to last valid time row
        end_cell = values_raw.iloc[end_row_idx]
        end_value: float | None = float(end_cell) if pd.notna(end_cell) else None
        end_minus_start: float | None = (end_value - start_value) if end_value is not None else None

        # Peak index = argmax(|values_raw - start_value|)
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

        # peak_kind with tolerance
        if peak_rel_f is None or abs(peak_rel_f) <= FLAT_TOL:
            peak_kind = "flat"
        else:
            peak_kind = "max" if peak_rel_f > 0.0 else "min"

        # % of peak at end (abs magnitudes)
        denom: float | None = abs(peak_value_f - start_value) if (peak_value_f is not None) else None
        numer: float | None = abs(end_value - start_value) if (end_value is not None) else None
        if denom is not None and denom > FLAT_TOL and numer is not None:
            end_pct_of_peak: float | None = 100.0 * (numer / denom)
        elif denom is not None and denom <= FLAT_TOL:
            end_pct_of_peak = 0.0 if (numer is not None and numer <= FLAT_TOL) else None
        else:
            end_pct_of_peak = None

        peak_above_start: float | None = (peak_value_f - start_value) if (peak_value_f is not None) else None

        # Warning status based on proximity of peak to end
        if peak_hours_f is None:
            status = "TIME_PARSE_FAIL"
            hours_from_end: float | None = None
        else:
            hours_from_end = end_hours - peak_hours_f
            status: str = (
                "WARN_1H" if hours_from_end < WARN_1HOUR else ("WARN_2H" if hours_from_end < WARN_2HOURS else "OK")
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


# -------------------------
# Worker wrapper (picklable + last-resort guard)
# -------------------------
def _analyze_one(path_str: str) -> list[dict[str, object]]:
    p = Path(path_str)
    try:
        rows = analyze_csv(p)
    except Exception:
        # Ensure a single bad file never kills the pool
        run_meta: dict[str, str] = parse_run_meta_from_filename(p)
        fallback = PeakCheckResult(
            file=str(p),
            run_code=run_meta.get("raw_run_code", p.stem),
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
            status="WORKER_FAIL",
        )
        rows: list[PeakCheckResult] = [fallback]

    out_rows: list[dict[str, object]] = []
    for r in rows:
        base = asdict(r)
        meta = base.pop("run_meta", {})
        row: dict[str, object] = base  # keep base fields first
        for k, v in meta.items():  # flatten WITHOUT any prefix
            if k in row:
                row[f"{k}_meta"] = v
            else:
                row[k] = v
        out_rows.append(row)
    return out_rows


# -------------------------
# Runner
# -------------------------
def main() -> None:
    files: list[Path] = sorted(WORKING_DIR.rglob(CSV_GLOB))
    if not files:
        print(f"[INFO] No files matched '{CSV_GLOB}' under {WORKING_DIR}")
        return

    all_rows: list[dict[str, object]] = []
    with cf.ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for rows in ex.map(_analyze_one, (str(p) for p in files), chunksize=CHUNKSIZE):
            if rows:
                all_rows.extend(rows)

    if not all_rows:
        print("[INFO] No matching data columns after filters.")
        return

    out_df = pd.DataFrame(data=all_rows)

    # Column order: key metrics first, then everything else
    first_cols: list[str] = [
        "run_code",
        "status",
        "datatype",
        "location",
        "peak_kind",
        "peak_value",
        "peak_time",
        "end_time",
        "hours_from_end",
        "start_value",
        "end_value",
        "end_minus_start",
        "peak_above_start",
        "end_pct_of_peak",
        # common meta fields (no prefix)
        "raw_run_code",
        "trim_run_code",
        "data_type",
        "TP",
        "TP_num",
        "Duration",
        "Duration_m",
        "AEP",
        "AEP_value",
        "file",
    ]
    ordered: list[str] = [c for c in first_cols if c in out_df.columns] + [
        c for c in out_df.columns if c not in first_cols
    ]
    out_df: pd.DataFrame = out_df.reindex(columns=ordered)

    # Strict Excel export via ExcelExporter (timestamped filename in WORKING_DIR)
    ExcelExporter().save_to_excel(
        data_frame=out_df,
        file_name_prefix="peaks_summary",
        sheet_name="Summary",
        output_directory=WORKING_DIR,
    )
    print(f"[OK] Wrote Excel summary in: {WORKING_DIR}")
    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
