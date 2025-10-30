
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal
import pandas as pd

# ---------- Core expectations ----------

ExpectedTP = Literal["TP01","TP02","TP03","TP04","TP05","TP06","TP07","TP08","TP09","TP10"]
EXPECTED_TPS: tuple[ExpectedTP, ...] = tuple(f"TP{n:02d}" for n in range(1, 11))  # TP01..TP10


# ---------- Data classes ----------

@dataclass(frozen=True, slots=True)
class CompletedSet:
    trim_run_code: str
    duration: str | int | float
    aep: str | int | float

@dataclass(frozen=True, slots=True)
class OutstandingSet:
    trim_run_code: str
    duration: str | int | float
    aep: str | int | float
    missing_tps: tuple[str, ...]

@dataclass(slots=True)
class AnalysisResult:
    no_sets: bool
    completed_sets: list[CompletedSet]
    outstanding_sets: list[OutstandingSet]
    expected_tps: tuple[str, ...] = EXPECTED_TPS


# ---------- Helpers ----------

def _standardize_tp(val: object) -> str:
    if val is None:
        return ""
    s = str(val).strip().upper()
    if s.startswith("TP"):
        s = s[2:]
    try:
        n = int(s)
    except ValueError:
        return ""
    if 1 <= n <= 10:
        return f"TP{n:02d}"
    return ""

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    for r in ("aep", "duration", "tp"):
        if r not in cols:
            raise KeyError(f"Required column '{r.upper()}' not found in DataFrame. Found: {list(df.columns)}")
    trc_key = None
    for cand in ("trim_run_code", "trim_runcode", "trim code", "trimcode", "trim"):
        if cand in cols:
            trc_key = cand
            break
    if trc_key is None:
        raise KeyError("Required column 'trim_run_code' (or alias 'trim_runcode') not found.")
    out = pd.DataFrame({
        "AEP": df[cols["aep"]],
        "Duration": df[cols["duration"]],
        "TP": df[cols["tp"]].map(_standardize_tp),
        "trim_run_code": df[cols[trc_key]],
    })
    return out

def _unique_sorted(series: pd.Series) -> list:
    vals = series.dropna().unique().tolist()
    try:
        nums = [float(v) for v in vals]
        order = sorted(zip(nums, vals))
        return [v for _, v in order]
    except Exception:
        return sorted(vals, key=lambda x: str(x))


# ---------- Analysis ----------

def analyze_missing_runs(df: pd.DataFrame) -> AnalysisResult:
    work = _normalize_columns(df).copy()
    work = work[work["TP"].isin(EXPECTED_TPS)].copy()
    work = work.drop_duplicates(subset=["trim_run_code", "Duration", "AEP", "TP"])

    completed: list[CompletedSet] = []
    outstanding: list[OutstandingSet] = []

    for (trc, dur, aep), sub in work.groupby(["trim_run_code", "Duration", "AEP"], dropna=False, sort=True):
        present = set(sub["TP"].unique().tolist())
        missing = [tp for tp in EXPECTED_TPS if tp not in present]
        if not missing:
            completed.append(CompletedSet(trim_run_code=str(trc), duration=dur, aep=aep))
        else:
            outstanding.append(OutstandingSet(trim_run_code=str(trc), duration=dur, aep=aep, missing_tps=tuple(missing)))

    no_sets = len(completed) == 0
    if no_sets:
        outstanding = []

    return AnalysisResult(no_sets=no_sets, completed_sets=completed, outstanding_sets=outstanding)


def to_summary_frames(result: AnalysisResult) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    frames["completed_sets"] = pd.DataFrame(
        [asdict(c) for c in result.completed_sets],
        columns=["trim_run_code", "Duration", "AEP"],
    ).sort_values(["trim_run_code", "Duration", "AEP"], ignore_index=True)

    if not result.no_sets and result.outstanding_sets:
        rows: list[dict[str, object]] = []
        for o in result.outstanding_sets:
            for tp in o.missing_tps:
                rows.append({"trim_run_code": o.trim_run_code, "Duration": o.duration, "AEP": o.aep, "MissingTP": tp})
        frames["outstanding_missing_tps"] = pd.DataFrame(rows).sort_values(
            ["trim_run_code", "Duration", "AEP", "MissingTP"], ignore_index=True
        )
    else:
        frames["outstanding_missing_tps"] = pd.DataFrame(columns=["trim_run_code", "Duration", "AEP", "MissingTP"])

    counts: dict[str, dict[str, int]] = {}
    for c in result.completed_sets:
        counts.setdefault(c.trim_run_code, {"completed_sets": 0, "outstanding_sets": 0})
        counts[c.trim_run_code]["completed_sets"] += 1
    if not result.no_sets:
        for o in result.outstanding_sets:
            counts.setdefault(o.trim_run_code, {"completed_sets": 0, "outstanding_sets": 0})
            counts[o.trim_run_code]["outstanding_sets"] += 1

    frames["per_trim_run_code_counts"] = (
        pd.DataFrame([{"trim_run_code": k, **v} for k, v in counts.items()])
        .sort_values("trim_run_code", ignore_index=True)
    )
    return frames


# ---------- Concise presentation (CLI + single-table export) ----------

def summarize_for_cli(df: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    """
    Concise CLI summary + flattened DataFrame.
    - Lists AEPs and Durations per trim_run_code up front.
    - Rollups:
        * "AEP X: missing all durations"
        * "Duration Y: missing all AEPs"
        * Per (AEP, Duration):
            - "missing all TP" if none present
            - If many missing (>=6): "missing K TP (not listed)"
            - Else: list the TP codes explicitly
    - If there are no complete sets anywhere, missing lists are suppressed.
    """
    result = analyze_missing_runs(df)
    lines: list[str] = []
    rows: list[dict[str, object]] = []

    if result.no_sets:
        lines.append("No complete (AEP, Duration) sets found in the data. Missing lists suppressed by rule.")
        rows.append({"trim_run_code": "ALL", "section": "notice", "message": "No complete sets; missing lists suppressed"})
        return "\n".join(lines), pd.DataFrame(rows)

    work = _normalize_columns(df).copy()
    work = work[work["TP"].isin(EXPECTED_TPS)].drop_duplicates(subset=["trim_run_code", "Duration", "AEP", "TP"])

    for trc, g in work.groupby("trim_run_code", sort=True):
        aeplist = _unique_sorted(g["AEP"])
        durlist = _unique_sorted(g["Duration"])

        lines.append(f"=== {trc} ===")
        lines.append(f"AEPs: {aeplist}")
        lines.append(f"Durations: {durlist}")
        rows.append({"trim_run_code": trc, "section": "header", "message": f"AEPs: {aeplist}"})
        rows.append({"trim_run_code": trc, "section": "header", "message": f"Durations: {durlist}"})

        present_map: dict[tuple[object, object], set[str]] = {}
        for (aep, dur), sub in g.groupby(["AEP", "Duration"], dropna=False):
            present_map[(aep, dur)] = set(sub["TP"].unique().tolist())

        # AEP rollups
        for aep in aeplist:
            any_present = any(len(present_map.get((aep, d), set())) > 0 for d in durlist)
            if not any_present:
                msg = f"AEP {aep}: missing all durations"
                lines.append(msg)
                rows.append({"trim_run_code": trc, "section": "rollup_aep", "message": msg})

        # Duration rollups
        for dur in durlist:
            any_present = any(len(present_map.get((a, dur), set())) > 0 for a in aeplist)
            if not any_present:
                msg = f"Duration {dur}: missing all AEPs"
                lines.append(msg)
                rows.append({"trim_run_code": trc, "section": "rollup_duration", "message": msg})

        # Per-cell summaries
        for aep in aeplist:
            for dur in durlist:
                tps = present_map.get((aep, dur), set())
                missing = [tp for tp in EXPECTED_TPS if tp not in tps]
                if len(tps) == 0:
                    msg = f"AEP {aep}, Duration {dur}: missing all TP"
                    lines.append(msg)
                    rows.append({"trim_run_code": trc, "section": "cell", "message": msg})
                elif missing:
                    if len(missing) >= 6:
                        msg = f"AEP {aep}, Duration {dur}: missing {len(missing)} TP (not listed)"
                    else:
                        msg = f"AEP {aep}, Duration {dur}: missing {', '.join(missing)}"
                    lines.append(msg)
                    rows.append({"trim_run_code": trc, "section": "cell", "message": msg})

        lines.append("")  # spacer

    table = pd.DataFrame(rows, columns=["trim_run_code", "section", "message"]).reset_index(drop=True)
    return "\n".join(lines).rstrip(), table


# ---------- Optional CLI (no argparse) ----------

def main(input_path: str | None = None, sheet_name: str | int | None = 0) -> None:
    if input_path is None:
        print("No input provided. Import this module and call analyze_missing_runs(df) or summarize_for_cli(df).")
        return
    if str(input_path).lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(input_path, sheet_name=sheet_name)
    else:
        df = pd.read_csv(input_path)

    text, table = summarize_for_cli(df)
    print(text)
    base = str(input_path).rsplit(".", 1)[0]
    out_csv = f"{base}__missing_runs_summary.csv"
    table.to_csv(out_csv, index=False)
    print(f"\nWrote: {out_csv}")

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else None
    sheet = sys.argv[2] if len(sys.argv) > 2 else 0
    try:
        if isinstance(sheet, str) and sheet.isdigit():
            sheet = int(sheet)
    except Exception:
        sheet = 0
    main(path, sheet)
