# ryan_library/functions/tlf_missing_runs.py
"""Analyze TUFLOW run tables (AEP, Duration, TP) for missing sets or runs.

This module provides pure data transformations to determine which expected
model runs have not yet completed, based on a tracking table.
"""

from __future__ import annotations

__lazy_modules__ = ["pandas"]

from dataclasses import dataclass, asdict
from typing import Literal
import pandas as pd

# ---------- Core expectations ----------

ExpectedTP = Literal["TP01", "TP02", "TP03", "TP04", "TP05", "TP06", "TP07", "TP08", "TP09", "TP10"]
EXPECTED_TPS: tuple[ExpectedTP, ...] = (
    "TP01",
    "TP02",
    "TP03",
    "TP04",
    "TP05",
    "TP06",
    "TP07",
    "TP08",
    "TP09",
    "TP10",
)
type DimensionValue = str | int | float


# ---------- Data classes ----------


@dataclass(frozen=True, slots=True)
class CompletedSet:
    trim_run_code: str
    duration: DimensionValue
    aep: DimensionValue


@dataclass(frozen=True, slots=True)
class OutstandingSet:
    trim_run_code: str
    duration: DimensionValue
    aep: DimensionValue
    missing_tps: tuple[ExpectedTP, ...]


@dataclass(slots=True)
class AnalysisResult:
    no_sets: bool
    completed_sets: list[CompletedSet]
    outstanding_sets: list[OutstandingSet]
    expected_tps: tuple[ExpectedTP, ...] = EXPECTED_TPS


# ---------- Helpers ----------


def _standardize_tp(val: object) -> str:
    if val is None:
        return ""
    s: str = str(val).strip().upper()
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
    cols: dict[str, str] = {c.lower(): c for c in df.columns}
    for r in ("aep", "duration", "tp"):
        if r not in cols:
            raise KeyError(f"Required column '{r.upper()}' not found in DataFrame. Found: {list(df.columns)}")
    trc_key: str | None = None
    for cand in ("trim_run_code", "trim_runcode", "trim code", "trimcode", "trim"):
        if cand in cols:
            trc_key = cand
            break
    if trc_key is None:
        raise KeyError("Required column 'trim_run_code' (or alias 'trim_runcode') not found.")
    out = pd.DataFrame(
        data={
            "AEP": df[cols["aep"]],
            "Duration": df[cols["duration"]],
            "TP": df[cols["tp"]].map(func=_standardize_tp),
            "trim_run_code": df[cols[trc_key]],
        }
    )
    return out


def _coerce_dimension(value: object) -> DimensionValue:
    """Convert a pandas grouping key into a stable display and record value."""

    if isinstance(value, (str, int, float)):
        return value
    return str(value)


def _unique_sorted(series: pd.Series) -> list[DimensionValue]:
    vals: list[DimensionValue] = [_coerce_dimension(value) for value in series.dropna().unique().tolist()]
    try:
        nums: list[float] = [float(str(value)) for value in vals]
        order: list[tuple[float, str | int | float]] = sorted(zip(nums, vals))
        return [v for _, v in order]
    except ValueError:
        return sorted(vals, key=lambda x: str(x))


# ---------- Analysis ----------


def analyze_missing_runs(df: pd.DataFrame) -> AnalysisResult:
    work: pd.DataFrame = _normalize_columns(df).copy()
    work = work[work["TP"].isin(EXPECTED_TPS)].copy()
    work = work.drop_duplicates(subset=["trim_run_code", "Duration", "AEP", "TP"])

    completed: list[CompletedSet] = []
    outstanding: list[OutstandingSet] = []

    for (trc, dur, aep), sub in work.groupby(["trim_run_code", "Duration", "AEP"], dropna=False, sort=True):
        present = set(sub["TP"].unique().tolist())
        missing: list[ExpectedTP] = [tp for tp in EXPECTED_TPS if tp not in present]
        if not missing:
            completed.append(
                CompletedSet(
                    trim_run_code=str(trc),
                    duration=_coerce_dimension(dur),
                    aep=_coerce_dimension(aep),
                )
            )
        else:
            outstanding.append(
                OutstandingSet(
                    trim_run_code=str(trc),
                    duration=_coerce_dimension(dur),
                    aep=_coerce_dimension(aep),
                    missing_tps=tuple(missing),
                )
            )

    no_sets: bool = len(completed) == 0
    if no_sets:
        outstanding = []

    return AnalysisResult(no_sets=no_sets, completed_sets=completed, outstanding_sets=outstanding)


def to_summary_frames(result: AnalysisResult) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    frames["completed_sets"] = pd.DataFrame(
        data=[asdict(c) for c in result.completed_sets],
        columns=["trim_run_code", "Duration", "AEP"],
    ).sort_values(by=["trim_run_code", "Duration", "AEP"], ignore_index=True)

    if not result.no_sets and result.outstanding_sets:
        rows: list[dict[str, object]] = []
        for o in result.outstanding_sets:
            for tp in o.missing_tps:
                rows.append({"trim_run_code": o.trim_run_code, "Duration": o.duration, "AEP": o.aep, "MissingTP": tp})
        frames["outstanding_missing_tps"] = pd.DataFrame(data=rows).sort_values(
            by=["trim_run_code", "Duration", "AEP", "MissingTP"], ignore_index=True
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

    frames["per_trim_run_code_counts"] = pd.DataFrame(
        data=[{"trim_run_code": k, **v} for k, v in counts.items()]
    ).sort_values(by="trim_run_code", ignore_index=True)
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
    result: AnalysisResult = analyze_missing_runs(df)
    lines: list[str] = []
    rows: list[dict[str, object]] = []

    if result.no_sets:
        lines.append("No complete (AEP, Duration) sets found in the data. Missing lists suppressed by rule.")
        rows.append(
            {"trim_run_code": "ALL", "section": "notice", "message": "No complete sets; missing lists suppressed"}
        )
        return "\n".join(lines), pd.DataFrame(data=rows)

    work: pd.DataFrame = _normalize_columns(df).copy()
    work = work[work["TP"].isin(EXPECTED_TPS)].drop_duplicates(subset=["trim_run_code", "Duration", "AEP", "TP"])

    for trc, g in work.groupby("trim_run_code", sort=True):
        aeplist: list[DimensionValue] = _unique_sorted(g["AEP"])
        durlist: list[DimensionValue] = _unique_sorted(g["Duration"])

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
            any_present: bool = any(len(present_map.get((aep, d), set())) > 0 for d in durlist)
            if not any_present:
                msg: str = f"AEP {aep}: missing all durations"
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
                tps: set[str] = present_map.get((aep, dur), set())
                missing: list[str] = [tp for tp in EXPECTED_TPS if tp not in tps]
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

    table: pd.DataFrame = pd.DataFrame(data=rows, columns=["trim_run_code", "section", "message"]).reset_index(
        drop=True
    )
    return "\n".join(lines).rstrip(), table
