# ryan_library/scripts/RORB/closure_durations.py
"""
Compute closure durations from RORB batch outputs or raw hydrograph CSVs.

Public API:
- run_closure_durations(...)
- run_closure_durations_from_csv(...)
"""

from datetime import datetime
from pathlib import Path
from collections.abc import Iterable
import re
import pandas as pd
from pandas import DataFrame
from loguru import logger

from ryan_library.functions.RORB.read_rorb_files import (
    find_batch_files,
    parse_batch_output,
    analyze_hydrograph,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.pandas.median_calc import median_stats as median_stats_func

__all__: list[str] = [
    "run_closure_durations",
    "run_closure_durations_from_csv",
]

# --- precompiled regexes -----------------------------------------------------

_AEP_SORT_RE: re.Pattern[str] = re.compile(pattern=r"([0-9]*\.?[0-9]+)")
_CSV_NAME_RE: re.Pattern[str] = re.compile(
    pattern=r"aep(?P<aep>[^_]+)_du(?P<duration>[^t]+)hourtp(?P<tp>\d+)",
    flags=re.IGNORECASE,
)


# --- helpers -----------------------------------------------------------------


def _default_thresholds() -> list[float]:
    """Default ascending flow thresholds (1..9, 10..98 step 2, 100..2090 step 10).
    Returned as a sorted list of float."""
    values: set[int] = set(range(1, 10)) | set(range(10, 100, 2)) | set(range(100, 2100, 10))
    return [float(v) for v in sorted(values)]


def _collect_batch_data(paths: Iterable[Path]) -> DataFrame:
    """Find and parse all batch.out files under the given paths."""
    batch_files: list[Path] = find_batch_files(paths=paths)
    if not batch_files:
        return pd.DataFrame()

    dfs: list[DataFrame] = [parse_batch_output(batchout_file=p) for p in batch_files]
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(objs=dfs, ignore_index=True) if dfs else pd.DataFrame()


def _process_hydrographs(batch_df: DataFrame, thresholds: list[float]) -> DataFrame:
    """Run analyze_hydrograph across each row of a parsed batch DataFrame."""
    records: list[DataFrame] = []
    # itertuples is materially faster than iterrows and keeps names
    for row in batch_df.itertuples(index=False):
        try:
            rec: DataFrame = analyze_hydrograph(
                aep=str(getattr(row, "AEP")),
                duration=str(getattr(row, "Duration")),
                tp=int(getattr(row, "TPat")),
                csv_path=Path(getattr(row, "csv")),
                out_path=Path(getattr(row, "Path")),
                thresholds=thresholds,
            )
        except Exception:
            logger.exception("analyze_hydrograph failed for row with csv=%s", getattr(row, "csv", "<unknown>"))
            continue

        if not rec.empty:
            records.append(rec)

    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


def _summarise_results(df: DataFrame) -> DataFrame:
    """
    Summarise exceedance durations to central/low/high/mean and critical TP/duration.

    Expects columns: out_path, Location, ThresholdFlow, AEP, Duration_Exceeding, TP, Duration
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Path",
                "Location",
                "ThresholdFlow",
                "AEP",
                "Central_Value",
                "Critical_Duration",
                "Critical_Tp",
                "Low_Value",
                "High_Value",
                "Average_Value",
                "Closest_Tpcrit",
                "Closest_Value",
            ]
        )

    out_rows: list[dict[str, object]] = []
    grouped = df.groupby(["out_path", "Location", "ThresholdFlow", "AEP"], dropna=False)

    for (out_path, location, threshold, aep), group in grouped:
        stats, _ = median_stats_func(thinned_df=group, stat_col="Duration_Exceeding", tp_col="TP", dur_col="Duration")

        out_rows.append(
            {
                "Path": out_path,
                "Location": location,
                "ThresholdFlow": threshold,
                "AEP": aep,
                "Central_Value": stats.get("median"),
                "Critical_Duration": stats.get("Duration"),
                "Critical_Tp": stats.get("Critical_TP"),
                "Low_Value": stats.get("low"),
                "High_Value": stats.get("high"),
                "Average_Value": stats.get("mean_including_zeroes"),
                # Keep existing semantics for the "closest" pair:
                "Closest_Tpcrit": stats.get("Critical_TP"),
                "Closest_Value": stats.get("median"),
            }
        )

    finaldb = pd.DataFrame(out_rows)
    # Ensure column order is stable for downstream consumers
    finaldb: DataFrame = finaldb[
        [
            "Path",
            "Location",
            "ThresholdFlow",
            "AEP",
            "Central_Value",
            "Critical_Duration",
            "Critical_Tp",
            "Low_Value",
            "High_Value",
            "Average_Value",
            "Closest_Tpcrit",
            "Closest_Value",
        ]
    ]
    return finaldb


def _export_results(result_df: DataFrame, summary_df: DataFrame) -> None:
    """Write detailed and summary outputs with a stable, timestamped prefix."""
    timestamp: str = datetime.now().strftime("%Y%m%d-%H%M")

    # Detailed outputs
    result_df.to_parquet(f"{timestamp}_durex.parquet.gzip", compression="gzip", index=False)
    result_df.to_csv(f"{timestamp}_durex.csv", index=False)

    # Sort summary in Path, Location, ThresholdFlow, numeric(AEP) order
    aep_key = pd.to_numeric(
        summary_df["AEP"].astype(str).str.extract(_AEP_SORT_RE)[0],
        errors="coerce",
    ).fillna(float("inf"))
    summary_df = (
        summary_df.assign(_aep_sort_key=aep_key)
        .sort_values(
            by=["Path", "Location", "ThresholdFlow", "_aep_sort_key"],
            ignore_index=True,
        )
        .drop(columns="_aep_sort_key")
    )

    summary_df.to_csv(f"{timestamp}_QvsTexc.csv", index=False)


# --- public entry points -----------------------------------------------------


def run_closure_durations(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    log_level: str = "INFO",
) -> None:
    """
    Process all ``batch.out`` files under ``paths`` and report closure durations.

    Parameters
    ----------
    paths
        Iterable of folders to search. Defaults to [Path.cwd()].
    thresholds
        Flow thresholds to test (same units as hydrograph flow). Defaults to a
        broad ascending list if None.
    log_level
        Console log level for Loguru (e.g., "INFO", "DEBUG").
    """
    search_paths: Iterable[Path] = paths or [Path.cwd()]
    threshold_values: list[float] = thresholds or _default_thresholds()

    with setup_logger(console_log_level=log_level):
        batch_df: DataFrame = _collect_batch_data(paths=search_paths)
        if batch_df.empty:
            logger.warning("No batch.out data found.")
            return

        result_df: DataFrame = _process_hydrographs(batch_df=batch_df, thresholds=threshold_values)
        if result_df.empty:
            logger.warning("No hydrograph data processed.")
            return

        summary_df: DataFrame = _summarise_results(df=result_df)
        _export_results(result_df=result_df, summary_df=summary_df)
        logger.info("Processing complete.")


def _collect_csv_runs(paths: Iterable[Path]) -> list[tuple[str, str, int, Path]]:
    """
    Return (aep, duration, tp, csv_path) tuples for hydrograph CSVs found under ``paths``.

    Expected filename pattern example: ``aep50_du168hourtp7.csv`` (spaces ignored).
    """
    csv_files: list[tuple[str, str, int, Path]] = []

    for root in paths:
        for csv_path in root.rglob("*.csv"):
            if not csv_path.is_file():
                continue

            match = _CSV_NAME_RE.search(csv_path.name.replace(" ", ""))
            if not match:
                logger.warning("Skipping CSV with unrecognised name: {}", csv_path)
                continue

            aep: str = match.group("aep").replace("p", ".")
            duration: str = match.group("duration").replace("_", ".")
            tp: int = int(match.group("tp"))
            csv_files.append((aep, duration, tp, csv_path))

    return csv_files


def run_closure_durations_from_csv(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    log_level: str = "INFO",
) -> None:
    """
    Process hydrograph CSV files under ``paths`` and report closure durations.

    This bypasses ``batch.out`` discovery and infers AEP/duration/TP from filenames.
    """
    search_paths: Iterable[Path] = paths or [Path.cwd()]
    threshold_values: list[float] = thresholds or _default_thresholds()

    with setup_logger(console_log_level=log_level):
        runs = _collect_csv_runs(paths=search_paths)
        if not runs:
            logger.warning("No CSV files found.")
            return

        records: list[DataFrame] = []
        for aep, duration, tp, csv_path in runs:
            try:
                rec: DataFrame = analyze_hydrograph(
                    aep=aep,
                    duration=duration,
                    tp=tp,
                    csv_path=csv_path,
                    out_path=csv_path,
                    thresholds=threshold_values,
                )
            except Exception:
                logger.exception("analyze_hydrograph failed for csv=%s", csv_path)
                continue

            if not rec.empty:
                records.append(rec)

        if not records:
            logger.warning("No hydrograph data processed.")
            return

        result_df: DataFrame = pd.concat(records, ignore_index=True)
        summary_df: DataFrame = _summarise_results(df=result_df)
        _export_results(result_df=result_df, summary_df=summary_df)
        logger.info("Processing complete.")
