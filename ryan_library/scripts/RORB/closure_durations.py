from datetime import datetime
from pathlib import Path
from collections.abc import Iterable
import re

from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger
from pandas import DataFrame

from ryan_library.functions.RORB.read_rorb_files import (
    find_batch_files,
    parse_batch_output,
    analyze_hydrograph,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.pandas.median_calc import median_stats as median_stats_func


def _collect_batch_data(paths: Iterable[Path]) -> pd.DataFrame:
    batch_files: list[Path] = find_batch_files(paths=paths)
    dfs: list[DataFrame] = [parse_batch_output(batchout_file=p) for p in batch_files]
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(objs=dfs, ignore_index=True) if dfs else pd.DataFrame()


def _process_hydrographs(batch_df: pd.DataFrame, thresholds: list[float]) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for _, row in batch_df.iterrows():
        rec: DataFrame = analyze_hydrograph(
            aep=str(row["AEP"]),
            duration=str(row["Duration"]),
            tp=int(row["TPat"]),
            csv_path=Path(row["csv"]),
            out_path=Path(row["Path"]),
            thresholds=thresholds,
        )
        if not rec.empty:
            records.append(rec)
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


def _summarise_results(df: pd.DataFrame) -> pd.DataFrame:
    final_columns: list[str] = [
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
    finaldb = pd.DataFrame(columns=final_columns)
    grouped = df.groupby(["out_path", "Location", "ThresholdFlow", "AEP"])
    for name, group in grouped:
        stats, _ = median_stats_func(group, "Duration_Exceeding", "TP", "Duration")
        row = list(name) + [
            stats.get("median"),
            stats.get("Duration"),
            stats.get("Critical_TP"),
            stats.get("low"),
            stats.get("high"),
            stats.get("mean_including_zeroes"),
            stats.get("Critical_TP"),
            stats.get("median"),
        ]
        finaldb.loc[len(finaldb)] = row
    finaldb.columns = final_columns
    return finaldb


def run_closure_durations(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    log_level: str = "INFO",
) -> None:
    """Process ``batch.out`` files under ``paths`` and report closure durations.

    Parameters can be overridden to specify custom folders or flow thresholds.
    ``paths`` defaults to the current working directory when ``None``.
    ``thresholds`` defaults to a wide range of increasing flow values.
    """
    if paths is None:
        paths = [Path.cwd()]
    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in values]
    threshold_values: list[float] = thresholds

    with setup_logger(console_log_level=log_level):
        batch_df: DataFrame = _collect_batch_data(paths=paths)
        if batch_df.empty:
            logger.warning("No batch.out data found.")
            return
        result_df: DataFrame = _process_hydrographs(batch_df=batch_df, thresholds=threshold_values)
        if result_df.empty:
            logger.warning("No hydrograph data processed.")
            return

        timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
        result_df.to_parquet(path=f"{timestamp}_durex.parquet.gzip", compression="gzip")
        result_df.to_csv(path_or_buf=f"{timestamp}_durex.csv", index=False)
        summary_df: DataFrame = _summarise_results(df=result_df)
        # ensure the export is in Path, Location, ThresholdFlow, AEP order:
        summary_df["AEP_sort_key"] = summary_df["AEP"].str.extract(r"([0-9]*\.?[0-9]+)")[0].astype(dtype=float)

        # now sort (including original AEP for display), then drop the helper column
        summary_df.sort_values(
            by=["Path", "Location", "ThresholdFlow", "AEP_sort_key"], ignore_index=True, inplace=True
        )
        summary_df.drop(columns="AEP_sort_key", inplace=True)
        summary_df.to_csv(path_or_buf=f"{timestamp}_QvsTexc.csv", index=False)
        logger.info("Processing complete")


def _collect_csv_runs(paths: Iterable[Path]) -> list[tuple[str, str, int, Path]]:
    """Return metadata tuples for hydrograph CSV files found under ``paths``.

    Each tuple contains the AEP label, duration (hours), temporal pattern number and
    path to the CSV file.
    """
    csv_files: list[tuple[str, str, int, Path]] = []
    pattern = re.compile(r"aep(?P<aep>[^_]+)_du(?P<duration>[^t]+)hourtp(?P<tp>\d+)", re.IGNORECASE)
    for root in paths:
        for csv_path in root.rglob("*.csv"):
            if not csv_path.is_file():
                continue
            match = pattern.search(csv_path.name.replace(" ", ""))
            if not match:
                logger.warning("Skipping CSV with unrecognised name: %s", csv_path)
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
    """Process hydrograph CSV files under ``paths`` and report closure durations.

    This variant bypasses ``batch.out`` files entirely and infers the AEP, duration
    and temporal pattern from each CSV filename (e.g. ``aep50_du168hourtp7``).
    """
    if paths is None:
        paths = [Path.cwd()]
    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in values]
    threshold_values: list[float] = thresholds

    with setup_logger(console_log_level=log_level):
        runs: list[tuple[str, str, int, Path]] = _collect_csv_runs(paths=paths)
        if not runs:
            logger.warning("No CSV files found.")
            return

        records: list[pd.DataFrame] = []
        for aep, duration, tp, csv_path in runs:
            rec: DataFrame = analyze_hydrograph(
                aep=aep,
                duration=duration,
                tp=tp,
                csv_path=csv_path,
                out_path=csv_path,
                thresholds=threshold_values,
            )
            if not rec.empty:
                records.append(rec)
        if not records:
            logger.warning("No hydrograph data processed.")
            return

        result_df: DataFrame = pd.concat(records, ignore_index=True)
        timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
        result_df.to_parquet(path=f"{timestamp}_durex.parquet.gzip", compression="gzip")
        result_df.to_csv(path_or_buf=f"{timestamp}_durex.csv", index=False)
        summary_df: DataFrame = _summarise_results(df=result_df)
        summary_df["AEP_sort_key"] = summary_df["AEP"].str.extract(r"([0-9]*\.?[0-9]+)")[0].astype(dtype=float)

        summary_df.sort_values(
            by=["Path", "Location", "ThresholdFlow", "AEP_sort_key"], ignore_index=True, inplace=True
        )
        summary_df.drop(columns="AEP_sort_key", inplace=True)
        summary_df.to_csv(path_or_buf=f"{timestamp}_QvsTexc.csv", index=False)
        logger.info("Processing complete")
