from __future__ import annotations

from datetime import datetime
from pathlib import Path
from collections.abc import Iterable

import pandas as pd
from pandas import DataFrame
from loguru import logger

from ryan_library.functions.tuflow.closure_durations import (
    analyze_po_file,
    find_po_files,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.pandas.median_calc import median_stats as median_stats_func


def _process_files(
    files: list[Path],
    thresholds: list[float],
    data_type: str,
    allowed_locations: set[str] | None,
) -> DataFrame:
    records: list[DataFrame] = []
    for file_path in files:
        rec: DataFrame = analyze_po_file(
            csv_path=file_path,
            thresholds=thresholds,
            data_type=data_type,
            allowed_locations=allowed_locations,
        )
        if not rec.empty:
            records.append(rec)
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


def _summarise_results(df: DataFrame) -> DataFrame:
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
    *,
    data_type: str = "Flow",
    allowed_locations: list[str] | None = None,
    log_level: str = "INFO",
) -> None:
    """Process ``*_PO.csv`` files under ``paths`` and report closure durations."""
    if paths is None:
        paths = [Path.cwd()]
    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in values]
    allowed_set: set[str] | None = set(allowed_locations) if allowed_locations else None

    with setup_logger(console_log_level=log_level):
        files: list[Path] = find_po_files(paths=paths)
        if not files:
            logger.warning("No PO CSV files found.")
            return
        result_df: DataFrame = _process_files(
            files=files,
            thresholds=thresholds,
            data_type=data_type,
            allowed_locations=allowed_set,
        )
        if result_df.empty:
            logger.warning("No hydrograph data processed.")
            return

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
