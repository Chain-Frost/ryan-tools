from datetime import datetime
from pathlib import Path
from collections.abc import Iterable

from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

from ryan_library.functions.RORB.read_rorb_files import (
    find_batch_files,
    parse_batch_output,
    analyze_hydrograph,
)

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from ryan_library.functions.loguru_helpers import setup_logger
else:  # pragma: no cover - runtime dynamic import
    from importlib import import_module

    setup_logger = import_module("ryan_library.functions.loguru_helpers").setup_logger
from ryan_library.functions.pandas.median_calc import median_stats as median_stats_func


def _collect_batch_data(paths: Iterable[Path]) -> pd.DataFrame:
    batch_files = find_batch_files(paths)
    dfs = [parse_batch_output(p) for p in batch_files]
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _process_hydrographs(batch_df: pd.DataFrame, thresholds: list[float]) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for _, row in batch_df.iterrows():
        rec = analyze_hydrograph(
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
    final_columns = [
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
        values = list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10))
        thresholds = [float(v) for v in values]
    threshold_values: list[float] = thresholds

    with setup_logger(console_log_level=log_level):
        batch_df = _collect_batch_data(paths)
        if batch_df.empty:
            logger.warning("No batch.out data found.")
            return
        result_df = _process_hydrographs(batch_df, threshold_values)
        if result_df.empty:
            logger.warning("No hydrograph data processed.")
            return

        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        result_df.to_parquet(f"{timestamp}_durex.parquet.gzip", compression="gzip")
        result_df.to_csv(f"{timestamp}_durex.csv", index=False)
        summary_df = _summarise_results(result_df)
        summary_df.to_csv(f"{timestamp}_QvsTexc.csv", index=False)
        logger.info("Processing complete")
