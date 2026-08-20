"""Read and summarize ensemble RORB results stored as Parquet data."""

# moved from unsorted, not tested in production yet - 2026-08-20

from pathlib import Path
from typing import Sequence
import math

import pandas as pd
from loguru import logger


def read_rorb_parquet(parquet_path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """
    Reads a RORB ensemble results Parquet file into a Pandas DataFrame.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    logger.info("Reading RORB parquet data from {}", parquet_path)
    try:
        df = pd.read_parquet(parquet_path, columns=list(columns) if columns is not None else None)
        return df
    except Exception as e:
        logger.error("Failed to read parquet file {}: {}", parquet_path, e)
        raise


def calculate_peak_flows(
    df: pd.DataFrame,
    group_cols: list[str],
    flow_col: str = "Flow",
    peak_flow_col_name: str = "PeakFlow",
) -> pd.DataFrame:
    """
    Calculates the maximum flow (peak flow) for each group.
    Typically grouped by Location, AEP, Duration, TP, etc.
    """
    logger.info("Calculating peak flows grouped by {}", group_cols)

    if not group_cols:
        raise ValueError("At least one grouping column is required")
    missing_cols = [col for col in group_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing grouping columns in dataframe: {missing_cols}")

    if flow_col not in df.columns:
        raise ValueError(f"Missing flow column in dataframe: {flow_col}")
    if not pd.api.types.is_numeric_dtype(df[flow_col]):
        raise ValueError(f"Flow column must be numeric: {flow_col}")

    peak_flows: pd.DataFrame = df.groupby(group_cols, sort=False, as_index=False)[[flow_col]].max()
    return peak_flows.rename(columns={flow_col: peak_flow_col_name})


def calculate_closure_times(
    df: pd.DataFrame,
    threshold: float,
    group_cols: list[str],
    time_col: str = "Time",
    flow_col: str = "Flow",
    closure_col_name: str = "ClosureTime",
) -> pd.DataFrame:
    """
    Calculates the elapsed time from the first to last threshold exceedance in each group.

    This preserves the legacy script's definition of closure duration. It does not infer
    separate closure intervals when flows dip below the threshold between exceedances.
    """
    logger.info(
        "Calculating closure times for threshold {}, grouped by {}",
        threshold,
        group_cols,
    )

    if not group_cols:
        raise ValueError("At least one grouping column is required")
    if not math.isfinite(threshold):
        raise ValueError("Closure threshold must be finite")

    # Ensure required columns exist
    missing_cols = [col for col in group_cols + [time_col, flow_col] if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in dataframe: {missing_cols}")
    if not pd.api.types.is_numeric_dtype(df[time_col]):
        raise ValueError(f"Time column must be numeric: {time_col}")
    if not pd.api.types.is_numeric_dtype(df[flow_col]):
        raise ValueError(f"Flow column must be numeric: {flow_col}")

    records: list[dict[str, object]] = []
    grouper: str | list[str] = group_cols[0] if len(group_cols) == 1 else group_cols
    for group_key, group_df in df.groupby(grouper, sort=False, dropna=False):
        if len(group_cols) == 1:
            key_values = (group_key,)
        else:
            if not isinstance(group_key, tuple):
                raise TypeError("Expected a tuple key when grouping by multiple columns")
            key_values = group_key
        exceeded_times = pd.to_numeric(group_df.loc[group_df[flow_col] > threshold, time_col], errors="coerce").dropna()
        closure_time = 0.0 if exceeded_times.empty else float(exceeded_times.max() - exceeded_times.min())
        record: dict[str, object] = dict(zip(group_cols, key_values, strict=True))
        record[closure_col_name] = closure_time
        records.append(record)

    return pd.DataFrame.from_records(records, columns=[*group_cols, closure_col_name])
