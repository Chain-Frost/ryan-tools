from pathlib import Path
from typing import Sequence

import pandas as pd
from loguru import logger


def read_rorb_parquet(parquet_path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """
    Reads a RORB ensemble results Parquet file into a Pandas DataFrame.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    logger.info(f"Reading RORB parquet data from {parquet_path}")
    try:
        df = pd.read_parquet(parquet_path, columns=columns)
        return df
    except Exception as e:
        logger.error(f"Failed to read parquet file {parquet_path}: {e}")
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
    logger.info(f"Calculating peak flows grouped by {group_cols}")
    
    # Ensure group_cols exist in dataframe
    missing_cols = [col for col in group_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing grouping columns in dataframe: {missing_cols}")

    peakf = df.groupby(group_cols, sort=False, as_index=False)[flow_col].max()
    peakf.rename(columns={flow_col: peak_flow_col_name}, inplace=True)
    return peakf


def calculate_closure_times(
    df: pd.DataFrame,
    threshold: float,
    group_cols: list[str],
    time_col: str = "Time",
    flow_col: str = "Flow",
    closure_col_name: str = "ClosureTime",
) -> pd.DataFrame:
    """
    Calculates the total time (closure time) where the flow exceeds a given threshold,
    for each group. Assumes uniform time steps within each group for the integration.
    """
    logger.info(f"Calculating closure times (exceedance duration) for threshold {threshold}, grouped by {group_cols}")
    
    # Ensure required columns exist
    missing_cols = [col for col in group_cols + [time_col, flow_col] if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in dataframe: {missing_cols}")

    # Mark rows that exceed the threshold
    df["_exceeds"] = df[flow_col] > threshold

    # Calculate time step. We assume the time step is consistent within each group.
    # Group and sum the number of steps exceeding the threshold, multiplied by the time step.
    
    def calc_closure(group_df: pd.DataFrame) -> pd.Series:
        if len(group_df) < 2:
            return pd.Series({closure_col_name: 0.0})
        
        # Estimate time step from first two rows
        times = group_df[time_col].values
        dt = times[1] - times[0]
        
        # Total time is number of exceeded steps * time step
        exceeded_count = group_df["_exceeds"].sum()
        return pd.Series({closure_col_name: exceeded_count * dt})

    closure_times = df.groupby(group_cols, sort=False).apply(calc_closure).reset_index()
    
    # Cleanup temporary column
    df.drop(columns=["_exceeds"], inplace=True)
    
    return closure_times
