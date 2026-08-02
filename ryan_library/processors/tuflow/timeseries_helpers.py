# ryan_library\processors\tuflow\timeseries_helpers.py
"""Utility helpers shared by TUFLOW timeseries processors."""

import pandas as pd
from loguru import logger


def reshape_h_timeseries(df: pd.DataFrame, category_type: str, file_label: str) -> pd.DataFrame:
    """Convert H timeseries data with upstream/downstream suffixes into a long format DataFrame.

    TUFLOW 1D H files typically use suffixes ".1" for upstream and ".2" for downstream.
    Example: "ds1.1" (Upstream) and "ds1.2" (Downstream).
    """
    if "Time" not in df.columns:
        raise ValueError("DataFrame must contain a 'Time' column before reshaping H data.")

    # Identify value columns (exclude Time)
    value_cols: list[str] = [c for c in df.columns if c != "Time"]
    if not value_cols:
        raise ValueError("No value columns found in the DataFrame.")

    # Melt everything except Time
    df_long: pd.DataFrame = df.melt(id_vars=["Time"], value_vars=value_cols, var_name="raw_col", value_name="H_val")

    # Normalise labels once so suffixed and unsuffixed identifiers have one
    # consistent string representation. Pandas' vectorised string operations
    # avoid the unknown lambda argument types exposed by pandas-stubs.
    raw_columns: pd.Series = df_long["raw_col"].astype("string")
    upstream_mask: pd.Series = raw_columns.str.endswith(".1", na=False)
    downstream_mask: pd.Series = raw_columns.str.endswith(".2", na=False)

    df_long["suffix_type"] = "unknown"
    df_long.loc[upstream_mask, "suffix_type"] = ".1"
    df_long.loc[downstream_mask, "suffix_type"] = ".2"
    df_long[category_type] = raw_columns.str.replace(r"\.[12]$", "", regex=True)

    # Filter out unknowns if necessary, or map them.
    # Mapping: .1 -> US_H, .2 -> DS_H
    type_map: dict[str, str] = {".1": "US_H", ".2": "DS_H"}
    df_long["col_type"] = df_long["suffix_type"].map(type_map)

    # Drop rows where col_type is NaN (unknown suffix)
    # df_long = df_long.dropna(subset=["col_type"])
    # Actually, let's keep them but maybe warn?
    # For robustness, let's just filter for now as we expect .1/.2
    df_long = df_long[df_long["col_type"].notna()]

    if df_long.empty:
        raise ValueError("No columns with valid '.1' (US) or '.2' (DS) suffixes found.")

    # Pivot to get US_H and DS_H as columns
    # index: Time, category_type
    # columns: col_type
    # values: H_val

    reshaped: pd.DataFrame = df_long.pivot_table(
        index=["Time", category_type],
        columns="col_type",
        values="H_val",
        aggfunc="first",  # Should be unique per time/channel
    ).reset_index()

    # Ensure both columns exist
    for col in ["US_H", "DS_H"]:
        if col not in reshaped.columns:
            reshaped[col] = pd.NA

    # Enforce column order
    expected_order: list[str] = ["Time", category_type, "US_H", "DS_H"]
    reshaped = reshaped[expected_order]

    logger.debug("{}: Reshaped 'H' DataFrame to long format with {} rows.", file_label, len(reshaped))
    return reshaped
