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
    value_cols = [c for c in df.columns if c != "Time"]
    if not value_cols:
        raise ValueError("No value columns found in the DataFrame.")

    # Melt everything except Time
    df_long = df.melt(id_vars=["Time"], value_vars=value_cols, var_name="raw_col", value_name="H_val")

    # Extract suffix (.1 or .2) and base channel name
    # We assume the suffix is the last 2 characters: ".1" or ".2"
    # If a column doesn't end in .1 or .2, we treat it as ambiguous or ignore it?
    # Real files seem to consistently use .1/.2 for H data.
    # Let's use a regex or string slicing.

    # Helper to classify
    def parse_col(s: str) -> tuple[str, str]:
        if s.endswith(".1"):
            return s[:-2], "US_H"
        elif s.endswith(".2"):
            return s[:-2], "DS_H"
        else:
            # Fallback or specific handling.
            # Some older files might use different conventions?
            # For now, assume everything else is US if it doesn't match?
            # Or log a warning?
            # Let's assume it's a single point and map to US_H for now, or skip?
            # Given the strict requirement for US/DS in H files, maybe skip?
            return s, "Unknown"

    # Apply parsing
    # This is vectorized-ish
    df_long["suffix_type"] = df_long["raw_col"].apply(
        lambda x: ".1" if x.endswith(".1") else (".2" if x.endswith(".2") else "unknown")
    )
    df_long[category_type] = df_long["raw_col"].apply(lambda x: x[:-2] if x.endswith((".1", ".2")) else x)

    # Filter out unknowns if necessary, or map them.
    # Mapping: .1 -> US_H, .2 -> DS_H
    type_map = {".1": "US_H", ".2": "DS_H"}
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

    reshaped = df_long.pivot_table(
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
    expected_order = ["Time", category_type, "US_H", "DS_H"]
    reshaped = reshaped[expected_order]

    logger.debug(f"{file_label}: Reshaped 'H' DataFrame to long format with {len(reshaped)} rows.")
    return reshaped
