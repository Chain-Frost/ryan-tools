"""Utility helpers shared by TUFLOW timeseries processors."""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]
from loguru import logger


def reshape_h_timeseries(df: pd.DataFrame, category_type: str, file_label: str) -> pd.DataFrame:
    """Convert H timeseries data with upstream/downstream suffixes into a long format DataFrame."""
    if "Time" not in df.columns:
        raise ValueError("DataFrame must contain a 'Time' column before reshaping H data.")

    suffixes: tuple[str, str] = ("_US", "_DS")
    channels: list[str] = sorted({col.rsplit("_", 1)[0] for col in df.columns if col.endswith(suffixes)})
    if not channels:
        raise ValueError("No channel columns with '_US' or '_DS' suffixes were found in the DataFrame.")

    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        time_value = row["Time"]
        for channel_id in channels:
            record: dict[str, object] = {
                "Time": time_value,
                category_type: channel_id,
                "H_US": row.get(f"{channel_id}_US", -9999.0),
                "H_DS": row.get(f"{channel_id}_DS", -9999.0),
            }
            records.append(record)

    reshaped_df = pd.DataFrame(records)
    logger.debug(f"{file_label}: Reshaped 'H' DataFrame to long format with {len(reshaped_df)} rows.")
    return reshaped_df
