# ryan_library\functions\tuflow\closure_durations_functions.py
"""Shared closure-duration helpers used by the TUFLOW orchestrator.

These helpers operate on processed PO DataFrames produced by the TUFLOW
processors. They are intentionally logic-only (no I/O) so they can be reused and
unit-tested independently of the orchestration layer.
"""

import pandas as pd
from pandas import DataFrame
from loguru import logger

from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def first_value(series: pd.Series) -> str:
    """Return the first non-empty string value from ``series``."""
    for value in series:
        if pd.notna(value):
            text: str = str(value).strip()
            if text:
                return text
    return ""


def timestep_from_series(time_values: pd.Series) -> float | None:
    """Return the timestep using the first two numeric time values."""
    numeric = pd.to_numeric(time_values, errors="coerce").dropna().unique()
    if len(numeric) < 2:
        return None
    sorted_times: pd.Series = pd.Series(numeric).sort_values(ignore_index=True)
    return float(sorted_times.iloc[1] - sorted_times.iloc[0])


def collect_po_data(collection: ProcessorCollection) -> DataFrame:
    """Concatenate PO processor DataFrames into a single DataFrame."""
    frames: list[DataFrame] = [processor.df for processor in collection.processors if processor.data_type == "PO"]
    return pd.concat(frames, ignore_index=True) if frames else DataFrame()


def calculate_threshold_durations(
    po_df: DataFrame,
    thresholds: list[float],
    measurement_type: str,
) -> DataFrame:
    """Return a DataFrame of duration exceedances for the requested measurement type."""

    if po_df.empty:
        return DataFrame()

    filtered: DataFrame = po_df.copy()
    filtered["Type"] = filtered["Type"].astype(str).str.strip()
    filtered = filtered[filtered["Type"].str.lower() == measurement_type.lower()]
    if filtered.empty:
        logger.warning(f"No PO rows found for measurement type '{measurement_type}'. Skipping.")
        return DataFrame()

    filtered["Time"] = pd.to_numeric(filtered["Time"], errors="coerce")
    filtered["Value"] = pd.to_numeric(filtered["Value"], errors="coerce")
    filtered = filtered.dropna(subset=["Time", "Value", "Location"])

    group_keys: list[str] = [
        "directory_path",
        "trim_runcode",
        "Location",
        "aep_text",
        "duration_text",
        "tp_text",
    ]
    available_keys: list[str] = [key for key in group_keys if key in filtered.columns]
    if "Location" not in available_keys:
        logger.warning("PO data is missing a 'Location' column. Skipping duration calculation.")
        return DataFrame()

    records: list[dict[str, object]] = []
    for _, group in filtered.groupby(available_keys, dropna=False):
        timestep: float | None = timestep_from_series(time_values=group["Time"])
        if timestep is None:
            location_label: str = first_value(group["Location"])
            logger.warning(f"Unable to determine timestep for group with Location '{location_label}'. Skipping.")
            continue

        out_path: str = first_value(series=group["directory_path"]) if "directory_path" in group.columns else ""
        aep: str = first_value(series=group["aep_text"]) if "aep_text" in group.columns else ""
        duration: str = first_value(series=group["duration_text"]) if "duration_text" in group.columns else ""
        tp: str = first_value(series=group["tp_text"]) if "tp_text" in group.columns else ""
        location: str = first_value(series=group["Location"])

        for threshold in thresholds:
            exceed_count: int = int((group["Value"] > threshold).sum())
            if exceed_count == 0:
                continue
            records.append(
                {
                    "AEP": aep,
                    "Duration": duration,
                    "TP": tp,
                    "Location": location,
                    "ThresholdFlow": float(threshold),
                    "Duration_Exceeding": float(exceed_count) * timestep,
                    "out_path": out_path,
                }
            )

    return DataFrame(data=records)


def summarise_results(df: DataFrame) -> DataFrame:
    """Summarise closure duration results."""
    from ..pandas.median_calc import median_stats as median_stats_func

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
    # Capture the full set of (Duration, TP) combinations for each location/AEP so that
    # thresholds which do not exceed for a given combination can still contribute zeros
    # to the summary statistics.  Without this we would discard zeros entirely and the
    # medians could erroneously increase as the threshold increased.
    combo_lookup = {
        key: grp.loc[:, ["Duration", "TP"]].drop_duplicates().reset_index(drop=True)
        for key, grp in df.loc[:, ["out_path", "Location", "AEP", "Duration", "TP"]]
        .drop_duplicates()
        .groupby(["out_path", "Location", "AEP"])
    }

    grouped = df.groupby(["out_path", "Location", "ThresholdFlow", "AEP"])
    for name, group in grouped:
        path, location, threshold, aep = name
        combos: DataFrame | None = combo_lookup.get((path, location, aep))
        if combos is not None:
            group: DataFrame = combos.merge(right=group, on=["Duration", "TP"], how="left")
            group["AEP"] = group["AEP"].fillna(aep)
            group["out_path"] = group["out_path"].fillna(path)
            group["Location"] = group["Location"].fillna(location)
            group["ThresholdFlow"] = group["ThresholdFlow"].fillna(threshold)
            group["Duration_Exceeding"] = group["Duration_Exceeding"].fillna(0.0)

        stats, _ = median_stats_func(group, "Duration_Exceeding", "TP", "Duration")
        row = list(name) + [
            stats.get("median"),
            stats.get("median_duration"),
            stats.get("median_TP"),
            stats.get("low"),
            stats.get("high"),
            stats.get("mean_including_zeroes"),
            stats.get("median_TP"),
            stats.get("median"),
        ]
        finaldb.loc[len(finaldb)] = row
    finaldb.columns = final_columns
    return finaldb
