# ryan_library/functions/tuflow/closure_durations_functions.py

# ryan_library\functions\tuflow\closure_durations_functions.py
"""Shared closure-duration helpers used by the TUFLOW orchestrator.

These helpers operate on processed PO DataFrames produced by the TUFLOW
processors. They are intentionally logic-only (no I/O) so they can be reused and
unit-tested independently of the orchestration layer.
"""

__lazy_modules__ = ["pandas"]

from math import isfinite
from typing import cast

import pandas as pd
from loguru import logger
from pandas import DataFrame

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
    """Return the timestep using the first two finite, distinct time values."""
    numeric = pd.to_numeric(time_values, errors="coerce")
    finite_times = numeric[numeric.notna() & numeric.abs().ne(float("inf"))].unique()
    if len(finite_times) < 2:
        return None
    sorted_times: pd.Series = pd.Series(finite_times).sort_values(ignore_index=True)
    timestep = float(sorted_times.iloc[1] - sorted_times.iloc[0])
    return timestep if isfinite(timestep) and timestep > 0 else None


def collect_po_data(collection: ProcessorCollection) -> DataFrame:
    """Concatenate PO processor DataFrames into a single DataFrame."""
    frames: list[DataFrame] = [processor.df for processor in collection.processors if processor.data_type == "PO"]
    return pd.concat(frames, ignore_index=True) if frames else DataFrame()


def calculate_threshold_durations(
    po_df: DataFrame,
    thresholds: list[float],
    measurement_type: str,
) -> DataFrame:
    """Return one closure observation for every observed event and threshold.

    Zero exceedances are a valid closure of zero hours. Invalid samples or an
    unusable timestep make the event unusable, not a dry event: its duration is
    NaN and it remains visible in the raw population and summary counts.
    """
    if po_df.empty:
        return DataFrame()

    filtered: DataFrame = po_df.copy()
    filtered["Type"] = filtered["Type"].astype(str).str.strip()
    filtered = filtered[filtered["Type"].str.lower() == measurement_type.lower()]
    if filtered.empty:
        logger.warning("No PO rows found for measurement type '{}'. Skipping.", measurement_type)
        return DataFrame()

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

    filtered["Time"] = pd.to_numeric(filtered["Time"], errors="coerce")
    filtered["Value"] = pd.to_numeric(filtered["Value"], errors="coerce")
    filtered = filtered.dropna(subset=["Location"])

    records: list[dict[str, object]] = []
    for _, group in filtered.groupby(available_keys, dropna=False, observed=False):
        timestep = timestep_from_series(time_values=group["Time"])
        # -99999 is TUFLOW's documented no-data marker; negative flows and
        # ordinary zeroes remain legitimate hydraulic observations.
        valid_time = group["Time"].notna() & group["Time"].abs().ne(float("inf")) & group["Time"].ne(-99999.0)
        valid_value = group["Value"].notna() & group["Value"].abs().ne(float("inf")) & group["Value"].ne(-99999.0)
        valid_event = timestep is not None and bool((valid_time & valid_value).all())
        if not valid_event:
            logger.warning(
                "Unusable PO event at location '{}': missing/nonfinite sample, no-data marker or timestep; "
                "closure will not be reported as zero.",
                first_value(group["Location"]),
            )

        out_path: str = first_value(series=group["directory_path"]) if "directory_path" in group.columns else ""
        trim_runcode: str = first_value(series=group["trim_runcode"]) if "trim_runcode" in group.columns else ""
        aep: str = first_value(series=group["aep_text"]) if "aep_text" in group.columns else ""
        duration: str = first_value(series=group["duration_text"]) if "duration_text" in group.columns else ""
        tp: str = first_value(series=group["tp_text"]) if "tp_text" in group.columns else ""
        location: str = first_value(series=group["Location"])

        for threshold in thresholds:
            closure_hours = (
                float((group["Value"] > threshold).sum()) * timestep
                if valid_event and timestep is not None
                else float("nan")
            )
            records.append(
                {
                    "AEP": aep,
                    "Duration": duration,
                    "TP": tp,
                    "Location": location,
                    "ThresholdFlow": float(threshold),
                    "Duration_Exceeding": closure_hours,
                    "out_path": out_path,
                    "trim_runcode": trim_runcode,
                    "Event_Status": "OK" if valid_event else "UNUSABLE",
                }
            )

    return DataFrame(data=records)


def summarise_results(df: DataFrame) -> DataFrame:
    """Summarise each observed scenario without manufacturing missing closures."""
    from ..pandas.median_calc import median_stats as median_stats_func

    final_columns: list[str] = [
        "Path",
        "trim_runcode",
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
        "count_TP_aep",
        "count_unusable_TP_aep",
        "count_duration",
        "count_duration_unusable",
        "tp_population_status",
    ]
    if df.empty:
        return DataFrame(columns=final_columns)

    observations = df.copy()
    if "trim_runcode" not in observations.columns:
        observations["trim_runcode"] = ""

    grouped = observations.groupby(["out_path", "trim_runcode", "Location", "ThresholdFlow", "AEP"], dropna=False)
    records: list[dict[str, object]] = []
    for name, group in grouped:
        path, run_code, location, threshold, aep = cast("tuple[str, str, str, float, str]", name)
        # Existing TP/source tie-breaking in median_stats is deterministic
        # when tp_text is present; raw closure observations use the TP column.
        group_for_stats = group.assign(tp_text=group["TP"].astype("string"))
        stats, _ = median_stats_func(group_for_stats, "Duration_Exceeding", "TP", "Duration")
        count_valid = stats.get("count_TP_aep", 0)
        count_unusable = stats.get("count_unusable_TP_aep", 0)
        population_status = "UNUSABLE" if count_valid == 0 else ("PARTIAL" if count_unusable else "OK")
        row: list[object] = [path, run_code, location, threshold, aep]
        row.extend(
            [
                stats.get("median"),
                stats.get("median_duration"),
                stats.get("median_TP"),
                stats.get("low"),
                stats.get("high"),
                stats.get("mean_including_zeroes"),
                stats.get("median_TP"),
                stats.get("median"),
                count_valid,
                count_unusable,
                stats.get("count_duration", 0),
                stats.get("count_duration_unusable", 0),
                population_status,
            ]
        )
        records.append(dict(zip(final_columns, row, strict=True)))
    return DataFrame.from_records(data=records, columns=final_columns)
