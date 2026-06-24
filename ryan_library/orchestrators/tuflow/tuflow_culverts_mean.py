"""
Generate AEP/Duration Mean and Median Summaries for Culvert Results.

This module processes culvert result data to calculate mean or upper-middle median statistics across differing durations
for each AEP event. It identifies the "critical" duration based on the highest flow metric and can "adopt"
corresponding values for other columns from the simulation that matches the target flow statistic most closely.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger
from pandas.api.types import is_numeric_dtype

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.pandas.median_calc import upper_middle_row, upper_middle_value
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

DEFAULT_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)
StatisticName = Literal["mean", "median"]


def run_culvert_mean_report(
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_data_types: Sequence[str] | None = None,
    locations_to_include: Collection[str] | None = None,
    export_raw: bool = True,
) -> None:
    """
    Generate AEP/Duration mean statistics for culvert results and export them to Excel.

    Args:
        script_directory: Output directory for exported files.
        paths_to_process: Folder roots to search for culvert result files.
        log_level: Console logging verbosity.
        include_data_types: List of file types to include (e.g. "Nmx", "Chan").
        locations_to_include: Filter for specific culvert IDs/locations.
        export_raw: If True, includes the raw combined maximums in a separate Excel sheet.
    """

    _run_culvert_statistic_report(
        statistic="mean",
        output_suffix="mean",
        script_directory=script_directory,
        paths_to_process=paths_to_process,
        log_level=log_level,
        include_data_types=include_data_types,
        locations_to_include=locations_to_include,
        export_raw=export_raw,
    )


def run_culvert_median_report(
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_data_types: Sequence[str] | None = None,
    locations_to_include: Collection[str] | None = None,
    export_raw: bool = True,
) -> None:
    """
    Generate AEP/Duration median statistics for culvert results and export them to Excel.

    Args:
        script_directory: Output directory for exported files.
        paths_to_process: Folder roots to search for culvert result files.
        log_level: Console logging verbosity.
        include_data_types: List of file types to include (e.g. "Nmx", "Chan").
        locations_to_include: Filter for specific culvert IDs/locations.
        export_raw: If True, includes the raw combined maximums in a separate Excel sheet.
    """

    _run_culvert_statistic_report(
        statistic="median",
        output_suffix="med",
        script_directory=script_directory,
        paths_to_process=paths_to_process,
        log_level=log_level,
        include_data_types=include_data_types,
        locations_to_include=locations_to_include,
        export_raw=export_raw,
    )


def _run_culvert_statistic_report(
    *,
    statistic: StatisticName,
    output_suffix: str,
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_data_types: Sequence[str] | None = None,
    locations_to_include: Collection[str] | None = None,
    export_raw: bool = True,
) -> None:
    """Generate AEP/Duration culvert statistics and export them to Excel."""

    if script_directory is None:
        script_directory = Path.cwd()
    effective_paths_to_process: list[Path] = list(paths_to_process or (script_directory,))
    context: str = f"Culvert {statistic} report"

    data_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )

    with setup_logger(console_log_level=log_level) as log_queue:
        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context=context,
        )

        collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=effective_paths_to_process,
            include_data_types=data_types,
            log_queue=log_queue,
            console_log_level=log_level,
        )

        if locations_to_include:
            normalized_locations: frozenset[str] = collection.filter_locations(locations=locations_to_include)
            if not normalized_locations:
                logger.warning("Location filter provided but no valid values found. Continuing without filtering.")

        if not collection.processors:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context=f"{context} completed",
            )
            logger.warning("No culvert result files were processed. Skipping export.")
            return

        aggregated_df: pd.DataFrame = collection.combine_1d_maximums()
        if aggregated_df.empty:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context=f"{context} completed",
            )
            logger.warning("Combined culvert maximums DataFrame is empty. Skipping export.")
            return

        # Calculate statistics grouped by AEP and Duration.
        aep_dur_stat: pd.DataFrame = _find_culvert_aep_dur_statistic(
            aggregated_df=aggregated_df,
            statistic=statistic,
        )
        if aep_dur_stat.empty:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context=f"{context} completed",
            )
            logger.warning(f"Unable to calculate AEP/Duration {statistic} statistics. Skipping export.")
            return

        # Calculate the duration with the highest statistic for each AEP.
        aep_stat_max: pd.DataFrame = _find_culvert_aep_statistic_max(
            aep_dur_stat=aep_dur_stat,
            statistic=statistic,
        )

        timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
        output_name: str = f"{timestamp}_culvert_{output_suffix}_peaks.xlsx"
        sheet_names: list[str] = [f"aep-dur-{output_suffix}", f"aep-{output_suffix}-max"]
        sheet_frames: list[pd.DataFrame] = [aep_dur_stat, aep_stat_max]

        if export_raw:
            sheet_names.append("culvert-maximums")
            sheet_frames.append(aggregated_df)

        ExcelExporter().export_dataframes(
            export_dict={
                Path(output_name).stem: {
                    "dataframes": sheet_frames,
                    "sheets": sheet_names,
                }
            },
            output_directory=script_directory,
            file_name=output_name,
        )

        logger.info(f"Culvert {statistic} report exported to {script_directory / output_name}")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context=f"{context} completed",
        )


ADOPTED_SOURCE_COLUMNS: tuple[str, ...] = ("Q", "V", "DS_h", "US_h")
ADOPTED_CONTEXT_COLUMNS: tuple[str, ...] = ("tp_text", "internalName")
EXCLUDED_MEAN_NUMERIC_COLUMNS: frozenset[str] = frozenset({"tp_numeric"})


def find_culvert_aep_dur_mean(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return mean statistics grouped by AEP, Duration, run variant and Culvert.

    Temporal patterns are intentionally collapsed here so each duration row represents
    the mean response across contributing TPs. Also determines "adopted" values: for
    each group, it finds the simulation run closest to the mean Flow (Q) and adopts
    its values for select columns (Q, V, DS_h, US_h).
    """

    return _find_culvert_aep_dur_statistic(aggregated_df=aggregated_df, statistic="mean")


def find_culvert_aep_dur_median(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return upper-middle median statistics grouped by AEP, Duration, run variant and Culvert.

    Temporal patterns are intentionally collapsed here so each duration row represents
    the upper-middle median response across contributing TPs. Also determines "adopted"
    values: for each group, it uses the simulation run at the upper-middle median Flow
    (Q) and adopts its values for select columns (Q, V, DS_h, US_h).
    """

    return _find_culvert_aep_dur_statistic(aggregated_df=aggregated_df, statistic="median")


def _find_culvert_aep_dur_statistic(aggregated_df: pd.DataFrame, statistic: StatisticName) -> pd.DataFrame:
    """Return grouped culvert statistics for each AEP/Duration/run/Culvert group."""

    if aggregated_df.empty:
        return pd.DataFrame()

    group_columns: list[str] = _group_columns(df=aggregated_df, include_duration=True)
    if not group_columns:
        logger.error("Required grouping columns were not found in the aggregated culvert DataFrame.")
        return pd.DataFrame()

    numeric_columns: list[str] = [
        col
        for col in aggregated_df.columns
        if col not in EXCLUDED_MEAN_NUMERIC_COLUMNS and is_numeric_dtype(arr_or_dtype=aggregated_df[col])
    ]
    if not numeric_columns:
        logger.warning(f"No numeric columns available for culvert {statistic} calculations.")
        return pd.DataFrame()

    grouped = aggregated_df.groupby(group_columns, observed=True)

    # Calculate statistic values for all numeric columns.
    if statistic == "mean":
        statistic_df: pd.DataFrame = grouped[numeric_columns].mean(numeric_only=True).reset_index()
    else:
        statistic_rows: list[dict[str, object]] = []
        for key, group in grouped:
            statistic_entry: dict[str, object] = _group_key_values(group_columns=group_columns, key=key)
            for column in numeric_columns:
                statistic_entry[column] = upper_middle_value(group=group, value_column=column)
            statistic_rows.append(statistic_entry)
        statistic_df = pd.DataFrame(data=statistic_rows)
    rename_map: dict[str, str] = {column: f"{statistic}_{column}" for column in numeric_columns}
    statistic_df = statistic_df.rename(columns=rename_map)

    count_df: pd.DataFrame = grouped.size().rename("count").reset_index()
    count_df["count"] = count_df["count"].astype("Int64")

    # Determine min/max values for key columns
    range_columns: list[str] = [column for column in ADOPTED_SOURCE_COLUMNS if column in aggregated_df.columns]
    min_df: pd.DataFrame = pd.DataFrame()
    max_df: pd.DataFrame = pd.DataFrame()
    if range_columns:
        min_df = grouped[range_columns].min(numeric_only=True).reset_index()
        min_df = min_df.rename(columns={column: f"min_{column}" for column in range_columns})

        max_df = grouped[range_columns].max(numeric_only=True).reset_index()
        max_df = max_df.rename(columns={column: f"max_{column}" for column in range_columns})

    # "Adopt" values from the run closest to the target Q statistic.
    adopted_rows: list[dict[str, object]] = []
    if range_columns:
        for key, group in grouped:
            adopted_entry: dict[str, object] = _group_key_values(group_columns=group_columns, key=key)

            q_series = pd.to_numeric(group.get("Q"), errors="coerce") if "Q" in group.columns else None
            if q_series is not None and q_series.notna().any():
                if statistic == "mean":
                    target_q = float(q_series.mean())
                    # Find index of value closest to the statistic.
                    idx = (q_series - target_q).abs().idxmin()
                    closest_row = group.loc[idx]
                else:
                    closest_row = upper_middle_row(group=group, value_column="Q")
                for column in range_columns:
                    adopted_entry[f"adopted_{column}"] = closest_row.get(column, pd.NA)
                for column in ADOPTED_CONTEXT_COLUMNS:
                    if column in group.columns:
                        adopted_entry[f"adopted_{column}"] = closest_row.get(column, pd.NA)
            else:
                for column in range_columns:
                    adopted_entry[f"adopted_{column}"] = pd.NA
                for column in ADOPTED_CONTEXT_COLUMNS:
                    if column in group.columns:
                        adopted_entry[f"adopted_{column}"] = pd.NA

            adopted_rows.append(adopted_entry)

    adopted_df: pd.DataFrame = pd.DataFrame(data=adopted_rows) if adopted_rows else pd.DataFrame()

    # Merge everything together
    merged: pd.DataFrame = count_df.copy()
    for frame in (statistic_df, adopted_df, min_df, max_df):
        if not frame.empty:
            merged = merged.merge(right=frame, on=group_columns, how="left")

    merged = merged.sort_values(by=group_columns, ignore_index=True)

    ordered_columns: list[str] = _ordered_columns(
        df=merged,
        lead=group_columns,
        secondary=["count"],
        value_prefixes=(f"{statistic}_", "adopted_", "min_", "max_"),
    )
    return merged.loc[:, ordered_columns]


def find_culvert_aep_mean_max(aep_dur_mean: pd.DataFrame) -> pd.DataFrame:
    """
    Return the duration row containing the highest mean discharge (or specified metric) for each AEP/culvert group.

    This finds the "Critical Duration" based on the mean-across-TP results calculated previously.
    """

    return _find_culvert_aep_statistic_max(aep_dur_stat=aep_dur_mean, statistic="mean")


def find_culvert_aep_median_max(aep_dur_median: pd.DataFrame) -> pd.DataFrame:
    """
    Return the duration row containing the highest median discharge for each AEP/culvert group.

    This finds the "Critical Duration" based on the median-across-TP results calculated previously.
    """

    return _find_culvert_aep_statistic_max(aep_dur_stat=aep_dur_median, statistic="median")


def _find_culvert_aep_statistic_max(aep_dur_stat: pd.DataFrame, statistic: StatisticName) -> pd.DataFrame:
    """Return the duration row containing the highest statistic for each AEP/culvert group."""

    if aep_dur_stat.empty:
        return pd.DataFrame()

    metric_column: str | None = _preferred_metric_column(aep_dur_stat=aep_dur_stat, statistic=statistic)
    if metric_column is None:
        logger.warning(f"No {statistic} columns were found for culvert {statistic}-max calculation.")
        return pd.DataFrame()

    # Group by AEP/Culvert (exclude duration) to find the max across durations.
    group_columns: list[str] = _group_columns(df=aep_dur_stat, include_duration=False)
    if not group_columns:
        logger.error(f"Required grouping columns were not found for the culvert {statistic}-max calculation.")
        return pd.DataFrame()

    df: pd.DataFrame = aep_dur_stat.copy()
    metric_work_column: str = f"_{statistic}_metric"
    has_metric_column: str = f"_has_{statistic}_metric"
    bin_column: str = f"{statistic}_bin"
    df[metric_work_column] = pd.to_numeric(df[metric_column], errors="coerce")
    df[has_metric_column] = df[metric_work_column].notna()

    if not df[has_metric_column].any():
        logger.warning(f"{statistic.title()} metric column '{metric_column}' does not contain any numeric values.")
        return pd.DataFrame()

    # Count how many valid entries exist for this group.
    df[bin_column] = df.groupby(group_columns, observed=True)[has_metric_column].transform("sum").astype("Int64")

    # Find the index of the max value within each group.
    idx = df[df[has_metric_column]].groupby(by=group_columns, observed=True)[metric_work_column].idxmax()
    result: pd.DataFrame = df.loc[idx].drop(columns=[metric_work_column, has_metric_column]).reset_index(drop=True)
    result = result.sort_values(group_columns, ignore_index=True)

    ordered_columns: list[str] = _ordered_columns(
        df=result,
        lead=_group_columns(df=result, include_duration=True),
        secondary=["count", bin_column],
        value_prefixes=(f"{statistic}_", "adopted_", "min_", "max_"),
    )
    return result.loc[:, ordered_columns]


def _group_key_values(group_columns: list[str], key: object) -> dict[str, object]:
    """Return grouping column values for a pandas groupby key."""

    if isinstance(key, tuple):
        return dict(zip(group_columns, key, strict=False))
    return {group_columns[0]: key}


def _preferred_metric_column(aep_dur_stat: pd.DataFrame, statistic: StatisticName) -> str | None:
    """Return the preferred statistic column used to identify maximum durations (e.g. mean_Q)."""

    prefix: str = f"{statistic}_"
    candidate_columns: list[str] = [column for column in aep_dur_stat.columns if column.startswith(prefix)]
    if not candidate_columns:
        return None

    preferred_order: tuple[str, ...] = (
        f"{prefix}Q",
        f"{prefix}V",
        f"{prefix}DS_h",
        f"{prefix}US_h",
    )
    for preferred in preferred_order:
        if preferred in candidate_columns:
            return preferred
    return candidate_columns[0]


def _group_columns(df: pd.DataFrame, include_duration: bool) -> list[str]:
    """Return the ordered list of grouping columns present in ``df``."""

    base_order: list[str] = ["aep_text"]
    if include_duration:
        base_order.append("duration_text")
    base_order.append("trim_runcode")
    base_order.append("Chan ID")

    resolved: list[str] = [column for column in base_order if column in df.columns]
    if "Chan ID" not in resolved:
        return []
    return resolved


def _ordered_columns(
    df: pd.DataFrame,
    lead: list[str],
    secondary: list[str],
    value_prefixes: Sequence[str],
) -> list[str]:
    """Return an ordered list of columns for presentation."""

    ordered: list[str] = []
    for column in lead + secondary:
        if column in df.columns and column not in ordered:
            ordered.append(column)

    for prefix in value_prefixes:
        prefixed_columns: list[str] = sorted([column for column in df.columns if column.startswith(prefix)])
        ordered.extend([column for column in prefixed_columns if column not in ordered])

    for column in df.columns:
        if column not in ordered:
            ordered.append(column)

    return ordered
