"""Generate AEP/Duration mean summaries for culvert result datasets."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger
from pandas.api.types import is_numeric_dtype

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv

DEFAULT_CULVERT_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA")


def run_culvert_mean_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_data_types: Sequence[str] | None = None,
    export_raw: bool = True,
) -> None:
    """Generate AEP/Duration mean statistics for culvert results and export them to Excel."""

    if script_directory is None:
        script_directory = Path.cwd()

    data_types: list[str] = list(include_data_types) if include_data_types else list(DEFAULT_CULVERT_DATA_TYPES)

    with setup_logger(console_log_level=log_level) as log_queue:
        collection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=[script_directory],
            include_data_types=data_types,
            log_queue=log_queue,
        )
    log_queue.close()
    log_queue.join_thread()

    if not collection.processors:
        logger.warning("No culvert result files were processed. Skipping export.")
        return

    aggregated_df: pd.DataFrame = collection.combine_1d_maximums()
    if aggregated_df.empty:
        logger.warning("Combined culvert maximums DataFrame is empty. Skipping export.")
        return

    aep_dur_mean: pd.DataFrame = find_culvert_aep_dur_mean(aggregated_df)
    if aep_dur_mean.empty:
        logger.warning("Unable to calculate AEP/Duration mean statistics. Skipping export.")
        return

    aep_mean_max: pd.DataFrame = find_culvert_aep_mean_max(aep_dur_mean)

    timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
    output_name: str = f"{timestamp}_culvert_mean_peaks.xlsx"
    sheet_names: list[str] = ["aep-dur-mean", "aep-mean-max"]
    sheet_frames: list[pd.DataFrame] = [aep_dur_mean, aep_mean_max]

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

    logger.info("Culvert mean report exported to {}", script_directory / output_name)


ADOPTED_SOURCE_COLUMNS: tuple[str, ...] = ("Q", "V", "DS_h", "US_h")


def find_culvert_aep_dur_mean(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Return mean statistics grouped by AEP, Duration, TP and Culvert."""

    if aggregated_df.empty:
        return pd.DataFrame()

    group_columns: list[str] = _group_columns(aggregated_df, include_duration=True)
    if not group_columns:
        logger.error("Required grouping columns were not found in the aggregated culvert DataFrame.")
        return pd.DataFrame()

    numeric_columns: list[str] = [col for col in aggregated_df.columns if is_numeric_dtype(aggregated_df[col])]
    if not numeric_columns:
        logger.warning("No numeric columns available for culvert mean calculations.")
        return pd.DataFrame()

    grouped = aggregated_df.groupby(group_columns, observed=True)

    mean_df: pd.DataFrame = grouped[numeric_columns].mean(numeric_only=True).reset_index()
    rename_map: dict[str, str] = {column: f"mean_{column}" for column in numeric_columns}
    mean_df = mean_df.rename(columns=rename_map)

    count_df: pd.DataFrame = grouped.size().rename("count").reset_index()
    count_df["count"] = count_df["count"].astype("Int64")

    range_columns: list[str] = [column for column in ADOPTED_SOURCE_COLUMNS if column in aggregated_df.columns]
    min_df: pd.DataFrame = pd.DataFrame()
    max_df: pd.DataFrame = pd.DataFrame()
    if range_columns:
        min_df = grouped[range_columns].min(numeric_only=True).reset_index()
        min_df = min_df.rename(columns={column: f"min_{column}" for column in range_columns})

        max_df = grouped[range_columns].max(numeric_only=True).reset_index()
        max_df = max_df.rename(columns={column: f"max_{column}" for column in range_columns})

    adopted_rows: list[dict[str, object]] = []
    if range_columns:
        for key, group in grouped:
            key_values: dict[str, object]
            if isinstance(key, tuple):
                key_values = dict(zip(group_columns, key, strict=False))
            else:
                key_values = {group_columns[0]: key}

            adopted_entry: dict[str, object] = {**key_values}

            q_series = pd.to_numeric(group.get("Q"), errors="coerce") if "Q" in group.columns else None
            if q_series is not None and q_series.notna().any():
                mean_q = float(q_series.mean())
                idx = (q_series - mean_q).abs().idxmin()
                closest_row = group.loc[idx]
                for column in range_columns:
                    adopted_entry[f"adopted_{column}"] = closest_row.get(column, pd.NA)
            else:
                for column in range_columns:
                    adopted_entry[f"adopted_{column}"] = pd.NA

            adopted_rows.append(adopted_entry)

    adopted_df: pd.DataFrame = pd.DataFrame(adopted_rows) if adopted_rows else pd.DataFrame()

    merged: pd.DataFrame = count_df.copy()
    for frame in (mean_df, adopted_df, min_df, max_df):
        if not frame.empty:
            merged = merged.merge(frame, on=group_columns, how="left")

    merged = merged.sort_values(group_columns, ignore_index=True)

    ordered_columns: list[str] = _ordered_columns(
        merged,
        lead=group_columns,
        secondary=["count"],
        value_prefixes=("mean_", "adopted_", "min_", "max_"),
    )
    return merged.loc[:, ordered_columns]


def find_culvert_aep_mean_max(aep_dur_mean: pd.DataFrame) -> pd.DataFrame:
    """Return the duration row containing the highest mean discharge for each AEP/culvert group."""

    if aep_dur_mean.empty:
        return pd.DataFrame()

    metric_column: str | None = _preferred_metric_column(aep_dur_mean)
    if metric_column is None:
        logger.warning("No mean columns were found for culvert mean-max calculation.")
        return pd.DataFrame()

    group_columns: list[str] = _group_columns(aep_dur_mean, include_duration=False)
    if not group_columns:
        logger.error("Required grouping columns were not found for the culvert mean-max calculation.")
        return pd.DataFrame()

    df: pd.DataFrame = aep_dur_mean.copy()
    df["_mean_metric"] = pd.to_numeric(df[metric_column], errors="coerce")
    df["_has_metric"] = df["_mean_metric"].notna()

    if not df["_has_metric"].any():
        logger.warning("Mean metric column '{}' does not contain any numeric values.", metric_column)
        return pd.DataFrame()

    df["mean_bin"] = (
        df.groupby(group_columns, observed=True)["_has_metric"].transform("sum").astype("Int64")
    )

    idx = df[df["_has_metric"]].groupby(group_columns, observed=True)["_mean_metric"].idxmax()
    result: pd.DataFrame = df.loc[idx].drop(columns=["_mean_metric", "_has_metric"]).reset_index(drop=True)
    result = result.sort_values(group_columns, ignore_index=True)

    ordered_columns: list[str] = _ordered_columns(
        result,
        lead=_group_columns(result, include_duration=True),
        secondary=["count", "mean_bin"],
        value_prefixes=("mean_", "adopted_", "min_", "max_"),
    )
    return result.loc[:, ordered_columns]


def _preferred_metric_column(aep_dur_mean: pd.DataFrame) -> str | None:
    """Return the preferred mean column used to identify maximum durations."""

    candidate_columns: list[str] = [column for column in aep_dur_mean.columns if column.startswith("mean_")]
    if not candidate_columns:
        return None

    preferred_order: tuple[str, ...] = (
        "mean_Q",
        "mean_V",
        "mean_DS_h",
        "mean_US_h",
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
    base_order.append("tp_text")
    base_order.append("trim_runcode")
    base_order.append("Chan ID")

    resolved = [column for column in base_order if column in df.columns]
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
