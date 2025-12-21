# ryan_library/scripts/pomm_utils.py
"""Utility helpers for processing POMM CSV files."""
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
from __future__ import annotations

from pathlib import Path
from multiprocessing import Queue
from collections.abc import Collection, Mapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, cast

import pandas as pd
from loguru import logger
from pandas import DataFrame, Index, Series

from ryan_library.classes.column_definitions import ColumnDefinition, ColumnMetadataRegistry
from ryan_library.functions.pandas.median_calc import median_calc
from ryan_library.functions.misc_functions import ExcelExporter, get_tools_version
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel
from ryan_library.classes.tuflow_string_classes import TuflowStringParser

NAType = type(pd.NA)
DataFrameAny = DataFrame
if TYPE_CHECKING:
    SeriesAny = Series[Any]
    QueueType = Queue[Any]
else:
    SeriesAny = Series
    QueueType = Queue


DATA_DICTIONARY_SHEET_NAME: str = "data-dictionary"


def _ordered_columns(
    df: DataFrameAny,
    column_groups: Sequence[Sequence[str]],
    info_columns: Sequence[str],
) -> list[str]:
    """Return a column order keeping identifiers first and meta-data last."""

    ordered: list[str] = []
    for group in column_groups:
        ordered.extend([column for column in group if column in df.columns])

    remaining: list[str] = [column for column in df.columns if column not in ordered and column not in info_columns]
    ordered.extend(remaining)
    ordered.extend([column for column in info_columns if column in df.columns])
    return ordered


def _select_internal_names_for_group(group: DataFrameAny) -> tuple[object, object]:
    """Return (median_internal_name, mean_internal_name) for a grouped DataFrame."""

    if "internalName" not in group.columns or "AbsMax" not in group.columns:
        return pd.NA, pd.NA

    stat_series: Series = pd.to_numeric(group["AbsMax"], errors="coerce")
    if not stat_series.notna().any():
        return pd.NA, pd.NA

    sorted_idx: Index[Any] = stat_series.sort_values(ascending=True, na_position="first").index
    median_pos = int(len(sorted_idx) / 2)
    median_internal_name = group.loc[sorted_idx[median_pos], "internalName"]

    mean_value = float(stat_series.mean())
    if pd.isna(mean_value):
        mean_internal_name = pd.NA
    else:
        closest_idx: int | str = (stat_series - mean_value).abs().idxmin()
        mean_internal_name = group.loc[closest_idx, "internalName"]

    return median_internal_name, mean_internal_name


def combine_processors_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    log_queue: QueueType | None = None,
) -> ProcessorCollection:
    """Return a :class:`ProcessorCollection` for the provided directories."""
    if include_data_types is None:
        include_data_types = ["POMM"]

    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations_to_include)

    def _run_with_queue(queue: QueueType) -> ProcessorCollection:
        logger.info(
            f"Starting POMM processing for combine_processors_from_paths. Data types: {include_data_types}; "
            f"searching in {len(paths_to_process)} folder(s)."
        )
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return ProcessorCollection()
        logger.info(f"Found {len(csv_file_list)} CSV file(s) to process.")

        if normalized_locations:
            logger.info(f"Applying location filter during processing for {len(normalized_locations)} location(s).")

        results_set_local: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=queue,
            log_level=console_log_level,
            entity_filters=normalized_locations if normalized_locations else None,
        )
        processed_count: int = len(results_set_local.processors)
        combined_rows: int = sum(len(processor.df) for processor in results_set_local.processors)
        logger.info(f"Processed {processed_count} file(s); combined {combined_rows} row(s) before filters.")
        return results_set_local

    if log_queue is None:
        with setup_logger(console_log_level=console_log_level) as queue:
            results_set: ProcessorCollection = _run_with_queue(queue)
    else:
        results_set = _run_with_queue(log_queue)

    return results_set


def combine_df_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    log_queue: QueueType | None = None,
) -> DataFrameAny:
    """Return an aggregated DataFrame for the given directories."""
    results_set: ProcessorCollection = combine_processors_from_paths(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        console_log_level=console_log_level,
        locations_to_include=locations_to_include,
        log_queue=log_queue,
    )

    if not results_set.processors:
        return DataFrame()

    combined: DataFrameAny = results_set.pomm_combine()
    logger.info(f"Combined POMM/RLL data into a single DataFrame with {len(combined)} row(s).")
    return combined


def aggregated_from_paths(
    paths: list[Path],
    locations_to_include: Collection[str] | None = None,
    include_data_types: list[str] | None = None,
    log_queue: QueueType | None = None,
) -> DataFrameAny:
    """Process directories and return a combined POMM DataFrame."""
    df: DataFrameAny = combine_df_from_paths(
        paths_to_process=paths,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        log_queue=log_queue,
    )

    if df.empty:
        return df
    logger.info(f"Aggregated dataframe ready for normalisation with {len(df)} row(s).")

    # Normalize columns for RLLQmx support
    if "Location" in df.columns and "Chan ID" in df.columns:
        df["Location"] = df["Location"].fillna(df["Chan ID"])
    elif "Location" not in df.columns and "Chan ID" in df.columns:
        df["Location"] = df["Chan ID"]

    if "AbsMax" in df.columns and "Q" in df.columns:
        df["AbsMax"] = df["AbsMax"].fillna(df["Q"])
    elif "AbsMax" not in df.columns and "Q" in df.columns:
        df["AbsMax"] = df["Q"]

    if "Type" not in df.columns:
        df["Type"] = "Q"
    else:
        df["Type"] = df["Type"].fillna("Q")

    return df


def find_aep_dur_max(aggregated_df: DataFrameAny) -> DataFrameAny:
    """Return peak rows for each AEP/Duration/Location/Type/RunCode group,
    with a column giving the size of each original group."""
    group_cols: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
    ]
    try:
        # copy so we don’t clobber the caller’s DataFrame
        df: DataFrameAny = aggregated_df.copy()
        # compute size of each group
        df["count_TP"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        count_group_cols: list[str] = [
            col for col in ("aep_text", "Location", "Type", "trim_runcode") if col in df.columns
        ]
        if count_group_cols:
            count_numeric: Series = pd.to_numeric(df["count_TP"], errors="coerce")
            df["count_TP"] = count_numeric.astype("Int64")
            df["_count_numeric"] = count_numeric.fillna(0)
            df["count_TP_aep"] = (
                df.groupby(count_group_cols, observed=True)["_count_numeric"].transform("sum").astype("Int64")
            )
            df["count_duration"] = (
                df.groupby(count_group_cols, observed=True)["duration_text"].transform("count").astype("Int64")
            )
        # find index of the max in each group
        idx = df.groupby(group_cols, observed=True)["AbsMax"].idxmax()
        # select those rows (they already carry a group_count column)
        aep_dur_max: DataFrameAny = df.loc[idx].reset_index(drop=True).drop(columns=["_count_numeric"], errors="ignore")
        logger.info(
            "Created 'aep_dur_max' DataFrame with peak records and group_count for each AEP-Duration-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_dur_max' grouping: {e}")
        aep_dur_max = DataFrame()
    return aep_dur_max


def find_aep_max(aep_dur_max: DataFrameAny) -> DataFrameAny:
    """Return peak rows for each AEP/Location/Type/RunCode group,
    with a column giving the size of each original AEP group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrameAny = aep_dur_max.copy()
        df["count_TP_aep"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        idx = df.groupby(group_cols, observed=True)["AbsMax"].idxmax()
        aep_max: DataFrameAny = df.loc[idx].reset_index(drop=True)
        logger.info(
            "Created 'aep_max' DataFrame with peak records and group_count for each AEP-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_max' grouping: {e}")
        aep_max = DataFrame()
    return aep_max


def save_to_excel(
    aep_dur_max: DataFrameAny,
    aep_max: DataFrameAny,
    aggregated_df: DataFrameAny,
    output_path: Path,
    include_pomm: bool = True,
    timestamp: str | None = None,
    aep_dur_sheet_name: str = "aep-dur-max",
    aep_sheet_name: str = "aep-max",
) -> None:
    """Save peak DataFrames to an Excel file."""
    logger.info(f"Output path: {output_path}")
    registry: ColumnMetadataRegistry = ColumnMetadataRegistry.default()
    metadata_rows: Mapping[str, str] = _build_metadata_rows(
        timestamp=timestamp,
        include_pomm=include_pomm,
        aep_dur_max=aep_dur_max,
        aep_max=aep_max,
        aggregated_df=aggregated_df,
        aep_dur_sheet_name=aep_dur_sheet_name,
        aep_sheet_name=aep_sheet_name,
    )

    sheet_frames: dict[str, DataFrameAny] = {
        aep_dur_sheet_name: aep_dur_max,
        aep_sheet_name: aep_max,
    }
    sheet_order: list[str] = [aep_dur_sheet_name, aep_sheet_name]
    sheet_dfs: list[DataFrameAny] = [aep_dur_max, aep_max]

    if include_pomm:
        sheet_frames["POMM"] = aggregated_df
        sheet_order.append("POMM")
        sheet_dfs.append(aggregated_df)

    data_dictionary_df: DataFrameAny = _build_data_dictionary(
        registry=registry,
        sheet_frames=sheet_frames,
        metadata_rows=metadata_rows,
    )

    sheet_order.append(DATA_DICTIONARY_SHEET_NAME)
    sheet_dfs.append(data_dictionary_df)

    ExcelExporter().export_dataframes(
        export_dict={
            output_path.stem: {
                "dataframes": sheet_dfs,
                "sheets": sheet_order,
            }
        },
        output_directory=output_path.parent,
        file_name=output_path.name,
    )

    logger.info(f"Peak data exported to {output_path}")


def _build_metadata_rows(
    timestamp: str | None,
    include_pomm: bool,
    aep_dur_max: DataFrameAny,
    aep_max: DataFrameAny,
    aggregated_df: DataFrameAny,
    aep_dur_sheet_name: str,
    aep_sheet_name: str,
) -> Mapping[str, str]:
    """Return ordered metadata rows for the data dictionary sheet."""

    generated_at: str = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    metadata: dict[str, str] = {
        "Generated at": generated_at,
        "Filename timestamp": timestamp if timestamp else "not supplied",
        "Generator module": __name__,
        "ryan_functions version": get_tools_version(package="ryan_functions"),
        "Include POMM sheet": "Yes" if include_pomm else "No",
        f"{aep_dur_sheet_name} rows": str(len(aep_dur_max)),
        f"{aep_sheet_name} rows": str(len(aep_max)),
    }

    if include_pomm:
        metadata["POMM rows"] = str(len(aggregated_df))

    if "directory_path" in aggregated_df.columns:
        try:
            directories_series: SeriesAny = aggregated_df["directory_path"].dropna()
        except AttributeError:
            directories_series = Series(dtype="string")
        unique_directories: list[str] = sorted({str(Path(dir_value)) for dir_value in directories_series.unique()})
        if unique_directories:
            metadata["Source directories"] = "\n".join(unique_directories)

    return metadata


def _build_data_dictionary(
    registry: ColumnMetadataRegistry,
    sheet_frames: Mapping[str, DataFrameAny],
    metadata_rows: Mapping[str, str],
) -> DataFrameAny:
    """Build the DataFrame backing the data dictionary worksheet."""

    rows: list[dict[str, str]] = []
    for key, value in metadata_rows.items():
        rows.append(
            {
                "sheet": "metadata",
                "column": key,
                "description": value,
                "value_type": "metadata",
                "pandas_dtype": "",
            }
        )

    for sheet_name, frame in sheet_frames.items():
        columns: list[str] = list(frame.columns)
        if not columns:
            rows.append(
                {
                    "sheet": sheet_name,
                    "column": "<no columns>",
                    "description": "Sheet exported without any columns. Review upstream processing.",
                    "value_type": "",
                    "pandas_dtype": "",
                }
            )
            continue

        dtype_map: dict[str, str] = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        definitions: list[ColumnDefinition] = registry.iter_definitions(columns, sheet_name=sheet_name)

        for column_name, definition in zip(columns, definitions):
            rows.append(
                {
                    "sheet": sheet_name,
                    "column": column_name,
                    "description": definition.description,
                    "value_type": definition.value_type or "",
                    "pandas_dtype": dtype_map.get(column_name, ""),
                }
            )

    return DataFrame(
        data=rows,
        columns=["sheet", "column", "description", "value_type", "pandas_dtype"],
    )


def save_peak_report(
    aggregated_df: DataFrameAny,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save peak data tables to an Excel file."""
    aep_dur_max: DataFrameAny = find_aep_dur_max(aggregated_df=aggregated_df)
    aep_max: DataFrameAny = find_aep_max(aep_dur_max=aep_dur_max)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of peak report to {output_path}")
    save_to_excel(
        aep_dur_max=aep_dur_max,
        aep_max=aep_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
    )
    logger.info(f"Completed peak report export to {output_path}")


def find_aep_dur_median(aggregated_df: DataFrameAny) -> DataFrameAny:
    """Return median stats for each AEP/Duration/Location/Type/RunCode group."""
    group_cols: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
    ]
    rows: list[dict[str, Any]] = []
    median_df: DataFrameAny = DataFrame()
    try:
        df: DataFrameAny = aggregated_df.copy()
        df["count_TP"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        for _, grp in df.groupby(group_cols, observed=True):
            stats_dict, _ = median_calc(
                thinned_df=grp,
                statcol="AbsMax",
                tpcol="tp_text",
                durcol="duration_text",
            )
            median_internal_name, mean_internal_name = _select_internal_names_for_group(grp)
            row = {
                "aep_text": grp["aep_text"].iloc[0],
                "duration_text": grp["duration_text"].iloc[0],
                "Location": grp["Location"].iloc[0],
                "Type": grp["Type"].iloc[0],
                "trim_runcode": grp["trim_runcode"].iloc[0],
            }
            if "internalName" in grp.columns:
                row["internalName"] = median_internal_name
                row["mean_internalName"] = mean_internal_name
            row.update(stats_dict)
            row["MedianAbsMax"] = row.pop("median")
            rows.append(row)
            median_df = DataFrame(rows)
        if not median_df.empty:
            # Normalise TP / duration text so the "mean storm equals median storm" flag is stable.
            def _normalize_tp(value: object) -> object:
                if pd.isna(cast(Any, value)):
                    return pd.NA
                normalized = TuflowStringParser.normalize_tp_label(value)
                return pd.NA if normalized is None else normalized

            for column in ("median_TP", "mean_TP"):
                if column in median_df.columns:
                    median_df[column] = median_df[column].apply(cast(Callable[[object], Any], _normalize_tp))

            mean_storm_matches: Series[bool] = pd.Series(False, index=median_df.index)
            required_cols: set[str] = {
                "median_duration",
                "mean_Duration",
                "median_TP",
                "mean_TP",
            }
            if required_cols.issubset(median_df.columns):
                median_duration_norm: Series[float] = median_df["median_duration"].map(
                    TuflowStringParser.normalize_duration_value
                )
                mean_duration_norm: Series[float] = median_df["mean_Duration"].map(
                    TuflowStringParser.normalize_duration_value
                )
                duration_match: Series[bool] = median_duration_norm.eq(mean_duration_norm)
                tp_match: Series[bool] = median_df["median_TP"].eq(median_df["mean_TP"])
                mean_storm_matches = (duration_match & tp_match).fillna(False)

            median_df["mean_storm_is_median_storm"] = mean_storm_matches
            count_group_cols: list[str] = [
                col for col in ("aep_text", "Location", "Type", "trim_runcode") if col in median_df.columns
            ]
            if count_group_cols and "count_TP" in median_df.columns:
                count_numeric: Series = pd.to_numeric(median_df["count_TP"], errors="coerce")
                median_df["count_TP"] = count_numeric.astype("Int64")
                median_df["_count_numeric"] = count_numeric.fillna(0)
                median_df["count_TP_aep"] = (
                    median_df.groupby(count_group_cols, observed=True)["_count_numeric"]
                    .transform("sum")
                    .astype("Int64")
                )
                median_df["count_duration"] = (
                    median_df.groupby(count_group_cols, observed=True)["duration_text"]
                    .transform("count")
                    .astype("Int64")
                )
                median_df = median_df.drop(columns=["_count_numeric"])

            id_columns: list[str] = [
                "aep_text",
                "duration_text",
                "Location",
                "Type",
                "trim_runcode",
                "internalName",
            ]
            mean_columns: list[str] = [
                "mean_including_zeroes",
                "mean_excluding_zeroes",
                "mean_PeakFlow",
                "mean_Duration",
                "mean_TP",
            ]
            median_columns: list[str] = ["MedianAbsMax", "median_duration", "median_TP"]
            info_columns: list[str] = [
                "low",
                "high",
                "count_TP",
                "count_TP_aep",
                "count_duration",
                "mean_storm_is_median_storm",
            ]

            ordered_cols: list[str] = _ordered_columns(
                df=median_df,
                column_groups=(id_columns, mean_columns, median_columns),
                info_columns=info_columns,
            )

            median_df = median_df[ordered_cols]
        logger.info("Created 'aep_dur_median' DataFrame with median records for each AEP-Duration group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_dur_median' grouping: {e}")
        median_df = DataFrame()
    return median_df


def find_aep_median_max(aep_dur_median: DataFrameAny) -> DataFrameAny:
    """Return rows representing the maximum median for each AEP/Location/Type/RunCode group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrameAny = aep_dur_median.copy()
        df["count_TP_aep"] = df.groupby(group_cols, observed=True)["MedianAbsMax"].transform("size")
        idx = df.groupby(group_cols, observed=True)["MedianAbsMax"].idxmax()
        aep_med_max: DataFrameAny = df.loc[idx].reset_index(drop=True)
        mean_value_columns: list[str] = [
            column
            for column in (
                "mean_including_zeroes",
                "mean_excluding_zeroes",
                "mean_PeakFlow",
                "mean_Duration",
                "mean_TP",
            )
            if column in aep_dur_median.columns
        ]
        if mean_value_columns:
            mean_df: DataFrameAny = aep_dur_median.copy()
            if "mean_PeakFlow" not in mean_df.columns:
                return DataFrame()
            mean_df["_mean_peakflow_numeric"] = pd.to_numeric(mean_df["mean_PeakFlow"], errors="coerce")
            if mean_df["_mean_peakflow_numeric"].notna().any():
                # When the mean columns are present we track the rows that best represent
                # the maximum mean independently from the median selection above.
                idx_mean = (
                    mean_df[mean_df["_mean_peakflow_numeric"].notna()]
                    .groupby(group_cols, observed=True)["_mean_peakflow_numeric"]
                    .idxmax()
                )
                merge_columns: list[str] = mean_value_columns.copy()
                if "mean_storm_is_median_storm" in aep_dur_median.columns:
                    merge_columns.append("mean_storm_is_median_storm")
                mean_subset: DataFrameAny = mean_df.loc[idx_mean, group_cols + merge_columns]
                aep_med_max = aep_med_max.drop(columns=merge_columns, errors="ignore")
                aep_med_max = aep_med_max.merge(mean_subset, on=group_cols, how="left")
            mean_df = mean_df.drop(columns=["_mean_peakflow_numeric"], errors="ignore")
        if not aep_med_max.empty:
            id_columns: list[str] = [
                "aep_text",
                "duration_text",
                "Location",
                "Type",
                "trim_runcode",
                "internalName",
            ]
            mean_columns: list[str] = [
                "mean_including_zeroes",
                "mean_excluding_zeroes",
                "mean_PeakFlow",
                "mean_Duration",
                "mean_TP",
            ]
            median_columns: list[str] = ["MedianAbsMax", "median_duration", "median_TP"]
            info_columns: list[str] = [
                "low",
                "high",
                "count_TP",
                "count_TP_aep",
                "count_duration",
                "mean_storm_is_median_storm",
            ]

            ordered_cols: list[str] = _ordered_columns(
                df=aep_med_max,
                column_groups=(id_columns, mean_columns, median_columns),
                info_columns=info_columns,
            )

            aep_med_max = aep_med_max[ordered_cols]
        logger.info("Created 'aep_median_max' DataFrame with maximum median records for each AEP group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_median_max' grouping: {e}")
        aep_med_max = DataFrame()
    return aep_med_max


def find_aep_dur_mean(aggregated_df: DataFrameAny) -> DataFrameAny:
    """Return mean stats for each AEP/Duration/Location/Type/RunCode group."""

    aep_dur_median: DataFrameAny = find_aep_dur_median(aggregated_df=aggregated_df)
    if aep_dur_median.empty:
        return aep_dur_median

    if "mean_internalName" in aep_dur_median.columns:
        aep_dur_median["internalName"] = aep_dur_median["mean_internalName"]
        aep_dur_median = aep_dur_median.drop(columns=["mean_internalName"], errors="ignore")

    id_columns: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
        "internalName",
    ]
    mean_columns: list[str] = [
        "mean_including_zeroes",
        "mean_excluding_zeroes",
        "mean_PeakFlow",
        "mean_Duration",
        "mean_TP",
    ]
    info_columns: list[str] = [
        "low",
        "high",
        "count_TP",
        "count_TP_aep",
        "count_duration",
        "mean_storm_is_median_storm",
    ]

    # Keep the mean-focused statistics grouped together; later callers drop any
    # residual median columns so the mean workflow can diverge independently.
    ordered_cols: list[str] = _ordered_columns(
        df=aep_dur_median,
        column_groups=(id_columns, mean_columns),
        info_columns=info_columns,
    )

    return aep_dur_median[ordered_cols]


def find_aep_mean_max(aep_dur_mean: DataFrameAny) -> DataFrameAny:
    """Return rows representing the maximum mean for each AEP/Location/Type/RunCode group."""

    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrameAny = aep_dur_mean.copy()
        if "mean_PeakFlow" not in df.columns:
            logger.error("'mean_PeakFlow' column not present for mean analysis. Returning empty DataFrame.")
            return DataFrame()

        df["_mean_peakflow_numeric"] = pd.to_numeric(df["mean_PeakFlow"], errors="coerce")
        if "count_TP" in df.columns:
            df["_count_numeric"] = pd.to_numeric(df["count_TP"], errors="coerce").fillna(0)
        else:
            df["_count_numeric"] = pd.Series(0, index=df.index, dtype="Int64")
        df["count_TP_aep"] = df.groupby(group_cols, observed=True)["_count_numeric"].transform("sum").astype("Int64")
        df["count_duration"] = (
            df.groupby(group_cols, observed=True)["duration_text"].transform("count").astype("Int64")
            if "duration_text" in df.columns
            else pd.Series(0, index=df.index, dtype="Int64")
        )

        valid_df: DataFrameAny = df[df["_mean_peakflow_numeric"].notna()]
        if valid_df.empty:
            logger.warning("No valid mean peak flow values found. Returning empty DataFrame.")
            return DataFrame()

        idx = valid_df.groupby(group_cols, observed=True)["_mean_peakflow_numeric"].idxmax()
        aep_mean_max: DataFrameAny = df.loc[idx].drop(columns=["_mean_peakflow_numeric", "_count_numeric"])
        aep_mean_max = aep_mean_max.reset_index(drop=True)

        if not aep_mean_max.empty:
            id_columns: list[str] = [
                "aep_text",
                "duration_text",
                "Location",
                "Type",
                "trim_runcode",
                "internalName",
            ]
            mean_columns: list[str] = [
                "mean_including_zeroes",
                "mean_excluding_zeroes",
                "mean_PeakFlow",
                "mean_Duration",
                "mean_TP",
            ]
            info_columns: list[str] = [
                "low",
                "high",
                "count_TP",
                "count_TP_aep",
                "count_duration",
                "mean_storm_is_median_storm",
            ]

            ordered_cols: list[str] = _ordered_columns(
                df=aep_mean_max,
                column_groups=(id_columns, mean_columns),
                info_columns=info_columns,
            )

            aep_mean_max = aep_mean_max[ordered_cols]

        logger.info("Created 'aep_mean_max' DataFrame with maximum mean records for each AEP group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_mean_max' grouping: {e}")
        aep_mean_max = DataFrame()
    return aep_mean_max


def _remove_columns_containing(df: DataFrameAny, substrings: tuple[str, ...]) -> DataFrameAny:
    """Return ``df`` without columns that include any ``substrings``."""

    filtered_df: DataFrameAny = df.copy()
    if filtered_df.empty:
        return filtered_df

    columns_to_drop: list[str] = [
        column for column in filtered_df.columns if any(substring in column.lower() for substring in substrings)
    ]
    if columns_to_drop:
        filtered_df = filtered_df.drop(columns=columns_to_drop, errors="ignore")
    return filtered_df


def _median_only_columns(df: DataFrameAny) -> DataFrameAny:
    """Return a DataFrame containing only median-focused columns."""

    return _remove_columns_containing(df=df, substrings=("mean",))


def _mean_only_columns(df: DataFrameAny) -> DataFrameAny:
    """Return a DataFrame containing only mean-focused columns."""

    return _remove_columns_containing(df=df, substrings=("median",))


def save_peak_report_median(
    aggregated_df: DataFrameAny,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_med_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save median-based peak data tables to an Excel file."""
    aep_dur_med: DataFrameAny = find_aep_dur_median(aggregated_df=aggregated_df)
    aep_med_max: DataFrameAny = find_aep_median_max(aep_dur_median=aep_dur_med)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of median peak report to {output_path}")
    aep_dur_med_filtered: DataFrameAny = _median_only_columns(df=aep_dur_med)
    aep_med_max_filtered: DataFrameAny = _median_only_columns(df=aep_med_max)
    save_to_excel(
        aep_dur_max=aep_dur_med_filtered,
        aep_max=aep_med_max_filtered,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
    )
    logger.info(f"Completed median peak report export to {output_path}")


def save_peak_report_mean(
    aggregated_df: DataFrameAny,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_mean_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save mean-based peak data tables to an Excel file."""

    aep_dur_mean: DataFrameAny = find_aep_dur_mean(aggregated_df=aggregated_df)
    aep_mean_max: DataFrameAny = find_aep_mean_max(aep_dur_mean=aep_dur_mean)
    logger.info(
        f"Preparing mean peak report. POMM rows: {len(aggregated_df)}, "
        f"AEP-duration mean rows: {len(aep_dur_mean)}, AEP mean-max rows: {len(aep_mean_max)}."
    )
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of mean peak report to {output_path}")
    aep_dur_mean_filtered: DataFrameAny = _mean_only_columns(df=aep_dur_mean)
    aep_mean_max_filtered: DataFrameAny = _mean_only_columns(df=aep_mean_max)
    save_to_excel(
        aep_dur_max=aep_dur_mean_filtered,
        aep_max=aep_mean_max_filtered,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
        aep_dur_sheet_name="aep-dur-mean",
        aep_sheet_name="aep-mean-max",
    )
    logger.info(f"Completed mean peak report export to {output_path}")
