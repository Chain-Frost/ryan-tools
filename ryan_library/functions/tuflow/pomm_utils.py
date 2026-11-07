# ryan_library/functions/tuflow/pomm_utils.py
"""Utility helpers for processing POMM CSV files."""

# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
from loguru import logger
from pandas import DataFrame, Index, Series

from ryan_library.classes.column_definitions import ColumnMetadataRegistry
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.excel_export import (
    DATA_DICTIONARY_SHEET_NAME,
    ExcelExporter,
    build_data_dictionary,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.pandas.median_calc import finite_sorted_rows, median_calc
from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel
from ryan_library.functions.versioning import get_tools_version
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

if TYPE_CHECKING:
    from ryan_library.functions.loguru_helpers import LogQueue


# ARR storm-duration sequence in minutes; see the TUFLOW scripts README for provenance.
STANDARD_DURATIONS_MINUTES: tuple[int, ...] = (
    10,
    15,
    20,
    25,
    30,
    45,
    60,
    90,
    120,
    180,
    270,
    360,
    540,
    720,
    1080,
    1440,
    1800,
    2160,
    2880,
    4320,
    5760,
    7200,
    8640,
    10080,
)


def _duration_minutes(value: object) -> float:
    """Normalize a duration-column value (including short minute labels)."""
    text = str(value).strip().lower()
    try:
        minutes = float(text.removesuffix("m"))
    except ValueError:
        minutes = TuflowStringParser.normalize_duration_value(value)
    return minutes if isfinite(minutes) and minutes > 0 else float("nan")


def _mean_coverage_findings(
    duration_rows: DataFrame,
    peak_rows: DataFrame,
    standard_durations: Sequence[int],
    expected_tp_count: int,
) -> DataFrame:
    """Attach coverage findings using every duration in the same report group."""
    if type(expected_tp_count) is not int or expected_tp_count < 1:
        msg = "expected_tp_count must be a positive integer, for example 10."
        raise ValueError(msg)
    if not standard_durations or any(type(value) is not int or value <= 0 for value in standard_durations):
        msg = "standard_durations must contain positive integer minutes, for example (30, 60, 90)."
        raise ValueError(msg)
    standards = sorted(set(standard_durations))
    keys = ["aep_text", "Location", "Type", "trim_runcode"]
    findings: list[dict[str, Any]] = []
    for key, group in duration_rows.groupby(keys, observed=True, dropna=False):
        minutes = group.get("duration_text", Series(index=group.index, dtype="object")).map(_duration_minutes)
        tested = sorted({float(value) for value in minutes.dropna()})
        minimum = min(tested) if tested else float("nan")
        maximum = max(tested) if tested else float("nan")
        missing = [value for value in standards if minimum < value < maximum and value not in tested]
        unexpected = [value for value in tested if value not in standards]
        counts = pd.to_numeric(group.get("count_TP", Series(index=group.index, dtype="float")), errors="coerce")
        bad_bins: list[str] = []
        for position, (_, row) in enumerate(group.iterrows()):
            count = counts.iloc[position]
            if pd.isna(count) or count != expected_tp_count:
                count_text = "unknown" if pd.isna(count) else f"{count:g}"
                bad_bins.append(f"{row.get('duration_text', 'unknown')}: {count_text}")
        duration_notes: list[str] = []
        if missing:
            duration_notes.append("Missing intermediate durations (min): " + ", ".join(map(str, missing)))
        if unexpected:
            duration_notes.append("Non-standard durations (min): " + ", ".join(f"{value:g}" for value in unexpected))
        if minutes.isna().any():
            duration_notes.append("Unrecognised or missing duration; coverage may be incomplete")
        findings.append(
            {
                **dict(zip(keys, key, strict=True)),
                "min_duration_tested_minutes": minimum,
                "max_duration_tested_minutes": maximum,
                "durations_tested_minutes": ", ".join(f"{value:g}" for value in tested),
                "missing_durations_minutes": ", ".join(map(str, missing)),
                "tp_bin_warning": f"WARNING: Expected {expected_tp_count} storms per duration; " + "; ".join(bad_bins)
                if bad_bins
                else "",
                "duration_coverage_warning": "WARNING: " + "; ".join(duration_notes) if duration_notes else "",
            }
        )
    result = peak_rows.merge(DataFrame(findings), on=keys, how="left", validate="one_to_one")
    boundary_warnings: list[str] = []
    for _, row in result.iterrows():
        peak = _duration_minutes(row.get("mean_Duration", row.get("duration_text")))
        bounds: list[str] = []
        if peak == row["min_duration_tested_minutes"]:
            bounds.append("minimum")
        if peak == row["max_duration_tested_minutes"]:
            bounds.append("maximum")
        boundary_warnings.append(
            "WARNING: Peak sourced from " + " and ".join(bounds) + " tested duration"
            if bounds
            else ("WARNING: Peak duration unavailable" if not isfinite(peak) else "")
        )
    result["peak_duration_warning"] = boundary_warnings
    warning_columns = ["peak_duration_warning", "tp_bin_warning", "duration_coverage_warning"]
    has_warnings = (result[warning_columns] != "").any(axis=1)
    result["findings_status"] = has_warnings.map({True: "WARNING", False: "OK"})
    result.attrs["coverage_settings"] = {
        "Expected storms per duration": str(expected_tp_count),
        "Standard duration schedule (minutes)": ", ".join(map(str, standards)),
        "Duration coverage rule": "Check missing standard durations strictly between each group's sampled bounds.",
    }
    return result


def _ordered_columns(
    df: DataFrame,
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


def _select_internal_names_for_group(group: DataFrame) -> tuple[object, object]:
    """Compatibility helper returning the same deterministic finite event choices."""
    if "internalName" not in group.columns or "AbsMax" not in group.columns:
        return pd.NA, pd.NA

    ordered = finite_sorted_rows(group=group, value_column="AbsMax")
    if ordered.empty:
        return pd.NA, pd.NA

    median_position = len(ordered) // 2
    mean_value = float(ordered["AbsMax"].mean())
    mean_position = int((ordered["AbsMax"] - mean_value).abs().to_numpy().argmin())
    return ordered["internalName"].iloc[median_position], ordered["internalName"].iloc[mean_position]


def combine_processors_from_paths(
    paths_to_process: list[Path],
    include_data_types: Collection[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    log_queue: LogQueue | None = None,  # type: ignore[name-defined]  # guarded by TYPE_CHECKING
) -> ProcessorCollection:
    """Return a :class:`ProcessorCollection` for the provided directories."""
    if include_data_types is None:
        include_data_types = ["POMM"]

    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations_to_include)

    def _run_with_queue(queue: LogQueue) -> ProcessorCollection:  # type: ignore[name-defined]
        logger.info(
            "Starting POMM processing for combine_processors_from_paths. Data types: {}; searching in {} folder(s).",
            include_data_types,
            len(paths_to_process),
        )
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return ProcessorCollection()
        logger.info("Found {} CSV file(s) to process.", len(csv_file_list))

        if normalized_locations:
            logger.info("Applying location filter during processing for {} location(s).", len(normalized_locations))

        results_set_local: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=queue,
            log_level=console_log_level,
            entity_filters=normalized_locations or None,
        )
        processed_count: int = len(results_set_local.processors)
        combined_rows: int = sum(len(processor.df) for processor in results_set_local.processors)
        logger.info("Processed {} file(s); combined {} row(s) before filters.", processed_count, combined_rows)
        return results_set_local

    if log_queue is None:
        with setup_logger(console_log_level=console_log_level) as new_queue:
            results_set: ProcessorCollection = _run_with_queue(queue=new_queue)
    else:
        results_set = _run_with_queue(log_queue)

    return results_set


def combine_df_from_paths(
    paths_to_process: list[Path],
    include_data_types: Collection[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    log_queue: LogQueue | None = None,  # type: ignore[name-defined]
) -> DataFrame:
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

    # The requested input types have already been selected by collect_files().
    # pomm_combine() drops RLL_Qmx because its registered dataformat is Maximums.
    combined: DataFrame = results_set.combine_raw()
    logger.info("Combined POMM/RLL data into a single DataFrame with {} row(s).", len(combined))
    return combined


def aggregated_from_paths(
    paths: list[Path],
    locations_to_include: frozenset[str] | None = None,
    include_data_types: Collection[str] | None = None,
    log_queue: LogQueue | None = None,  # type: ignore[name-defined]
) -> DataFrame:
    """Process directories and return a combined POMM DataFrame."""
    df: DataFrame = combine_df_from_paths(
        paths_to_process=paths,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        log_queue=log_queue,
    )

    if df.empty:
        return df
    logger.info("Aggregated dataframe ready for normalisation with {} row(s).", len(df))

    # RLL_Qmx is a signed *maximum* flow result (Qmax), whereas POMM
    # AbsMax is the larger magnitude of its time-series minimum and maximum.
    # Preserve the original signed Q and the source filename for provenance.
    # No missing RLL time-series minimum can be reconstructed from Qmax.
    if "Chan ID" in df.columns:
        channel_ids = df["Chan ID"].astype("string")
        if "Location" in df.columns:
            # A mixed POMM/RLL collection may have categorical Location values
            # whose categories do not contain the incoming RLL identifiers.
            df["Location"] = df["Location"].astype("string").fillna(value=channel_ids)
        else:
            df["Location"] = channel_ids

    if "Q" in df.columns:
        signed_q = pd.to_numeric(arg=df["Q"], errors="coerce")
        magnitude_q = signed_q.abs()
        if "AbsMax" in df.columns:
            df["AbsMax"] = pd.to_numeric(arg=df["AbsMax"], errors="coerce").fillna(value=magnitude_q)
        else:
            df["AbsMax"] = magnitude_q
        if "SignedAbsMax" in df.columns:
            df["SignedAbsMax"] = df["SignedAbsMax"].fillna(value=signed_q)
        else:
            df["SignedAbsMax"] = signed_q

    if "Type" not in df.columns:
        df["Type"] = "Q"
    else:
        # Keep RLL's Q distinct from POMM's Flow: each is independently
        # grouped, despite both describing discharge.
        df["Type"] = df["Type"].astype("string").fillna(value="Q")

    return df


def find_aep_dur_max(aggregated_df: DataFrame) -> DataFrame:
    """Return peak rows for each AEP/Duration/Location/Type/RunCode group,
    with a column giving the size of each original group.
    """
    group_cols: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
    ]
    try:
        # copy so we don't clobber the caller's DataFrame
        df: DataFrame = aggregated_df.copy()
        # compute size of each group
        df["count_TP"] = df.groupby(by=group_cols, observed=True)["AbsMax"].transform("size")
        count_group_cols: list[str] = [
            col for col in ("aep_text", "Location", "Type", "trim_runcode") if col in df.columns
        ]
        if count_group_cols:
            count_numeric: Series = pd.to_numeric(arg=df["count_TP"], errors="coerce")
            df["count_TP"] = count_numeric.astype(dtype="Int64")
            df["_count_numeric"] = count_numeric.fillna(value=0)
            df["count_TP_aep"] = (
                df.groupby(by=count_group_cols, observed=True)["_count_numeric"].transform("sum").astype("Int64")
            )
            df["count_duration"] = (
                df.groupby(by=count_group_cols, observed=True)["duration_text"].transform("count").astype("Int64")
            )
        # find index of the max in each group
        idx: Series = df.groupby(by=group_cols, observed=True)["AbsMax"].idxmax()
        # select those rows (they already carry a group_count column)
        aep_dur_max: DataFrame = df.loc[idx].reset_index(drop=True).drop(columns=["_count_numeric"], errors="ignore")
        logger.info(
            "Created 'aep_dur_max' DataFrame with peak records and group_count for each AEP-Duration-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error("Missing expected columns for 'aep_dur_max' grouping: {}", e)
        aep_dur_max = DataFrame()
    return aep_dur_max


def find_aep_max(aep_dur_max: DataFrame) -> DataFrame:
    """Return peak rows for each AEP/Location/Type/RunCode group,
    with a column giving the size of each original AEP group.
    """
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrame = aep_dur_max.copy()
        df["count_TP_aep"] = df.groupby(by=group_cols, observed=True)["AbsMax"].transform("size")
        idx: Series = df.groupby(by=group_cols, observed=True)["AbsMax"].idxmax()
        aep_max: DataFrame = df.loc[idx].reset_index(drop=True)
        logger.info(
            "Created 'aep_max' DataFrame with peak records and group_count for each AEP-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error("Missing expected columns for 'aep_max' grouping: {}", e)
        aep_max = DataFrame()
    return aep_max


def save_to_excel(
    aep_dur_max: DataFrame,
    aep_max: DataFrame,
    aggregated_df: DataFrame,
    output_path: Path,
    include_pomm: bool = True,
    timestamp: str | None = None,
    aep_dur_sheet_name: str = "aep-dur-max",
    aep_sheet_name: str = "aep-max",
) -> None:
    """Save peak DataFrames to an Excel file."""
    logger.info("Output path: {}", output_path)
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

    sheet_frames: dict[str, DataFrame] = {
        aep_dur_sheet_name: aep_dur_max,
        aep_sheet_name: aep_max,
    }
    sheet_order: list[str] = [aep_dur_sheet_name, aep_sheet_name]
    sheet_dfs: list[DataFrame] = [aep_dur_max, aep_max]

    if include_pomm:
        sheet_frames["POMM"] = aggregated_df
        sheet_order.append("POMM")
        sheet_dfs.append(aggregated_df)

    data_dictionary_df: DataFrame = _build_data_dictionary(
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

    logger.info("Peak data exported to {}", output_path)


def _build_metadata_rows(
    timestamp: str | None,
    include_pomm: bool,
    aep_dur_max: DataFrame,
    aep_max: DataFrame,
    aggregated_df: DataFrame,
    aep_dur_sheet_name: str,
    aep_sheet_name: str,
) -> Mapping[str, str]:
    """Return ordered metadata rows for the data dictionary sheet."""
    generated_at: str = datetime.now(UTC).astimezone().isoformat(timespec="seconds")
    metadata: dict[str, str] = {
        "Generated at": generated_at,
        "Filename timestamp": timestamp or "not supplied",
        "Generator module": __name__,
        "ryan-tools version": get_tools_version(package="ryan-tools"),
        "Include POMM sheet": "Yes" if include_pomm else "No",
        f"{aep_dur_sheet_name} rows": str(len(aep_dur_max)),
        f"{aep_sheet_name} rows": str(len(aep_max)),
    }

    if include_pomm:
        metadata["POMM rows"] = str(len(aggregated_df))

    metadata.update(aep_max.attrs.get("coverage_settings", {}))

    if "directory_path" in aggregated_df.columns:
        try:
            directories_series: Series[Any] = aggregated_df["directory_path"].dropna()
        except AttributeError:
            directories_series = Series(dtype="string")
        unique_directories: list[str] = sorted({str(Path(dir_value)) for dir_value in directories_series.unique()})
        if unique_directories:
            metadata["Source directories"] = "\n".join(unique_directories)

    return metadata


def _build_data_dictionary(
    registry: ColumnMetadataRegistry,
    sheet_frames: Mapping[str, DataFrame],
    metadata_rows: Mapping[str, str],
) -> DataFrame:
    """Build the DataFrame backing the data dictionary worksheet."""
    return build_data_dictionary(
        registry=registry,
        sheet_frames=sheet_frames,
        metadata_rows=metadata_rows,
    )


def save_peak_report(
    aggregated_df: DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save peak data tables to an Excel file."""
    aep_dur_max: DataFrame = find_aep_dur_max(aggregated_df=aggregated_df)
    aep_max: DataFrame = find_aep_max(aep_dur_max=aep_dur_max)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info("Starting export of peak report to {}", output_path)
    save_to_excel(
        aep_dur_max=aep_dur_max,
        aep_max=aep_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
    )
    logger.info("Completed peak report export to {}", output_path)


def find_aep_dur_median(aggregated_df: DataFrame) -> DataFrame:
    """Return median stats for each AEP/Duration/Location/Type/RunCode group."""
    group_cols: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
    ]
    rows: list[dict[str, Any]] = []
    median_df: DataFrame = DataFrame()
    try:
        df: DataFrame = aggregated_df.copy()
        df["count_TP"] = df.groupby(by=group_cols, observed=True)["AbsMax"].transform("size")
        for _, grp in df.groupby(by=group_cols, observed=True):
            stats_dict, _ = median_calc(
                thinned_df=grp,
                statcol="AbsMax",
                tpcol="tp_text",
                durcol="duration_text",
            )
            row: dict[str, Any] = {
                "aep_text": grp["aep_text"].iloc[0],
                "duration_text": grp["duration_text"].iloc[0],
                "Location": grp["Location"].iloc[0],
                "Type": grp["Type"].iloc[0],
                "trim_runcode": grp["trim_runcode"].iloc[0],
            }
            row.update(stats_dict)
            if "internalName" in grp.columns:
                # Normal processing gets both identifiers directly from the
                # same already-selected statistical records. The fallback is
                # retained for old/custom median helpers returning no metadata.
                if "median_internalName" in row and "mean_internalName" in row:
                    row["internalName"] = row.pop("median_internalName")
                else:
                    median_name, mean_name = _select_internal_names_for_group(grp)
                    row["internalName"] = median_name
                    row["mean_internalName"] = mean_name
            if "median_file" in row:
                row["file"] = row.pop("median_file")
            row["MedianAbsMax"] = row.pop("median")
            rows.append(row)
        median_df = DataFrame(rows)
        if not median_df.empty:
            # Normalise TP / duration text so the "mean storm equals median storm" flag is stable.
            def _normalize_tp(value: object) -> object:
                if pd.isna(cast("Any", value)):
                    return pd.NA
                normalized: str | None = TuflowStringParser.normalize_tp_label(value)
                return pd.NA if normalized is None else normalized

            for column in ("median_TP", "mean_TP"):
                if column in median_df.columns:
                    median_df[column] = median_df[column].apply(cast("Callable[[object], Any]", _normalize_tp))

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
                duration_match: Series[bool] = median_duration_norm.eq(other=mean_duration_norm)
                tp_match: Series[bool] = median_df["median_TP"].eq(other=median_df["mean_TP"])
                mean_storm_matches = (duration_match & tp_match).fillna(value=False)

            median_df["mean_storm_is_median_storm"] = mean_storm_matches
            count_group_cols: list[str] = [
                col for col in ("aep_text", "Location", "Type", "trim_runcode") if col in median_df.columns
            ]
            if count_group_cols and "count_TP" in median_df.columns:
                count_numeric: Series = pd.to_numeric(arg=median_df["count_TP"], errors="coerce")
                median_df["count_TP"] = count_numeric.astype(dtype="Int64")
                median_df["_count_numeric"] = count_numeric.fillna(value=0)
                median_df["count_TP_aep"] = (
                    median_df.groupby(by=count_group_cols, observed=True)["_count_numeric"]
                    .transform(func="sum")
                    .astype(dtype="Int64")
                )
                median_df["count_duration"] = (
                    median_df.groupby(by=count_group_cols, observed=True)["duration_text"]
                    .transform(func="count")
                    .astype(dtype="Int64")
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
        logger.error("Missing expected columns for 'aep_dur_median' grouping: {}", e)
        median_df = DataFrame()
    return median_df


def find_aep_median_max(aep_dur_median: DataFrame) -> DataFrame:
    """Return rows representing the maximum median for each AEP/Location/Type/RunCode group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrame = aep_dur_median.copy()
        df["count_TP_aep"] = df.groupby(by=group_cols, observed=True)["MedianAbsMax"].transform("size")
        idx: Series = df.groupby(by=group_cols, observed=True)["MedianAbsMax"].idxmax()
        aep_med_max: DataFrame = df.loc[idx].reset_index(drop=True)
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
            mean_df: DataFrame = aep_dur_median.copy()
            if "mean_PeakFlow" not in mean_df.columns:
                return DataFrame()
            mean_df["_mean_peakflow_numeric"] = pd.to_numeric(arg=mean_df["mean_PeakFlow"], errors="coerce")
            if mean_df["_mean_peakflow_numeric"].notna().any():
                # When the mean columns are present we track the rows that best represent
                # the maximum mean independently from the median selection above.
                idx_mean: Series = (
                    mean_df[mean_df["_mean_peakflow_numeric"].notna()]
                    .groupby(by=group_cols, observed=True)["_mean_peakflow_numeric"]
                    .idxmax()
                )
                merge_columns: list[str] = mean_value_columns.copy()
                if "mean_storm_is_median_storm" in aep_dur_median.columns:
                    merge_columns.append("mean_storm_is_median_storm")
                mean_subset: DataFrame = mean_df.loc[idx_mean, group_cols + merge_columns]
                aep_med_max = aep_med_max.drop(columns=merge_columns, errors="ignore")
                aep_med_max = aep_med_max.merge(right=mean_subset, on=group_cols, how="left")
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
        logger.error("Missing expected columns for 'aep_median_max' grouping: {}", e)
        aep_med_max = DataFrame()
    return aep_med_max


def find_aep_dur_mean(aggregated_df: DataFrame) -> DataFrame:
    """Return mean stats for each AEP/Duration/Location/Type/RunCode group."""
    aep_dur_median: DataFrame = find_aep_dur_median(aggregated_df=aggregated_df)
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


def find_aep_mean_max(
    aep_dur_mean: DataFrame,
    *,
    standard_durations: Sequence[int] = STANDARD_DURATIONS_MINUTES,
    expected_tp_count: int = 10,
) -> DataFrame:
    """Return rows representing the maximum mean for each AEP/Location/Type/RunCode group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: DataFrame = aep_dur_mean.copy()
        if "mean_PeakFlow" not in df.columns:
            logger.error("'mean_PeakFlow' column not present for mean analysis. Returning empty DataFrame.")
            return DataFrame()

        df["_mean_peakflow_numeric"] = pd.to_numeric(arg=df["mean_PeakFlow"], errors="coerce")
        if "count_TP" in df.columns:
            df["_count_numeric"] = pd.to_numeric(arg=df["count_TP"], errors="coerce").fillna(value=0)
        else:
            df["_count_numeric"] = pd.Series(data=0, index=df.index, dtype="Int64")
        df["count_TP_aep"] = (
            df.groupby(by=group_cols, observed=True)["_count_numeric"].transform(func="sum").astype(dtype="Int64")
        )
        df["count_duration"] = (
            df.groupby(by=group_cols, observed=True)["duration_text"].transform(func="count").astype(dtype="Int64")
            if "duration_text" in df.columns
            else pd.Series(data=0, index=df.index, dtype="Int64")
        )

        valid_df: DataFrame = df[df["_mean_peakflow_numeric"].notna()]
        if valid_df.empty:
            logger.warning("No valid mean peak flow values found. Returning empty DataFrame.")
            return DataFrame()

        idx: Series = valid_df.groupby(by=group_cols, observed=True)["_mean_peakflow_numeric"].idxmax()
        aep_mean_max: DataFrame = df.loc[idx].drop(columns=["_mean_peakflow_numeric", "_count_numeric"])
        aep_mean_max = aep_mean_max.reset_index(drop=True)

        if not aep_mean_max.empty:
            aep_mean_max = _mean_coverage_findings(df, aep_mean_max, standard_durations, expected_tp_count)
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
        logger.error("Missing expected columns for 'aep_mean_max' grouping: {}", e)
        aep_mean_max = DataFrame()
    return aep_mean_max


def _remove_columns_containing(df: DataFrame, substrings: tuple[str, ...]) -> DataFrame:
    """Return ``df`` without columns that include any ``substrings``."""
    filtered_df: DataFrame = df.copy()
    if filtered_df.empty:
        return filtered_df

    columns_to_drop: list[str] = [
        column for column in filtered_df.columns if any(substring in column.lower() for substring in substrings)
    ]
    if columns_to_drop:
        filtered_df = filtered_df.drop(columns=columns_to_drop, errors="ignore")
    return filtered_df


def _median_only_columns(df: DataFrame) -> DataFrame:
    """Return a DataFrame containing only median-focused columns."""
    return _remove_columns_containing(df=df, substrings=("mean",))


def _mean_only_columns(df: DataFrame) -> DataFrame:
    """Return a DataFrame containing only mean-focused columns."""
    return _remove_columns_containing(df=df, substrings=("median",))


def save_peak_report_median(
    aggregated_df: DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_med_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save median-based peak data tables to an Excel file."""
    aep_dur_med: DataFrame = find_aep_dur_median(aggregated_df=aggregated_df)
    aep_med_max: DataFrame = find_aep_median_max(aep_dur_median=aep_dur_med)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info("Starting export of median peak report to {}", output_path)
    aep_dur_med_filtered: DataFrame = _median_only_columns(df=aep_dur_med)
    aep_med_max_filtered: DataFrame = _median_only_columns(df=aep_med_max)
    save_to_excel(
        aep_dur_max=aep_dur_med_filtered,
        aep_max=aep_med_max_filtered,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
    )
    logger.info("Completed median peak report export to {}", output_path)


def save_peak_report_mean(
    aggregated_df: DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_mean_peaks.xlsx",
    include_pomm: bool = True,
    *,
    standard_durations: Sequence[int] = STANDARD_DURATIONS_MINUTES,
    expected_tp_count: int = 10,
) -> None:
    """Save mean-based peak data tables to an Excel file."""
    aep_dur_mean: DataFrame = find_aep_dur_mean(aggregated_df=aggregated_df)
    aep_mean_max: DataFrame = find_aep_mean_max(
        aep_dur_mean=aep_dur_mean, standard_durations=standard_durations, expected_tp_count=expected_tp_count
    )
    logger.info(
        "Preparing mean peak report. POMM rows: {}, AEP-duration mean rows: {}, AEP mean-max rows: {}.",
        len(aggregated_df),
        len(aep_dur_mean),
        len(aep_mean_max),
    )
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info("Starting export of mean peak report to {}", output_path)
    aep_dur_mean_filtered: DataFrame = _mean_only_columns(df=aep_dur_mean)
    aep_mean_max_filtered: DataFrame = _mean_only_columns(df=aep_mean_max)
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
    logger.info("Completed mean peak report export to {}", output_path)
