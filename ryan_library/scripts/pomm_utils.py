# ryan_library/scripts/pomm_utils.py
"""Utility helpers for processing POMM CSV files."""

from pathlib import Path
from multiprocessing import Pool
from collections.abc import Collection, Iterable, Mapping
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import pandas as pd
from loguru import logger

from ryan_library.classes.column_definitions import ColumnMetadataRegistry
from ryan_library.functions.pandas.median_calc import median_calc

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger, worker_initializer

NAType = type(pd.NA)

NAType = type(pd.NA)

DATA_DICTIONARY_SHEET_NAME: str = "data-dictionary"


def collect_files(
    paths_to_process: Iterable[Path],
    include_data_types: Iterable[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """Collect and filter files based on specified data types.

    Args:
        paths_to_process (list[Path]): Directories to search.
        include_data_types (list[str] ): List of data types to include.
        suffixes_config (SuffixesConfig ): Suffixes configuration instance.

    Returns:
        list[Path]: List of valid file paths."""

    csv_file_list: list[Path] = []
    suffixes: list[str] = []

    # Determine which suffixes to include based on data types
    # Invert suffixes config
    data_type_to_suffix: dict[str, list[str]] = suffixes_config.invert_suffix_to_type()

    for data_type in include_data_types:
        dt_suffixes: list[str] | None = data_type_to_suffix.get(data_type)
        if not dt_suffixes:
            logger.error(f"No suffixes found for data type '{data_type}'. Skipping.")
            continue
        suffixes.extend(dt_suffixes)

    if not suffixes:
        logger.error("No suffixes found for the specified data types.")
        return csv_file_list

    # Prepend '*' for wildcard searching
    patterns: list[str] = [f"*{suffix}" for suffix in suffixes]

    root_dirs: list[Path] = [p for p in paths_to_process if p.is_dir()]
    invalid_dirs: set[Path] = set(paths_to_process) - set(root_dirs)
    for invalid_dir in invalid_dirs:
        logger.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files: list[Path] = find_files_parallel(root_dirs=root_dirs, patterns=patterns)
    csv_file_list.extend(matched_files)

    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
    return csv_file_list


def process_file(file_path: Path, location_filter: frozenset[str] | None = None) -> BaseProcessor:
    """Process a single file by delegating to BaseProcessor.

    Args:
        file_path (Path): Path to the file to process.
        location_filter: Normalized set of locations to retain.

    Returns:
        BaseProcessor: The processed BaseProcessor instance.

    Raises:
        Exception: If processing fails."""
    try:
        # BaseProcessor.from_file determines and instantiates the correct processor
        processor: BaseProcessor = BaseProcessor.from_file(file_path=file_path)
        processor.process()

        if location_filter:
            processor.filter_locations(location_filter)

        if processor.validate_data():
            logger.info(f"Successfully processed file: {file_path}")
        else:
            logger.warning(f"Validation failed for file: {file_path}")
        return processor
    except Exception as e:
        logger.exception(f"Failed to process file {file_path}: {e}")
        raise


def process_files_in_parallel(
    file_list: list[Path], log_queue, location_filter: frozenset[str] | None = None
) -> ProcessorCollection:
    """Process files using multiprocessing and return a collection."""
    pool_size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    with Pool(processes=pool_size, initializer=worker_initializer, initargs=(log_queue,)) as pool:
        task_arguments = [(file_path, location_filter) for file_path in file_list]
        results: list[BaseProcessor] = pool.starmap(func=process_file, iterable=task_arguments)

    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(processor=result)
    return results_set


def combine_processors_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
) -> ProcessorCollection:
    """Return a :class:`ProcessorCollection` for the provided directories."""
    if include_data_types is None:
        include_data_types = ["POMM"]

    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations_to_include)

    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting POMM processing for combine_processors_from_paths.")
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return ProcessorCollection()

        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=log_queue,
            location_filter=normalized_locations if normalized_locations else None,
        )

    if normalized_locations:
        results_set.filter_locations(normalized_locations)

    return results_set


def combine_df_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
) -> pd.DataFrame:
    """Return an aggregated DataFrame for the given directories."""
    results_set: ProcessorCollection = combine_processors_from_paths(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        console_log_level=console_log_level,
        locations_to_include=locations_to_include,
    )

    if not results_set.processors:
        return pd.DataFrame()

    return results_set.pomm_combine()


def aggregated_from_paths(paths: list[Path], locations_to_include: Collection[str] | None = None) -> pd.DataFrame:
    """Process directories and return a combined POMM DataFrame."""
    return combine_df_from_paths(paths_to_process=paths, locations_to_include=locations_to_include)


def find_aep_dur_max(aggregated_df: pd.DataFrame) -> pd.DataFrame:
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
        df: pd.DataFrame = aggregated_df.copy()
        # compute size of each group
        df["aep_dur_bin"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        # find index of the max in each group
        idx = df.groupby(group_cols, observed=True)["AbsMax"].idxmax()
        # select those rows (they already carry a group_count column)
        aep_dur_max: pd.DataFrame = df.loc[idx].reset_index(drop=True)
        logger.info(
            "Created 'aep_dur_max' DataFrame with peak records and group_count for each AEP-Duration-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_dur_max' grouping: {e}")
        aep_dur_max = pd.DataFrame()
    return aep_dur_max


def find_aep_max(aep_dur_max: pd.DataFrame) -> pd.DataFrame:
    """Return peak rows for each AEP/Location/Type/RunCode group,
    with a column giving the size of each original AEP group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: pd.DataFrame = aep_dur_max.copy()
        df["aep_bin"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        idx = df.groupby(group_cols, observed=True)["AbsMax"].idxmax()
        aep_max: pd.DataFrame = df.loc[idx].reset_index(drop=True)
        logger.info(
            "Created 'aep_max' DataFrame with peak records and group_count for each AEP-Location-Type-RunCode group."
        )
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_max' grouping: {e}")
        aep_max = pd.DataFrame()
    return aep_max


def save_to_excel(
    aep_dur_max: pd.DataFrame,
    aep_max: pd.DataFrame,
    aggregated_df: pd.DataFrame,
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
    data_dictionary_df: pd.DataFrame = _build_data_dictionary(
        registry=registry,
        sheet_frames={
            aep_dur_sheet_name: aep_dur_max,
            aep_sheet_name: aep_max,
        },
        metadata_rows=metadata_rows,
    )

    with pd.ExcelWriter(output_path) as writer:
        aep_dur_max.to_excel(
            excel_writer=writer,
            sheet_name=aep_dur_sheet_name,
            index=False,
            merge_cells=False,
        )
        aep_max.to_excel(
            excel_writer=writer,
            sheet_name=aep_sheet_name,
            index=False,
            merge_cells=False,
        )
        if include_pomm:
            aggregated_df.to_excel(
                excel_writer=writer,
                sheet_name="POMM",
                index=False,
                merge_cells=False,
            )
        data_dictionary_df.to_excel(
            excel_writer=writer,
            sheet_name=DATA_DICTIONARY_SHEET_NAME,
            index=False,
            merge_cells=False,
        )

    logger.info(f"Peak data exported to {output_path}")


def _build_metadata_rows(
    timestamp: str | None,
    include_pomm: bool,
    aep_dur_max: pd.DataFrame,
    aep_max: pd.DataFrame,
    aggregated_df: pd.DataFrame,
    aep_dur_sheet_name: str,
    aep_sheet_name: str,
) -> Mapping[str, str]:
    """Return ordered metadata rows for the data dictionary sheet."""

    generated_at: str = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    metadata: dict[str, str] = {
        "Generated at": generated_at,
        "Filename timestamp": timestamp if timestamp else "not supplied",
        "Generator module": __name__,
        "ryan_functions version": _resolve_package_version("ryan_functions"),
        "Include POMM sheet": "Yes" if include_pomm else "No",
        f"{aep_dur_sheet_name} rows": str(len(aep_dur_max)),
        f"{aep_sheet_name} rows": str(len(aep_max)),
    }

    if include_pomm:
        metadata["POMM rows"] = str(len(aggregated_df))

    if "directory_path" in aggregated_df.columns:
        try:
            directories_series = aggregated_df["directory_path"].dropna()
        except AttributeError:
            directories_series = pd.Series(dtype="string")
        unique_directories = sorted({str(Path(dir_value)) for dir_value in directories_series.unique()})
        if unique_directories:
            metadata["Source directories"] = "\n".join(unique_directories)

    return metadata


def _resolve_package_version(package_name: str) -> str:
    """Return the installed version for ``package_name`` if available."""

    try:
        return version(package_name)
    except PackageNotFoundError:
        return "unknown"


def _build_data_dictionary(
    registry: ColumnMetadataRegistry,
    sheet_frames: Mapping[str, pd.DataFrame],
    metadata_rows: Mapping[str, str],
) -> pd.DataFrame:
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

        dtype_map: dict[str, str] = {column: str(dtype) for column, dtype in frame.dtypes.items()}
        definitions = registry.iter_definitions(columns, sheet_name=sheet_name)

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

    return pd.DataFrame(
        rows,
        columns=["sheet", "column", "description", "value_type", "pandas_dtype"],
    )


def save_peak_report(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save peak data tables to an Excel file."""
    aep_dur_max: pd.DataFrame = find_aep_dur_max(aggregated_df=aggregated_df)
    aep_max: pd.DataFrame = find_aep_max(aep_dur_max=aep_dur_max)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of peak report to {output_path}")
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
    logger.info(f"Completed peak report export to {output_path}")


def find_aep_dur_median(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Return median stats for each AEP/Duration/Location/Type/RunCode group."""
    group_cols: list[str] = [
        "aep_text",
        "duration_text",
        "Location",
        "Type",
        "trim_runcode",
    ]
    rows: list[dict[str, Any]] = []
    try:
        df: pd.DataFrame = aggregated_df.copy()
        df["aep_dur_bin"] = df.groupby(group_cols, observed=True)["AbsMax"].transform("size")
        for _, grp in df.groupby(group_cols, observed=True):
            stats_dict, _ = median_calc(
                thinned_df=grp,
                statcol="AbsMax",
                tpcol="tp_text",
                durcol="duration_text",
            )
            row = {
                "aep_text": grp["aep_text"].iloc[0],
                "duration_text": grp["duration_text"].iloc[0],
                "Location": grp["Location"].iloc[0],
                "Type": grp["Type"].iloc[0],
                "trim_runcode": grp["trim_runcode"].iloc[0],
            }
            row.update(stats_dict)
            row["MedianAbsMax"] = row.pop("median")
            rows.append(row)
        median_df = pd.DataFrame(rows)
        if not median_df.empty:

            def norm_tp(value: str | int | float | None) -> str | NAType:
                if pd.isna(value):
                    return pd.NA
                cleaned = str(value).replace("TP", "")
                numeric = pd.to_numeric(cleaned, errors="coerce")
                return pd.NA if pd.isna(numeric) else f"TP{int(numeric):02d}"

            def norm_duration(value: object) -> float:
                if pd.isna(value):
                    return float("nan")
                text = str(value).strip().lower()
                suffixes: tuple[str, ...] = (
                    "hours",
                    "hour",
                    "hrs",
                    "hr",
                    "h",
                    "minutes",
                    "minute",
                    "mins",
                    "min",
                    "m",
                )
                for suffix in suffixes:
                    if text.endswith(suffix):
                        text = text[: -len(suffix)]
                        break
                cleaned = text.strip()
                numeric = pd.to_numeric(cleaned, errors="coerce")
                return float(numeric) if pd.notna(numeric) else float("nan")

            for column in ("median_TP", "mean_TP"):
                if column in median_df.columns:
                    median_df[column] = median_df[column].apply(norm_tp)

            mean_storm_matches = pd.Series(False, index=median_df.index)
            required_cols = {
                "median_duration",
                "mean_Duration",
                "median_TP",
                "mean_TP",
            }
            if required_cols.issubset(median_df.columns):
                median_duration_norm = median_df["median_duration"].map(norm_duration)
                mean_duration_norm = median_df["mean_Duration"].map(norm_duration)
                duration_match = median_duration_norm.eq(mean_duration_norm)
                tp_match = median_df["median_TP"].eq(median_df["mean_TP"])
                mean_storm_matches = (duration_match & tp_match).fillna(False)

            median_df["mean_storm_is_median_storm"] = mean_storm_matches

            id_columns: list[str] = ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
            mean_columns: list[str] = [
                "mean_including_zeroes",
                "mean_excluding_zeroes",
                "mean_PeakFlow",
                "mean_Duration",
                "mean_TP",
            ]
            median_columns: list[str] = ["MedianAbsMax", "median_duration", "median_TP"]
            info_columns: list[str] = ["low", "high", "count", "count_bin", "mean_storm_is_median_storm"]

            ordered_cols: list[str] = []
            for group in (id_columns, mean_columns, median_columns):
                ordered_cols.extend([col for col in group if col in median_df.columns])

            remaining_cols: list[str] = [
                col for col in median_df.columns if col not in ordered_cols and col not in info_columns
            ]
            ordered_cols.extend(remaining_cols)
            ordered_cols.extend([col for col in info_columns if col in median_df.columns])

            median_df = median_df[ordered_cols]
        logger.info("Created 'aep_dur_median' DataFrame with median records for each AEP-Duration group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_dur_median' grouping: {e}")
        median_df = pd.DataFrame()
    return median_df


def find_aep_median_max(aep_dur_median: pd.DataFrame) -> pd.DataFrame:
    """Return rows representing the maximum median for each AEP/Location/Type/RunCode group."""
    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: pd.DataFrame = aep_dur_median.copy()
        df["aep_bin"] = df.groupby(group_cols, observed=True)["MedianAbsMax"].transform("size")
        idx = df.groupby(group_cols, observed=True)["MedianAbsMax"].idxmax()
        aep_med_max: pd.DataFrame = df.loc[idx].reset_index(drop=True)
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
            mean_df: pd.DataFrame = aep_dur_median.copy()
            mean_df["_mean_peakflow_numeric"] = pd.to_numeric(mean_df.get("mean_PeakFlow"), errors="coerce")
            if mean_df["_mean_peakflow_numeric"].notna().any():
                idx_mean = (
                    mean_df[mean_df["_mean_peakflow_numeric"].notna()]
                    .groupby(group_cols, observed=True)["_mean_peakflow_numeric"]
                    .idxmax()
                )
                merge_columns: list[str] = mean_value_columns.copy()
                if "mean_storm_is_median_storm" in aep_dur_median.columns:
                    merge_columns.append("mean_storm_is_median_storm")
                mean_subset: pd.DataFrame = mean_df.loc[idx_mean, group_cols + merge_columns]
                aep_med_max = aep_med_max.drop(columns=merge_columns, errors="ignore")
                aep_med_max = aep_med_max.merge(mean_subset, on=group_cols, how="left")
            mean_df = mean_df.drop(columns=["_mean_peakflow_numeric"], errors="ignore")
        if not aep_med_max.empty:
            id_columns: list[str] = ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
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
                "count",
                "count_bin",
                "mean_storm_is_median_storm",
                "aep_bin",
            ]

            ordered_cols: list[str] = []
            for group in (id_columns, mean_columns, median_columns):
                ordered_cols.extend([col for col in group if col in aep_med_max.columns])

            remaining_cols: list[str] = [
                col for col in aep_med_max.columns if col not in ordered_cols and col not in info_columns
            ]
            ordered_cols.extend(remaining_cols)
            ordered_cols.extend([col for col in info_columns if col in aep_med_max.columns])

            aep_med_max = aep_med_max[ordered_cols]
        if not aep_med_max.empty:
            id_columns: list[str] = ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
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
                "count",
                "count_bin",
                "mean_storm_is_median_storm",
                "aep_bin",
            ]

            ordered_cols: list[str] = []
            for group in (id_columns, mean_columns, median_columns):
                ordered_cols.extend([col for col in group if col in aep_med_max.columns])

            remaining_cols: list[str] = [
                col for col in aep_med_max.columns if col not in ordered_cols and col not in info_columns
            ]
            ordered_cols.extend(remaining_cols)
            ordered_cols.extend([col for col in info_columns if col in aep_med_max.columns])

            aep_med_max = aep_med_max[ordered_cols]
        logger.info("Created 'aep_median_max' DataFrame with maximum median records for each AEP group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_median_max' grouping: {e}")
        aep_med_max = pd.DataFrame()
    return aep_med_max


def find_aep_dur_mean(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Return mean stats for each AEP/Duration/Location/Type/RunCode group."""

    aep_dur_median: pd.DataFrame = find_aep_dur_median(aggregated_df=aggregated_df)
    if aep_dur_median.empty:
        return aep_dur_median

    id_columns: list[str] = ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
    mean_columns: list[str] = [
        "mean_including_zeroes",
        "mean_excluding_zeroes",
        "mean_PeakFlow",
        "mean_Duration",
        "mean_TP",
    ]
    info_columns: list[str] = ["low", "high", "count", "count_bin", "mean_storm_is_median_storm"]

    ordered_cols: list[str] = []
    for group in (id_columns, mean_columns):
        ordered_cols.extend([col for col in group if col in aep_dur_median.columns])

    remaining_cols: list[str] = [
        col for col in aep_dur_median.columns if col not in ordered_cols and col not in info_columns
    ]
    ordered_cols.extend(remaining_cols)
    ordered_cols.extend([col for col in info_columns if col in aep_dur_median.columns])

    return aep_dur_median[ordered_cols]


def find_aep_mean_max(aep_dur_mean: pd.DataFrame) -> pd.DataFrame:
    """Return rows representing the maximum mean for each AEP/Location/Type/RunCode group."""

    group_cols: list[str] = ["aep_text", "Location", "Type", "trim_runcode"]
    try:
        df: pd.DataFrame = aep_dur_mean.copy()
        if "mean_PeakFlow" not in df.columns:
            logger.error("'mean_PeakFlow' column not present for mean analysis. Returning empty DataFrame.")
            return pd.DataFrame()

        df["_mean_peakflow_numeric"] = pd.to_numeric(df["mean_PeakFlow"], errors="coerce")
        df["mean_bin"] = df.groupby(group_cols, observed=True)["_mean_peakflow_numeric"].transform("count")

        valid_df: pd.DataFrame = df[df["_mean_peakflow_numeric"].notna()]
        if valid_df.empty:
            logger.warning("No valid mean peak flow values found. Returning empty DataFrame.")
            return pd.DataFrame()

        idx = valid_df.groupby(group_cols, observed=True)["_mean_peakflow_numeric"].idxmax()
        aep_mean_max: pd.DataFrame = df.loc[idx].drop(columns=["_mean_peakflow_numeric"]).reset_index(drop=True)

        if not aep_mean_max.empty:
            id_columns: list[str] = ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
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
                "count",
                "count_bin",
                "mean_storm_is_median_storm",
                "mean_bin",
            ]

            ordered_cols: list[str] = []
            for group in (id_columns, mean_columns):
                ordered_cols.extend([col for col in group if col in aep_mean_max.columns])

            remaining_cols: list[str] = [
                col for col in aep_mean_max.columns if col not in ordered_cols and col not in info_columns
            ]
            ordered_cols.extend(remaining_cols)
            ordered_cols.extend([col for col in info_columns if col in aep_mean_max.columns])

            aep_mean_max = aep_mean_max[ordered_cols]

        logger.info("Created 'aep_mean_max' DataFrame with maximum mean records for each AEP group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_mean_max' grouping: {e}")
        aep_mean_max = pd.DataFrame()
    return aep_mean_max


def save_peak_report_median(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_med_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save median-based peak data tables to an Excel file."""
    aep_dur_med: pd.DataFrame = find_aep_dur_median(aggregated_df=aggregated_df)
    aep_med_max: pd.DataFrame = find_aep_median_max(aep_dur_median=aep_dur_med)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of median peak report to {output_path}")
    logger.info(f"Starting export of median peak report to {output_path}")
    save_to_excel(
        aep_dur_max=aep_dur_med,
        aep_max=aep_med_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
    )


def save_peak_report_mean(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_mean_peaks.xlsx",
    include_pomm: bool = True,
) -> None:
    """Save mean-based peak data tables to an Excel file."""

    aep_dur_mean: pd.DataFrame = find_aep_dur_mean(aggregated_df=aggregated_df)
    aep_mean_max: pd.DataFrame = find_aep_mean_max(aep_dur_mean=aep_dur_mean)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    logger.info(f"Starting export of mean peak report to {output_path}")
    save_to_excel(
        aep_dur_max=aep_dur_mean,
        aep_max=aep_mean_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
        include_pomm=include_pomm,
        timestamp=timestamp,
        aep_dur_sheet_name="aep-dur-mean",
        aep_sheet_name="aep-mean-max",
    )
    logger.info(f"Completed mean peak report export to {output_path}")
    logger.info(f"Completed mean peak report export to {output_path}")
