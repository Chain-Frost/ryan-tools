# ryan_library/scripts/pomm_utils.py
"""Utility helpers for processing POMM CSV files."""

from pathlib import Path
from multiprocessing import Pool
from collections.abc import Iterable
from typing import Any

import pandas as pd
from loguru import logger

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


def process_file(file_path: Path) -> BaseProcessor:
    """Process a single file by delegating to BaseProcessor.

    Args:
        file_path (Path): Path to the file to process.

    Returns:
        BaseProcessor: The processed BaseProcessor instance.

    Raises:
        Exception: If processing fails."""
    try:
        # BaseProcessor.from_file determines and instantiates the correct processor
        processor: BaseProcessor = BaseProcessor.from_file(file_path=file_path)
        processor.process()

        if processor.validate_data():
            logger.info(f"Successfully processed file: {file_path}")
        else:
            logger.warning(f"Validation failed for file: {file_path}")
        return processor
    except Exception as e:
        logger.exception(f"Failed to process file {file_path}: {e}")
        raise


def process_files_in_parallel(file_list: list[Path], log_queue) -> ProcessorCollection:
    """Process files using multiprocessing and return a collection."""
    pool_size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    with Pool(processes=pool_size, initializer=worker_initializer, initargs=(log_queue,)) as pool:
        results: list[BaseProcessor] = pool.map(func=process_file, iterable=file_list)

    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(processor=result)
    return results_set


def combine_processors_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
) -> ProcessorCollection:
    """Return a :class:`ProcessorCollection` for the provided directories."""
    if include_data_types is None:
        include_data_types = ["POMM"]

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

        results_set: ProcessorCollection = process_files_in_parallel(file_list=csv_file_list, log_queue=log_queue)

    return results_set


def combine_df_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
) -> pd.DataFrame:
    """Return an aggregated DataFrame for the given directories."""
    results_set: ProcessorCollection = combine_processors_from_paths(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        console_log_level=console_log_level,
    )

    if not results_set.processors:
        return pd.DataFrame()

    return results_set.pomm_combine()


def aggregated_from_paths(paths: list[Path]) -> pd.DataFrame:
    """Process directories and return a combined POMM DataFrame."""
    return combine_df_from_paths(paths_to_process=paths)


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
) -> None:
    """Save peak DataFrames to an Excel file."""
    logger.info(f"Output path: {output_path}")
    with pd.ExcelWriter(output_path) as writer:
        aep_dur_max.to_excel(
            excel_writer=writer,
            sheet_name="aep-dur-max",
            index=False,
            merge_cells=False,
        )
        aep_max.to_excel(excel_writer=writer, sheet_name="aep-max", index=False, merge_cells=False)
        aggregated_df.to_excel(excel_writer=writer, sheet_name="POMM", index=False, merge_cells=False)

    logger.info(f"Peak data exported to {output_path}")


def save_peak_report(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
) -> None:
    """Save peak data tables to an Excel file."""
    aep_dur_max: pd.DataFrame = find_aep_dur_max(aggregated_df=aggregated_df)
    aep_max: pd.DataFrame = find_aep_max(aep_dur_max=aep_dur_max)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    save_to_excel(
        aep_dur_max=aep_dur_max,
        aep_max=aep_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
    )


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
                "MedianAbsMax": stats_dict["median"],
            }
            rows.append(row)
        median_df = pd.DataFrame(rows)
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
        logger.info("Created 'aep_median_max' DataFrame with maximum median records for each AEP group.")
    except KeyError as e:
        logger.error(f"Missing expected columns for 'aep_median_max' grouping: {e}")
        aep_med_max = pd.DataFrame()
    return aep_med_max


def save_peak_report_median(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_med_peaks.xlsx",
) -> None:
    """Save median-based peak data tables to an Excel file."""
    aep_dur_med: pd.DataFrame = find_aep_dur_median(aggregated_df=aggregated_df)
    aep_med_max: pd.DataFrame = find_aep_median_max(aep_dur_median=aep_dur_med)
    output_filename: str = f"{timestamp}{suffix}"
    output_path: Path = script_directory / output_filename
    save_to_excel(
        aep_dur_max=aep_dur_med,
        aep_max=aep_med_max,
        aggregated_df=aggregated_df,
        output_path=output_path,
    )
