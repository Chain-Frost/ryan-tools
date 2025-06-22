"""Utility helpers for processing POMM CSV files."""

from pathlib import Path
from multiprocessing import Pool
from typing import Iterable

import pandas as pd
from loguru import logger

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger, worker_initializer


# ---------------------------------------------------------------------------
# File collection and processing helpers
# ---------------------------------------------------------------------------

def collect_files(
    paths_to_process: Iterable[Path],
    include_data_types: Iterable[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """Collect and filter files based on suffix configuration."""
    csv_file_list: list[Path] = []
    suffixes: list[str] = []

    data_type_to_suffix = suffixes_config.invert_suffix_to_type()

    for data_type in include_data_types:
        dt_suffixes = data_type_to_suffix.get(data_type)
        if not dt_suffixes:
            logger.error(f"No suffixes found for data type '{data_type}'. Skipping.")
            continue
        suffixes.extend(dt_suffixes)

    if not suffixes:
        logger.error("No suffixes found for the specified data types.")
        return csv_file_list

    patterns = [f"*{suffix}" for suffix in suffixes]

    root_dirs = [p for p in paths_to_process if p.is_dir()]
    invalid_dirs = set(paths_to_process) - set(root_dirs)
    for invalid_dir in invalid_dirs:
        logger.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files = find_files_parallel(root_dirs=root_dirs, patterns=patterns)
    csv_file_list.extend(matched_files)

    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
    return csv_file_list


def process_file(file_path: Path) -> BaseProcessor:
    """Process a single file using the :class:`BaseProcessor` hierarchy."""
    processor = BaseProcessor.from_file(file_path=file_path)
    processor.process()
    if processor.validate_data():
        logger.info(f"Successfully processed file: {file_path}")
    else:
        logger.warning(f"Validation failed for file: {file_path}")
    return processor


def process_files_in_parallel(file_list: list[Path], log_queue) -> ProcessorCollection:
    """Process files using multiprocessing and return a collection."""
    pool_size = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    with Pool(processes=pool_size, initializer=worker_initializer, initargs=(log_queue,)) as pool:
        results = pool.map(func=process_file, iterable=file_list)

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
        csv_file_list = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return ProcessorCollection()

        results_set = process_files_in_parallel(file_list=csv_file_list, log_queue=log_queue)

    return results_set


def combine_df_from_paths(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
) -> pd.DataFrame:
    """Return an aggregated DataFrame for the given directories."""
    results_set = combine_processors_from_paths(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        console_log_level=console_log_level,
    )

    if not results_set.processors:
        return pd.DataFrame()

    return results_set.pomm_combine()


# ---------------------------------------------------------------------------
# Peak report helpers
# ---------------------------------------------------------------------------

def aggregated_from_paths(paths: list[Path]) -> pd.DataFrame:
    """Process directories and return a combined POMM DataFrame."""
    return combine_df_from_paths(paths_to_process=paths)


def find_aep_dur_max(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Return peak rows for each AEP/Duration/Location/Type/RunCode group."""
    return aggregated_df.loc[
        aggregated_df.groupby(
            ["aep_text", "duration_text", "Location", "Type", "trim_runcode"]
        )["AbsMax"].idxmax()
    ].reset_index(drop=True)


def find_aep_max(aep_dur_max: pd.DataFrame) -> pd.DataFrame:
    """Return peak rows for each AEP/Location/Type/RunCode group."""
    return aep_dur_max.loc[
        aep_dur_max.groupby(["aep_text", "Location", "Type", "trim_runcode"])["AbsMax"].idxmax()
    ].reset_index(drop=True)


def save_to_excel(aep_dur_max: pd.DataFrame, aep_max: pd.DataFrame, output_path: Path) -> None:
    """Save peak DataFrames to an Excel file."""
    logger.info(f"Output path: {output_path}")
    with pd.ExcelWriter(output_path) as writer:
        aep_dur_max.to_excel(writer, sheet_name="aep-dur-max", index=False, merge_cells=False)
        aep_max.to_excel(writer, sheet_name="aep-max", index=False, merge_cells=False)
    logger.info(f"Peak data exported to {output_path}")


def save_peak_report(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
) -> None:
    """Save peak data tables to an Excel file."""
    aep_dur_max = find_aep_dur_max(aggregated_df)
    aep_max = find_aep_max(aep_dur_max)
    output_filename = f"{timestamp}{suffix}"
    output_path = script_directory / output_filename
    save_to_excel(aep_dur_max, aep_max, output_path)

