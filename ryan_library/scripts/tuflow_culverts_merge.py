# ryan_library/scripts/tuflow_culverts_merge.py

import os
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from loguru import logger
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

from ryan_library.functions.file_utils import find_files_parallel, is_non_zero_file
from ryan_library.functions.misc_functions import calculate_pool_size, ExcelExporter
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)
from ryan_library.classes.suffixes_and_dtypes import (
    SuffixesConfig,
    suffixes_config,
)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str],
    console_log_level: str = "INFO",
) -> None:
    """
    Generate merged culvert data by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
        include_data_types (list[str] | None): List of data types to include. If None, include all.
        console_log_level (str): Logging level for the console.
    """

    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting culvert results files processing...")
        logger.info(f"Current working directory: {os.getcwd()}")

        # Step 1: Collect and validate files
        csv_file_list = collect_files(
            paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=suffixes_config,
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        logger.debug(f"Files to process: {csv_file_list}")
        logger.info(f"Total files to process: {len(csv_file_list)}")

        # Step 2: Process files in parallel
        try:
            results_set: ProcessorCollection = process_files_in_parallel(
                file_list=csv_file_list,
                log_queue=log_queue,
            )
        except Exception as e:
            logger.error(f"Error during multiprocessing: {e}")
            return

        # Step 3: Concatenate and export results
        logger.info("Now exporting results...")
        export_results(results_set)

        logger.info("Culvert results combination completed successfully.")


def collect_files(
    paths_to_process: list[Path],
    include_data_types: list[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """
    Collect and filter files based on specified data types.

    Args:
        paths_to_process (list[Path]): Directories to search.
        include_data_types (list[str] ): List of data types to include.
        suffixes_config (SuffixesConfig ): Suffixes configuration instance.

    Returns:
        list[Path]: List of valid file paths.
    """

    csv_file_list: list[Path] = []
    suffixes = []

    # Determine which suffixes to include based on data types
    # Invert suffixes config
    data_type_to_suffix = suffixes_config.invert_suffix_to_type()

    if include_data_types and len(include_data_types) > 0:
        for data_type in include_data_types:
            dt_suffixes = data_type_to_suffix.get(data_type)
            if not dt_suffixes:
                logger.error(
                    f"No suffixes found for data type '{data_type}'. Skipping."
                )
                continue
            suffixes.extend([s for s in dt_suffixes])  # Preserve capitalization

    if not suffixes:
        logger.error("No suffixes found for the specified data types.")
        return csv_file_list

    # Prepend '*' for wildcard searching
    patterns: list[str] = [f"*{suffix}" for suffix in suffixes]

    root_dirs: list[Path] = [p for p in paths_to_process if p.is_dir()]
    invalid_dirs: set[Path] = set(paths_to_process) - set(root_dirs)
    for invalid_dir in invalid_dirs:
        logger.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files: list[Path] = find_files_parallel(
        root_dirs=root_dirs, patterns=patterns
    )
    csv_file_list.extend(matched_files)

    # Filter for non-zero files
    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
    # logger.debug(f"Collected files: {csv_file_list}")

    return csv_file_list


def process_files_in_parallel(file_list: list[Path], log_queue) -> ProcessorCollection:
    """
    Process files using multiprocessing.

    Args:
        file_list (list[Path]): Files to process.
        log_queue (Queue): Logging queue.

    Returns:
        ProcessorCollection: Collection of successfully processed processors.
    """
    pool_size = calculate_pool_size(len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    # Initialize the Pool with the worker initializer and pass the log_queue via initargs

    with Pool(
        processes=pool_size,
        initializer=worker_initializer,
        initargs=(log_queue,),
    ) as pool:
        try:
            results = pool.map(process_file, file_list)
        except Exception:
            logger.error("Error during multiprocessing Pool.map")
            return results_set

    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(result)

    return results_set


def process_file(file_path: Path) -> BaseProcessor:
    """
    Process a single file by delegating to BaseProcessor.

    Args:
        file_path (Path): Path to the file to process.

    Returns:
        BaseProcessor: The processed BaseProcessor instance.

    Raises:
        Exception: If processing fails.
    """
    try:
        # BaseProcessor.from_file determines and instantiates the correct processor
        processor: BaseProcessor = BaseProcessor.from_file(file_path)
        processor.process()

        if processor.validate_data():
            logger.info(f"Successfully processed file: {file_path}")
            return processor
        else:
            logger.warning(f"Validation failed for file: {file_path}")
            raise ValueError(f"Validation failed for file: {file_path}")
    except Exception as e:
        logger.exception(f"Failed to process file {file_path}: {e}")
        raise


def export_results(results: ProcessorCollection) -> None:
    """
    Concatenate all DataFrames and export to Excel.

    Args:
        results (ProcessorCollection): Collection of processed processors.
    """
    if not results.processors:
        logger.warning("No results to export.")
        return

    df_list = [processor.df for processor in results.processors]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.fillna(value=pd.NA, inplace=True)

    # Export to Excel
    exporter = ExcelExporter()
    exporter.save_to_excel(
        data_frame=combined_df,
        file_name_prefix="1d_data_processed_",
        sheet_name="Culverts",
    )

    # Optionally create a pivot or aggregated view
    pivot_df = combined_df.groupby(["internalName", "Chan ID"]).max().reset_index()
    exporter.save_to_excel(
        data_frame=pivot_df,
        file_name_prefix="1d_data_forPivot_",
        sheet_name="Culverts",
    )

    logger.info("Exported all results successfully.")
