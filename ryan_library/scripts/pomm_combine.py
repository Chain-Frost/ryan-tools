# ryan_library/scripts/pomm_combine.py

import os
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from loguru import logger

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
    ensure_output_directory,
)
from ryan_library.functions.misc_functions import calculate_pool_size, ExcelExporter
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] = ["POMM"],
    console_log_level: str = "INFO",
) -> None:
    """Generate merged culvert data by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
        include_data_types (list[str] | None): List of data types to include. If None, include all.
        console_log_level (str): Logging level for the console."""

    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting culvert results files processing...")
        logger.info(f"Current working directory: {os.getcwd()}")

        # Step 1: Collect and validate files
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        logger.debug(f"Files to process: {csv_file_list}")
        logger.info(f"Total files to process: {len(csv_file_list)}")

        # Step 2: Process files in parallel

        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=log_queue,
        )

        # Step 3: Combine and export results based on scenarios
        logger.info("Now exporting results...")
        export_results(results=results_set)

        logger.info("End of culvert results combination processing")


def collect_files(
    paths_to_process: list[Path],
    include_data_types: list[str],
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

    if include_data_types and len(include_data_types) > 0:
        for data_type in include_data_types:
            dt_suffixes: list[str] | None = data_type_to_suffix.get(data_type)
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
    """Process files using multiprocessing.

    Args:
        file_list (list[Path]): Files to process.
        log_queue (Queue): Logging queue.

    Returns:
        ProcessorCollection: Collection of successfully processed processors."""
    pool_size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    # Initialize the Pool with the worker initializer and pass the log_queue via initargs

    with Pool(
        processes=pool_size,
        initializer=worker_initializer,
        initargs=(log_queue,),
    ) as pool:
        results: list[BaseProcessor] = pool.map(func=process_file, iterable=file_list)

    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(processor=result)

    return results_set


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


def export_results(results: ProcessorCollection) -> None:
    """Export combined DataFrames
    Args:
        results (ProcessorCollection): Collection of processed processors."""
    if not results.processors:
        logger.warning("No results to export.")
        return

    exporter = ExcelExporter()

    # Scenario 1: combine_1d_maximums
    scenario1 = "combined_POMM"
    file_name_prefix1 = "combined_POMM"
    sheet_name1 = "combined_POMM"
    output_path1: Path = Path.cwd()
    # Path("exports/maximums")  # Optional: Specify your desired path

    combined_df1: pd.DataFrame = results.pomm_combine()

    if combined_df1.empty:
        logger.warning(f"No data found for scenario '{scenario1}'. Skipping export.")
    else:
        # Ensure the output directory exists
        ensure_output_directory(output_dir=output_path1)

        # Perform the export for combine_1d_maximums
        exporter.save_to_excel(
            data_frame=combined_df1,
            file_name_prefix=file_name_prefix1,
            sheet_name=sheet_name1,
            output_directory=output_path1,  # Pass the optional path
        )
        logger.info(f"Exported data for scenario '{scenario1}' successfully.")
