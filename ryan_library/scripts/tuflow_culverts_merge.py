# ryan_library\scripts\tuflow_culverts_merge.py
print("loading from repo")
import os
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from ryan_library.processors.tuflow.processor_registry import ProcessorRegistry
from ryan_library.processors.tuflow.processor_collection import (
    ProcessorCollection,
    BaseProcessor,
)
from ryan_library.functions.file_utils import find_files_parallel, is_non_zero_file
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)
from loguru import logger
from typing import Optional
from ryan_library.classes.suffixes_and_dtypes import (
    data_types_config,
)


def main_processing(
    paths_to_process: list[Path], console_log_level: str = "INFO"
) -> None:
    """
    Generate merged culvert data by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
        console_log_level (str): Logging level for the console.
    """
    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting culvert results files processing...")
        logger.info(f"Current working directory: {os.getcwd()}")

        # Step 1: Collect and validate files
        csv_file_list = collect_files(paths_to_process)
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        logger.debug(f"Files to process: {csv_file_list}")
        logger.info(f"Total files to process: {len(csv_file_list)}")

        # Step 2: Process files in parallel
        try:
            results_set: ProcessorCollection = process_files_in_parallel(
                file_list=csv_file_list, log_queue=log_queue
            )
        except Exception as e:
            logger.error(f"Error during multiprocessing: {e}")
            return

        # Step 3: Concatenate and export results
        logger.info("Now exporting results...")
        export_results(results_set)

        logger.info("Culvert results combination completed successfully.")


def collect_files(paths_to_process: list[Path]) -> list[Path]:
    """
    Collect and filter files based on registered suffix patterns.

    Args:
        paths_to_process (list[Path]): Directories to search.

    Returns:
        list[Path]: List of valid file paths.
    """
    csv_file_list: list[Path] = []

    # Retrieve all suffixes from the ProcessorRegistry
    registered_processors = ProcessorRegistry.list_registered_processors()
    suffixes = []
    for data_type, processor_cls in registered_processors.items():
        # Assuming data_types_config has been loaded in suffixes_and_dtypes.py
        data_type_def = data_types_config.data_types.get(data_type.capitalize())
        if data_type_def:
            for suffix in data_type_def.suffixes:
                suffixes.append(suffix.lower())

    # Prepend '*' to each pattern for wildcard searching
    patterns = [f"*{suffix}" for suffix in suffixes]

    root_dirs: list[Path] = [
        Path(path) for path in paths_to_process if Path(path).is_dir()
    ]
    invalid_dirs: set[Path] = set(paths_to_process) - {Path(rd) for rd in root_dirs}
    for invalid_dir in invalid_dirs:
        logger.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files: list[Path] = find_files_parallel(
        root_dirs=root_dirs, patterns=patterns
    )
    csv_file_list.extend(matched_files)

    # Filter non-zero files
    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
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

    results: list[Optional[BaseProcessor]] = []
    # Initialize the Pool with the worker initializer and pass the log_queue via initargs
    with Pool(
        processes=pool_size,
        initializer=worker_initializer,
        initargs=(log_queue,),
    ) as pool:
        try:
            results = pool.map(process_file, file_list)
        except Exception:
            logger.exception("Error during multiprocessing Pool.map")

    results_set = ProcessorCollection()
    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(result)

    return results_set


def process_file(file_path: Path) -> Optional[BaseProcessor]:
    """
    Process a single file based on its suffix.

    Args:
        file_path (Path): Path to the file to process.

    Returns:
        Optional[BaseProcessor]: An instance of the processor class if successful, else None.
    """
    file_name = file_path.name.lower()

    # Iterate through registered processors to find a matching suffix
    registered_processors = ProcessorRegistry.list_registered_processors()
    for data_type, processor_cls in registered_processors.items():
        data_type_def = data_types_config.data_types.get(data_type.capitalize())
        if not data_type_def:
            continue
        for suffix in data_type_def.suffixes:
            if file_name.endswith(suffix.lower()):
                try:
                    processor = processor_cls(file_path)
                    logger.info(
                        f"Processing file: {file_path} with {processor_cls.__name__}"
                    )
                    processor.process()
                    if processor.validate_data():
                        return processor
                    else:
                        logger.warning(f"Validation failed for file: {file_path}")
                        return None
                except Exception as e:
                    logger.exception(f"Failed to process file {file_path}: {e}")
                    return None

    logger.warning(f"No registered processor found for file: {file_path}. Skipping.")
    return None


def export_results(results: ProcessorCollection):
    """
    Concatenate all DataFrames and export to Excel.

    Args:
        results (ProcessorCollection): Collection of processed processors.
    """
    if not results.processors:
        logger.warning("No results to export.")
        return

    try:
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

        # Optionally, create a pivot or aggregated view
        pivot_df = combined_df.groupby(["internalName", "Chan ID"]).max().reset_index()
        exporter.save_to_excel(
            data_frame=pivot_df,
            file_name_prefix="1d_data_forPivot_",
            sheet_name="Culverts",
        )

        logger.info("Exported all results successfully.")
    except Exception as e:
        logger.error(f"Failed to export results: {e}")
        raise
