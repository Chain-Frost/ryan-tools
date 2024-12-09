# ryan_library\scripts\tuflow_culverts_merge.py
import os
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from ryan_library.processors.tuflow.base_processor import (
    BaseProcessor,
    ProcessorCollection,
)
from ryan_library.processors.tuflow.cmx_processor import CmxProcessor
from ryan_library.processors.tuflow.nmx_processor import NmxProcessor
from ryan_library.processors.tuflow.chan_processor import ChanProcessor
from ryan_library.processors.tuflow.cca_processor import CcaProcessor
from ryan_library.functions.file_utils import find_files_parallel, is_non_zero_file
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)
from loguru import logger
from typing import Any

# Processor mapping dictionary
PROCESSOR_MAPPING = {
    "_1d_Cmx.csv": CmxProcessor,
    # "_1d_Nmx.csv": NmxProcessor,
    # "_1d_Chan.csv": ChanProcessor,
    # "_1d_ccA_L.dbf": CcaProcessor,
    # "_Results1d.gpkg": CcaProcessor,
    # "_Results.gpkg": CcaProcessor,
}

# Create a suffix-to-processor mapping for faster lookup
SUFFIX_PROCESSOR_MAP = {
    pattern.lower(): processor for pattern, processor in PROCESSOR_MAPPING.items()
}


def main_processing(
    paths_to_process: list[Path], console_log_level: str = "INFO"
) -> None:
    """
    Generate merged culvert data by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
    """
    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting culvert results files processing...")
        logger.info(os.getcwd())

        # Step 1: Collect and validate files
        csv_file_list = collect_files(paths_to_process)
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        logger.debug(csv_file_list)
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
        logger.info("Now to export")
        logger.info(Path.cwd())
        results_set.export_to_excel()
        # export_results(results)

        logger.info("Culvert results combination completed successfully.")


def collect_files(paths_to_process: list[Path]) -> list[Path]:
    """
    Collect and filter files based on PROCESSOR_MAPPING patterns.

    Args:
        paths_to_process (list[Path]): Directories to search.

    Returns:
        list[Path]: List of valid file paths.
    """
    csv_file_list: list[Path] = []
    # Prepend '*' to each pattern
    patterns = [f"*{pattern}" for pattern in SUFFIX_PROCESSOR_MAP.keys()]
    # All patterns at once

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
        list[pd.DataFrame]: List of processed DataFrames.
    """
    pool_size = calculate_pool_size(len(file_list))

    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    pool_size = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Processing {len(file_list)} files using {pool_size} processes.")
    results: list[BaseProcessor | None] = []
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
        if result is not None:
            if result.processed == True:
                results_set.add_processor(result)

    return results_set


def process_file(file_path: Path) -> BaseProcessor | None:
    """
    Process a single file based on its suffix.

    Args:
        file_path (Path): Path to the file to process.

    Returns:
        BaseProcessor | None: An instance of the processor class if successful, else None.
    """

    file_name = file_path.name.lower()

    # Find a matching processor by checking suffixes
    processor_class = next(
        (
            processor
            for suffix, processor in SUFFIX_PROCESSOR_MAP.items()
            if file_name.endswith(suffix)
        ),
        None,
    )

    if not processor_class:
        logger.warning(f"Unsupported file type: {file_path}. Skipping.")
        return None

    try:
        processor = processor_class(file_path)
        logger.info(f"Processing file: {file_path} with {processor_class.__name__}")
        processor.process()
        if processor.validate_data():
            return processor
        else:
            logger.warning(f"Validation failed for file: {file_path}")
            return None
    except Exception as e:
        logger.exception(f"Failed to process file {file_path}: {e}")
        return None


def export_results(results: list[pd.DataFrame]):
    """
    Concatenate all DataFrames and export to Excel.

    Args:
        results (list[pd.DataFrame]): List of processed DataFrames.
    """
    if not results:
        logger.warning("No results to export.")
        return

    try:
        df = pd.concat(results, ignore_index=True)
        df.fillna(value=pd.NA, inplace=True)
        ExcelExporter().save_to_excel(
            data_frame=df, file_name_prefix="1d_data_processed_", sheet_name="Culverts"
        )
        flat_df = df.groupby(["internalName", "Chan ID"]).max().reset_index()
        ExcelExporter().save_to_excel(
            data_frame=df, file_name_prefix="1d_data_forPivot_", sheet_name="Culverts"
        )
        logger.info("Exported all results successfully.")
    except Exception as e:
        logger.error(f"Failed to export results: {e}")
        raise
