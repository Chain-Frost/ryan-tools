# ryan_library\scripts\tuflow_culverts_merge.py
import os
from multiprocessing import Queue, Process, Pool
from pathlib import Path
import pandas as pd
import logging
from ryan_library.functions.logging_helpers import (
    configure_multiprocessing_logging,
    log_listener_process,
    LoggerConfigurator,
    worker_initializer,
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.cmx_processor import CmxProcessor
from ryan_library.processors.tuflow.nmx_processor import NmxProcessor
from ryan_library.processors.tuflow.chan_processor import ChanProcessor
from ryan_library.processors.tuflow.cca_processor import CcaProcessor
from ryan_library.functions.file_utils import find_files_parallel, is_non_zero_file
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.misc_functions import ExcelExporter

# Processor mapping dictionary with lowercased keys
PROCESSOR_MAPPING = {
    "_1d_Cmx.csv": CmxProcessor,
    "_1d_Nmx.csv": NmxProcessor,
    "_1d_Chan.csv": ChanProcessor,
    "_1d_ccA_L.dbf": CcaProcessor,
    "_Results1d.gpkg": CcaProcessor,
    "_Results.gpkg": CcaProcessor,
}

# Create a suffix-to-processor mapping for faster lookup
SUFFIX_PROCESSOR_MAP = {Path(k).suffix.lower(): v for k, v in PROCESSOR_MAPPING.items()}


def tf_culv_merge(paths_to_process) -> None:
    """
    Generate peak reports by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[str]): List of directory paths to search for files.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting culvert report generation.")
    logger.info(os.getcwd())

    # Step 1: Collect and validate files
    csv_file_list = collect_files(paths_to_process)
    logger.info(f"Total files to process: {len(csv_file_list)}")

    if not csv_file_list:
        logger.info("No valid files found to process.")
        return

    # Step 2: Set up logging
    log_queue = setup_logging_queue()

    # Step 3: Process files in parallel
    try:
        results = process_files_in_parallel(csv_file_list, log_queue)
    except Exception as e:
        logger.error(f"Error during multiprocessing: {e}")
        return

    # Step 4: Finalize logging
    finalize_logging(log_queue)

    # Step 5: Concatenate and export results
    export_results(results)

    logger.info("Culvert report generation completed successfully.")


def collect_files(paths_to_process: list[str]) -> list[Path]:
    """
    Collect and filter files based on PROCESSOR_MAPPING patterns.

    Args:
        paths_to_process (list[str]): Directories to search.

    Returns:
        list[Path]: List of valid file paths.
    """
    csv_file_list = []
    patterns = list(PROCESSOR_MAPPING.keys())  # All patterns at once

    root_dirs = [Path(path) for path in paths_to_process if Path(path).is_dir()]
    invalid_dirs = set(paths_to_process) - {str(rd) for rd in root_dirs}
    for invalid_dir in invalid_dirs:
        logging.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files = find_files_parallel(root_dirs=root_dirs, patterns=patterns)
    csv_file_list.extend(matched_files)

    # Filter non-zero files
    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
    return csv_file_list


def setup_logging_queue() -> Queue:
    """
    Set up a logging queue and start the listener process.

    Returns:
        Queue: The logging queue.
    """
    log_queue = Queue()
    listener = Process(
        target=log_listener_process,
        args=(log_queue, logging.INFO),
        daemon=True,
    )
    listener.start()

    # Configure the root logger to send logs to the queue
    configure_multiprocessing_logging(log_queue, logging.INFO)

    # Initialize the LoggerConfigurator
    logger_config = LoggerConfigurator(
        log_level=logging.INFO,
        log_file=None,  # Set to a file path if needed
        use_rotating_file=False,
        enable_color=True,
    )
    logger_config.configure()

    # Get the logger and send a starting message
    logger = logging.getLogger(__name__)
    logger.info("Starting culvert report processing...", extra={"simple_format": True})

    return log_queue


def process_files_in_parallel(
    file_list: list[Path], log_queue: Queue
) -> list[pd.DataFrame]:
    """
    Process files using multiprocessing.

    Args:
        file_list (list[Path]): Files to process.
        log_queue (Queue): Logging queue.

    Returns:
        list[pd.DataFrame]: List of processed DataFrames.
    """
    pool_size = calculate_pool_size(len(file_list))
    logger = logging.getLogger(__name__)
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    with Pool(
        processes=pool_size,
        initializer=worker_initializer,
        initargs=(log_queue, logging.INFO),
    ) as pool:
        try:
            results = pool.map(process_file, file_list)
            pool.close()
            pool.join()
            return results
        except Exception as e:
            logger.error(f"Error during multiprocessing: {e}")
            pool.terminate()
            pool.join()
            raise


def finalize_logging(log_queue: Queue):
    """
    Finalize logging by sending termination signal to the listener.

    Args:
        log_queue (Queue): Logging queue.
    """
    log_queue.put_nowait(None)


def export_results(results: list[pd.DataFrame]):
    """
    Concatenate all DataFrames and export to Excel.

    Args:
        results (list[pd.DataFrame]): List of processed DataFrames.
    """
    logger = logging.getLogger(__name__)
    if not results:
        logger.warning("No results to export.")
        return

    try:
        df = pd.concat(results, ignore_index=True)
        df.fillna(value=pd.NA, inplace=True)
        ExcelExporter.save_to_excel(
            data_frame=df, file_name_prefix="1d_data_processed_", sheet_name="Culverts"
        )
        flat_df = df.groupby(["internalName", "Chan ID"]).max().reset_index()
        ExcelExporter.save_to_excel(
            data_frame=df, file_name_prefix="1d_data_forPivot_", sheet_name="Culverts"
        )
        logger.info("Exported all results successfully.")
    except Exception as e:
        logger.error(f"Failed to export results: {e}")
        raise


def process_file(file_path: Path) -> pd.DataFrame | None:
    """
    Process the file based on its suffix.

    Args:
        file_path (Path): Path to the file to be processed.

    Returns:
        Optional[pd.DataFrame]: Processed DataFrame or None if unsupported.
    """
    suffix = file_path.suffix.lower()
    processor_class = SUFFIX_PROCESSOR_MAP.get(suffix)
    if not processor_class:
        logging.warning(f"Unsupported file type: {file_path}. Skipping.")
        return None

    try:
        processor = processor_class(file_path)
        logging.info(f"Processing file: {file_path} with {processor_class.__name__}")
        return processor.process()
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        return None


def export_to_excel(df: pd.DataFrame, suffix: str, directory: str | Path = ".") -> None:
    """
    Export the DataFrame to an Excel file with a timestamp and given suffix.

    Args:
        df (pd.DataFrame): DataFrame to export.
        suffix (str): Suffix to include in the export file name.
        directory (str | Path, optional): Directory to save the exported file. Defaults to '.'.
    """
    from datetime import datetime

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    datetime_str = datetime.now().strftime("%Y%m%d-%H%M")
    export_name = directory / f"{datetime_str}_1d_data_{suffix}.xlsx"
    df.to_excel(export_name, index=False)
    logging.info(f"Exported data to {export_name}")
