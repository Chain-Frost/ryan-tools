# LogSummary.py

import pandas as pd
import sys
import logging
import multiprocessing
import os
from multiprocessing import Queue
from pathlib import Path
from typing import List

from ryan_library.functions.logging_helpers import (
    configure_multiprocessing_logging,
    log_listener_process,
    setup_logging,
)
from ryan_library.scripts.tuflow_logsummary import process_log_file
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import calculate_pool_size, save_to_excel


def worker_init(log_queue: Queue):
    """
    Initialize logging for worker processes.
    This function is called by each worker process when it starts.
    """
    configure_multiprocessing_logging(log_queue, logging.INFO)


def main():
    """Main function to set up logging and process log files."""
    # Create a logging queue
    log_queue: Queue = multiprocessing.Queue()

    # Start the log listener process
    listener = multiprocessing.Process(
        target=log_listener_process,
        args=(log_queue, logging.INFO, "processing.log"),
        daemon=True,
    )
    listener.start()

    # Configure the root logger to send logs to the queue
    # Note: We don't call setup_logging here because the listener handles it
    logger = logging.getLogger()
    logger.handlers = []  # Remove any existing handlers
    queue_handler = logging.handlers.QueueHandler(log_queue)
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    # Change working directory to the script's directory
    try:
        script_dir = Path(__file__).parent.resolve()
        os.chdir(script_dir)
        logger.info(f"Current working directory: {script_dir}")
    except Exception as e:
        logger.error(f"Failed to change working directory: {e}")
        # Signal the listener to shut down
        log_queue.put_nowait(None)
        listener.join()
        sys.exit(1)

    # Find log files
    root_dir = Path.cwd()
    files: List[str] = find_files_parallel(
        root_dir=str(root_dir), pattern=".tlf", exclude=(".hpc.tlf", ".gpu.tlf")
    )
    logger.info(f"Found {len(files)} log files.")

    if not files:
        logger.warning("No log files found to process.")
    else:
        # Determine pool size
        pool_size = calculate_pool_size(num_files=len(files))
        logger.info(f"Processing {len(files)} files using {pool_size} processes.")

        # Initialize a multiprocessing Pool with worker initializer
        with multiprocessing.Pool(
            processes=pool_size,
            initializer=worker_init,
            initargs=(log_queue,),
        ) as pool:
            try:
                results = pool.map(process_log_file, files)
            except Exception as e:
                logger.error(f"Error during multiprocessing Pool.map: {e}")
                results = []

        # Filtering out empty DataFrames
        results = [res for res in results if not res.empty]
        successful_runs = len(results)

        if results:
            try:
                # Merge all DataFrames
                merged_df = pd.concat(results, ignore_index=True)

                # Sort by StartDate if it exists
                if "StartDate" in merged_df.columns:
                    merged_df.sort_values(by="StartDate", ascending=False, inplace=True)
                else:
                    logger.warning("Sort column 'StartDate' not found in DataFrame.")

                # Reorder columns
                first_column = "Runcode"
                second_column = "_tcf"
                prefix_order = ["-e", "-s"]
                ordered_columns = []
                if first_column in merged_df.columns:
                    ordered_columns.append(first_column)
                if second_column in merged_df.columns:
                    ordered_columns.append(second_column)
                for prefix in prefix_order:
                    ordered_columns.extend(
                        sorted(
                            [col for col in merged_df.columns if col.startswith(prefix)]
                        )
                    )
                remaining_cols = [
                    col for col in merged_df.columns if col not in ordered_columns
                ]
                ordered_columns.extend(sorted(remaining_cols))
                merged_df = merged_df[ordered_columns]

                # Save to Excel
                save_to_excel(
                    data_frame=merged_df,
                    file_name_prefix="ModellingLog",
                    sheet_name="Log Summary",
                )
                logger.info("Log file processing completed successfully.")
            except Exception as e:
                logger.error(f"Error during merging/saving DataFrames: {e}")
        else:
            logger.warning("No completed logs found - no output generated.")

        # Report number of successful runs
        logger.info(f"Number of successful runs: {successful_runs}")
        print(f"Number of successful runs: {successful_runs}")

    # Shutdown log listener
    log_queue.put_nowait(None)
    listener.join()

    # Use os.system("PAUSE") for Windows
    os.system("PAUSE")


if __name__ == "__main__":
    main()
