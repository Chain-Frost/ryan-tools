# ryan_library/scripts/tuflow_logsummary.py

from multiprocessing import Pool
from pathlib import Path
from typing import Any, List
import pandas as pd
import numpy as np
from loguru import logger
from ryan_library.functions.data_processing import (
    check_string_aep,
    check_string_duration,
    check_string_TP,
    safe_apply,
)
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import calculate_pool_size, save_to_excel
from ryan_library.functions.path_stuff import convert_to_relative_path
from ryan_library.functions.loguru_helpers import (
    logging_context,
    initialize_worker,
)
from ryan_library.functions.parse_tlf import (
    search_for_completion,
    search_from_top,
    read_last_n_lines,
    find_initialisation_info,
    remove_e_s_from_runcode,
    merge_and_sort_data,
    reorder_columns,
)


def pool_initializer(log_queue: Any) -> None:
    """
    Initializer for each worker process to set up logging.

    Args:
        log_queue (Queue): The shared logging queue.
    """
    initialize_worker(log_queue)


def process_log_file(logfile: Path) -> pd.DataFrame:
    """
    Processes a single log file and returns a DataFrame with the extracted data.

    Args:
        logfile (Path): Path object to the log file to process.

    Returns:
        pd.DataFrame: DataFrame containing the processed data, or an empty DataFrame on failure.
    """
    logfile_path = logfile
    sim_complete: int = 0
    success: int = 0
    spec_events: bool = False
    spec_scen: bool = False
    spec_var: bool = False
    data_dict: dict[str, Any] = {}

    file_size = logfile_path.stat().st_size
    is_large_file = file_size > 10 * 1024 * 1024  # 10 MB

    try:
        if is_large_file:
            logger.info(f"Processing large file: {logfile_path}")
            lines = read_last_n_lines(logfile_path, n=10000)
            lines_reversed = reversed(lines)
        else:
            lines = logfile_path.read_text(encoding="utf-8").splitlines()
            lines_reversed = reversed(lines)
    except Exception as e:
        logger.error(f"Error reading {logfile_path}: {e}")
        return pd.DataFrame()

    runcode: str = logfile_path.stem
    relative_logfile_path = convert_to_relative_path(logfile_path)
    logger.info(f"Processing {runcode} : {relative_logfile_path}")

    for line in lines_reversed:
        data_dict, sim_complete = search_for_completion(line, data_dict, sim_complete)
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break

    if sim_complete == 2:
        try:
            if is_large_file:
                with logfile_path.open("r", encoding="utf-8") as lfile:
                    for counter, line in enumerate(lfile, 1):
                        result = search_from_top(
                            line, data_dict, success, spec_events, spec_scen, spec_var
                        )
                        if result is None:
                            logger.error(
                                f"search_from_top returned None for file: {relative_logfile_path}"
                            )
                            return pd.DataFrame()
                        data_dict, success, spec_events, spec_scen, spec_var = result
                        if success == 4 and counter > 4000:
                            logger.debug(
                                f"Early termination after {counter} lines for {runcode}"
                            )
                            break
            else:
                for counter, line in enumerate(lines, 1):
                    result = search_from_top(
                        line, data_dict, success, spec_events, spec_scen, spec_var
                    )
                    if result is None:
                        logger.error(
                            f"search_from_top returned None for file: {relative_logfile_path}"
                        )
                        return pd.DataFrame()
                    data_dict, success, spec_events, spec_scen, spec_var = result
                    if success == 4 and counter > 4000:
                        logger.debug(
                            f"Early termination after {counter} lines for {runcode}"
                        )
                        break
        except Exception as e:
            logger.error(f"Error processing top lines in {relative_logfile_path}: {e}")
            return pd.DataFrame()

        if success == 4:
            try:
                last_lines = read_last_n_lines(logfile_path, n=100)
                initialisation = False
                final = False
                for line in last_lines:
                    initialisation, final, data_dict = find_initialisation_info(
                        line, initialisation, final, data_dict
                    )

                adj_runcode: str = runcode.replace("+", "_")
                for idx, elem in enumerate(adj_runcode.split("_"), start=1):
                    data_dict[f"R{idx}"] = elem
                data_dict["_tcf"] = remove_e_s_from_runcode(adj_runcode, data_dict)
                data_dict["TP"] = safe_apply(check_string_TP, adj_runcode)
                data_dict["Duration"] = safe_apply(check_string_duration, adj_runcode)
                data_dict["AEP"] = safe_apply(check_string_aep, adj_runcode)

                df: pd.DataFrame = pd.DataFrame([data_dict])
                col_dtypes: dict[str, type] = {
                    "RunTime": np.float64,
                    "CPU_Time": np.float64,
                    "ModelTime": np.float64,
                    "ModelStart": np.float64,
                    "Final Cumulative ME pct": np.float64,
                }

                for col, dtype in col_dtypes.items():
                    if col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                                dtype
                            )
                        except ValueError:
                            logger.warning(
                                f"Failed to convert column {col} to {dtype} in {runcode}"
                            )
                            df[col] = pd.NA

                return df
            except Exception as e:
                logger.error(f"Error finalizing data for {runcode}: {e}")
                return pd.DataFrame()
        else:
            logger.warning(f"{runcode} ({success}) did not complete, skipping")
            return pd.DataFrame()
    else:
        logger.warning(f"{runcode} did not complete, skipping")
        return pd.DataFrame()


def main_processing() -> None:
    """
    Main function to process log files using multiprocessing.
    """
    # log_dir = Path.home() / "Documents" / "MyAppLogs"
    # log_file = "tuflow_logsummary.log"
    results = [pd.DataFrame()]
    successful_runs: int = 0
    with logging_context(
        log_level="INFO",
        # log_file=log_file,  # Specify a log file if desired
        # log_dir=log_dir,  # Specify log directory
        max_bytes=10**6,  # 1 MB
        backup_count=5,
        enable_color=True,
        additional_sinks=None,  # Add any additional sinks if needed
    ) as logger_manager:
        logger.info("Starting log file processing...")

        root_dir = Path.cwd()
        files: List[Path] = list(
            find_files_parallel(
                root_dirs=[root_dir],
                patterns="*.tlf",
                excludes=["*.hpc.tlf", "*.gpu.tlf"],
            )
        )

        logger.info(f"Found {len(files)} log files.")

        if not files:
            logger.warning("No log files found to process.")
        else:
            pool_size = calculate_pool_size(num_files=len(files))
            logger.info(f"Processing {len(files)} files using {pool_size} processes.")

            # Initialize the Pool with the worker initializer and pass the log_queue via initargs
            with Pool(
                processes=pool_size,
                initializer=pool_initializer,
                initargs=(logger_manager.log_queue,),
            ) as pool:
                try:
                    results = pool.map(process_log_file, files)
                except Exception:
                    logger.exception("Error during multiprocessing Pool.map")
                    results = []

        # After the Pool context, shutdown is handled by the logging_context
        # Filter out empty DataFrames
        results: list[pd.DataFrame] = [res for res in results if not res.empty]
        successful_runs = len(results)
    if results:
        try:
            merged_df = merge_and_sort_data(results)
            merged_df = reorder_columns(merged_df)
            save_to_excel(
                data_frame=merged_df,
                file_name_prefix="ModellingLog",
                sheet_name="Log Summary",
            )
            logger.info("Log file processing completed successfully.")
        except Exception:
            logger.exception("Error during merging/saving DataFrames")
    else:
        logger.warning("No completed logs found - no output generated.")

    logger.info(f"Number of successful runs: {successful_runs}")


if __name__ == "__main__":
    main_processing()
