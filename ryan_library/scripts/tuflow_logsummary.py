# ryan_library/scripts/tuflow_logsummary.py

from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from loguru import logger
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import calculate_pool_size, save_to_excel
from ryan_library.functions.path_stuff import convert_to_relative_path
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    worker_initializer,
)
from ryan_library.functions.parse_tlf import (
    search_for_completion,
    read_log_file,
    process_top_lines,
    finalise_data,
)
from ryan_library.functions.dataframe_helpers import (
    merge_and_sort_data,
    reorder_columns,
)


def process_log_file(logfile: Path) -> pd.DataFrame:
    """Processes a single log file and returns a DataFrame with the extracted data.
    Args:
        logfile (Path): Path object to the log file to process.
    Returns:
        pd.DataFrame: DataFrame containing the processed data, or an empty DataFrame on failure.
    """
    logfile_path: Path = logfile
    sim_complete: int = 0
    success: int = 0
    spec_events: bool = False
    spec_scen: bool = False
    spec_var: bool = False
    data_dict: dict[str, str | float] = {}
    current_section = None

    file_size: int = logfile_path.stat().st_size
    is_large_file: bool = file_size > 10 * 1024 * 1024  # 10 MB

    lines: list[str] = read_log_file(
        logfile_path=logfile_path,
        is_large_file=is_large_file,
    )
    # large file currently does nothing - can be a problem if there are lots of errors logged in the file

    # lines_reversed = list(reversed(lines))

    if not lines:
        return pd.DataFrame()

    runcode: str = logfile_path.stem
    relative_logfile_path: Path = convert_to_relative_path(user_path=logfile_path)
    logger.info(f"Processing {runcode} : {relative_logfile_path}")

    logger.debug(f"search_for_completion: {runcode}")
    for line in lines[-100:]:
        data_dict, sim_complete, current_section = search_for_completion(
            line=line,
            data_dict=data_dict,
            sim_complete=sim_complete,
            current_section=current_section,
        )
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break
    logger.debug(f"search_for_completion: {data_dict}")

    if sim_complete == 2:
        data_dict, success, spec_events, spec_scen, spec_var = process_top_lines(
            logfile_path=logfile_path,
            lines=lines,  # if not is_large_file else [],
            data_dict=data_dict,
            success=success,
            spec_events=spec_events,
            spec_scen=spec_scen,
            spec_var=spec_var,
            is_large_file=is_large_file,
            runcode=runcode,
            relative_logfile_path=relative_logfile_path,
        )
        logger.debug(f"process_top_lines: {data_dict}")

        if success == 4:
            df: pd.DataFrame = finalise_data(
                runcode=runcode,
                data_dict=data_dict,
            )
            if not df.empty:
                logger.debug(df.head())
                return df
            else:
                logger.warning(f"Finalization failed for {runcode}, skipping")
                return pd.DataFrame()
        else:
            logger.warning(f"{runcode} ({success}) did not complete, skipping")
            return pd.DataFrame()
    else:
        logger.warning(f"{runcode} did not complete, skipping")
        return pd.DataFrame()


def main_processing(console_log_level: str | None = None) -> None:
    """Main function to process log files using multiprocessing."""
    # log_dir = Path.home() / "Documents" / "MyAppLogs"
    # log_file = "tuflow_logsummary.log"

    results = [pd.DataFrame()]
    successful_runs: int = 0
    if not console_log_level:
        console_log_level = "INFO"
    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting log file processing...")
        logger.info(
            "Built and tested with 2023-03-AF-iSP-w64. Might miss some items for older versions - use the old log_summary version instead if required"
        )

        root_dir: Path = Path.cwd()
        files: list[Path] = list(
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
                initializer=worker_initializer,
                initargs=(log_queue,),
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
                merged_df: pd.DataFrame = merge_and_sort_data(
                    frames=results, sort_column="StartDate"
                )

                # Define the desired column order
                prioritized_columns = [
                    "Runcode",
                    "trim_tcf",
                    "StartDate",
                ]

                prefix_order = ["-e", "-s"]  # Event and Scenario variables

                second_priority_columns = [
                    "Initialise_RunTime",
                    "Final_RunTime",
                    "Model_Start_Time",
                    "Model_End_Time",
                    "TGC",
                    "TBC",
                    "ECF",
                    "TEF",
                    "BC_dbase",
                    "TCF",
                    "TUFLOW_version",
                    "ComputerName",
                    "username",
                    "EndStatus",
                    "AEP",
                    "Duration",
                    "TP",
                ]

                columns_to_end = ["orig_TCF_path", "orig_log_path", "orig_results_path"]

                # Reorder the columns using the helper function
                merged_df = reorder_columns(
                    data_frame=merged_df,
                    prioritized_columns=prioritized_columns,
                    prefix_order=prefix_order,
                    second_priority_columns=second_priority_columns,
                    columns_to_end=columns_to_end,
                )

                save_to_excel(
                    data_frame=merged_df,
                    file_name_prefix="ModellingLog",
                    sheet_name="Log Summary",
                )

                logger.success("Log file processing completed successfully.")
            except Exception:
                logger.exception("Error during merging/saving DataFrames")
        else:
            logger.warning("No completed logs found - no output generated.")

        logger.success(f"Number of successful runs: {successful_runs}")


if __name__ == "__main__":
    main_processing(console_log_level="DEBUG")
