# ryan_library/scripts/tuflow_logsummary.py

import logging
import multiprocessing
import os
from multiprocessing import Pool, Process, Queue
from pathlib import Path
from typing import Any
import re
from collections import deque
from datetime import datetime


import pandas as pd

from ryan_library.functions.data_processing import (
    check_string_aep,
    check_string_duration,
    check_string_TP,
    safe_apply,
)
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import calculate_pool_size, save_to_excel
from ryan_library.functions.logging_helpers import (
    configure_multiprocessing_logging,
    log_listener_process,
    setup_logging,
)
from ryan_library.functions.path_stuff import convert_to_relative_path


def search_for_completion(
    line: str, data_dict: dict[str, Any], sim_complete: int
) -> tuple[dict[str, Any], int]:
    """Search log line for simulation completion markers using regular expressions."""
    final_me_match = re.search(r"Final Cumulative ME:\s*([\d.]+)%", line)
    if final_me_match:
        data_dict["Final Cumulative ME pct"] = float(final_me_match.group(1))

    if "Simulation FINISHED" in line:
        data_dict["EndStatus"] = line.strip()
        sim_complete = 1

    if sim_complete == 1:
        clock_time_match = re.search(r"Clock Time:\s*\[(.*?)\]", line)
        cpu_time_match = re.search(r"CPU Time:\s*\[(.*?)\]", line)
        model_time_match = re.search(r"End Time \(h\):\s*(\d+\.?\d*)", line)
        model_start_match = re.search(r"Start Time \(h\):\s*(\d+\.?\d*)", line)

        if clock_time_match:
            data_dict["RunTime"] = clock_time_match.group(1).strip()
        if cpu_time_match:
            data_dict["CPU_Time"] = cpu_time_match.group(1).strip()
        if model_time_match:
            data_dict["ModelTime"] = float(model_time_match.group(1).strip())
        if model_start_match:
            data_dict["ModelStart"] = float(model_start_match.group(1).strip())
            sim_complete = 2

    return data_dict, sim_complete


def search_from_top(
    line: str,
    data_dict: dict[str, Any],
    success: int,
    spec_events: bool,
    spec_scen: bool,
    spec_var: bool,
) -> tuple[dict[str, Any], int, bool, bool, bool]:
    """Search log line for key simulation details using regular expressions."""
    if match := re.match(r"Build:\s*(.*)", line):
        data_dict["TUFLOW_version"] = match.group(1).strip()
    elif match := re.match(r"Simulations Log Folder == .*\\([^\\]+)$", line):
        data_dict["username"] = match.group(1).strip()
    elif match := re.match(r"Computer Name:\s*(.*)", line):
        data_dict["ComputerName"] = match.group(1).strip()
        success += 1
    elif "! GPU Solver from 2016-03 Release or earlier invoked." in line:
        data_dict["Version_note"] = (
            "! GPU Solver from 2016-03 Release or earlier invoked."
        )
    elif match := re.match(r"Simulation Started\s*:\s*(.+)", line):
        dt_str = match.group(1).strip().rstrip(".")
        try:
            data_dict["StartDate"] = datetime.strptime(dt_str, "%Y-%b-%d %H:%M")
            success += 1
        except ValueError:
            logging.warning(f"Failed to parse StartDate from line: {line}")
    elif spec_events:
        if len(line.strip()) == 0:  # no more events
            spec_events = False
            success += 1
        else:
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                key, value = parts
                data_dict[key] = value.strip()
            else:
                logging.warning(f"Unexpected event format: {line}")
    elif "Specified Events:" in line:
        spec_events = True
    elif spec_scen:
        if len(line.strip()) == 0:  # no more scenarios
            spec_scen = False
            success += 1
        else:
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                key, value = parts
                data_dict[key] = value.strip()
            else:
                logging.warning(f"Unexpected scenario format: {line}")
    elif "Specified Scenarios:" in line:
        spec_scen = True
    elif "No Specified Scenarios." in line or "No Specified Events." in line:
        success += 1  # nothing to process
    elif match := re.search(r"Reading \.tcf File .. .*\\([^\\]+)", line):
        data_dict["TCF"] = match.group(1).strip()
        data_dict["orig_TCF_path"] = line.split("..", 1)[1].strip()
    elif match := re.search(r"BC Database == .*\\([^\\]+)", line):
        data_dict["BC_dbase"] = match.group(1).strip()
    elif match := re.search(r"Geometry Control File == .*\\([^\\]+)", line):
        data_dict["TGC"] = match.group(1).strip()
    elif match := re.search(r"BC Control File == .*\\([^\\]+)", line):
        data_dict["TBC"] = match.group(1).strip()
    elif match := re.search(r"ESTRY Control File == .*\\([^\\.]+)", line):
        data_dict["ECF"] = match.group(1).strip()
    elif match := re.search(r"BC Event File == .*\\([^\\.]+)", line):
        data_dict["TEF"] = match.group(1).strip()
    elif "Number of defined variables:" in line:
        spec_var = True  # next lines will be custom variables
    elif spec_var:
        if len(line.strip()) == 0:  # no more variables
            spec_var = False
        else:
            parts = line.split("==")
            if len(parts) != 2:
                logging.warning(f"Unexpected variable format: {line}")
            else:
                key, value = parts[0].strip(), parts[1].strip()
                if not (
                    f"-{key.lower()}" in data_dict
                    or f"-{key.upper()}" in data_dict
                    or key in ["~E~", "~S~"]
                ):
                    data_dict[key] = value
    elif "Output Files to be Pre-fixed by:" in line:
        data_dict["orig_results_path"] = line.split(":", 1)[1].strip()
    elif "Log and message files to be pre-fixed by:" in line:
        data_dict["orig_log_path"] = line.split(":", 1)[1].strip()

    return data_dict, success, spec_events, spec_scen, spec_var


def read_last_n_lines(file_path: Path, n: int = 10000) -> list[str]:
    """
    Read the last n lines from a file efficiently.

    Args:
        file_path (Path): Path to the file.
        n (int): Number of lines to read from the end of the file.

    Returns:
        list[str]: List of the last n lines.
    """
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return list(deque(f, maxlen=n))
    except Exception as e:
        logging.error(f"Error reading last {n} lines from {file_path}: {e}")
        return []


def find_initialisation_info(
    line: str,
    initialisation: bool,
    final: bool,
    data_dict: dict[str, Any],
) -> tuple[bool, bool, dict[str, Any]]:
    """Extract initialisation and final times from the log file using regular expressions."""
    try:
        if "Initialisation Times" in line:
            initialisation = True
        elif "Final Times" in line:
            initialisation = False
            final = True
        elif initialisation:
            clock_match = re.search(r"Clock Time:\s*\[(.*?)\]", line)
            proc_match = re.search(r"Processor Time:\s*\[(.*?)\]", line)
            if clock_match:
                data_dict["Clock_init"] = clock_match.group(1).strip()
            if proc_match:
                data_dict["Proc_init"] = proc_match.group(1).strip()
        elif final:
            clock_match = re.search(r"Clock Time:\s*\[(.*?)\]", line)
            proc_match = re.search(r"Processor Time:\s*\[(.*?)\]", line)
            if clock_match:
                data_dict["Clock_final"] = clock_match.group(1).strip()
            if proc_match:
                data_dict["Proc_final"] = proc_match.group(1).strip()
    except Exception as e:
        logging.error(f"Error parsing initialisation info: {e}")

    return initialisation, final, data_dict


def remove_e_s_from_runcode(runcode: str, data_dict: dict[str, Any]) -> str:
    """
    Removes -e and -s variables based on their values in data_dict from runcode,
    treating + as _.
    """
    runcode = runcode.replace("+", "_")
    parts = runcode.split("_")

    # Collect values to remove
    patterns_to_remove = {
        str(value).lower()
        for key, value in data_dict.items()
        if key.startswith("-e") or key.startswith("-s")
    }

    filtered_parts = [part for part in parts if part.lower() not in patterns_to_remove]
    return "_".join(filtered_parts)


def process_log_file(logfile: str) -> pd.DataFrame:
    """Process a single log file and return the extracted data as a DataFrame."""
    logfile_path = Path(logfile)
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
            logging.info(f"Processing large file: {logfile}")
            lines = read_last_n_lines(logfile_path, n=10000)
            lines_reversed = reversed(lines)
        else:
            with logfile_path.open("r", encoding="utf-8") as lfile:
                lines = lfile.readlines()
            lines_reversed = reversed(lines)
    except Exception as e:
        logging.error(f"Error reading {logfile}: {e}")
        return pd.DataFrame()

    runcode: str = logfile_path.stem
    # Convert logfile_path to relative path if possible
    relative_logfile_path = convert_to_relative_path(logfile_path)
    logging.info(f"Processing {runcode} : {relative_logfile_path}")

    # Search for completion markers from the end
    for line in lines_reversed:
        data_dict, sim_complete = search_for_completion(line, data_dict, sim_complete)
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break

    if sim_complete == 2:
        try:
            if is_large_file:
                # For large files, read from the beginning up to a certain limit
                with logfile_path.open("r", encoding="utf-8") as lfile:
                    for counter, line in enumerate(lfile, 1):
                        result = search_from_top(
                            line, data_dict, success, spec_events, spec_scen, spec_var
                        )
                        if result is None:
                            logging.error(
                                f"search_from_top returned None for file: {relative_logfile_path}"
                            )
                            return pd.DataFrame()
                        data_dict, success, spec_events, spec_scen, spec_var = result
                        if success == 4 and counter > 4000:
                            logging.debug(
                                f"Early termination after {counter} lines for {runcode}"
                            )
                            break
            else:
                for counter, line in enumerate(lines, 1):
                    result = search_from_top(
                        line, data_dict, success, spec_events, spec_scen, spec_var
                    )
                    if result is None:
                        logging.error(
                            f"search_from_top returned None for file: {relative_logfile_path}"
                        )
                        return pd.DataFrame()
                    data_dict, success, spec_events, spec_scen, spec_var = result
                    if success == 4 and counter > 4000:
                        logging.debug(
                            f"Early termination after {counter} lines for {runcode}"
                        )
                        break
        except Exception as e:
            logging.error(f"Error processing top lines in {relative_logfile_path}: {e}")
            return pd.DataFrame()

        if success == 4:
            try:
                # Extract initialization info from the last 100 lines
                last_lines = read_last_n_lines(logfile_path, n=100)
                for line in last_lines:
                    initialisation, final, data_dict = find_initialisation_info(
                        line, False, False, data_dict
                    )

                adj_runcode: str = runcode.replace("+", "_")
                for idx, elem in enumerate(adj_runcode.split("_"), start=1):
                    data_dict[f"R{idx}"] = elem
                data_dict["_tcf"] = remove_e_s_from_runcode(adj_runcode, data_dict)
                data_dict["TP"] = safe_apply(check_string_TP, adj_runcode)
                data_dict["Duration"] = safe_apply(check_string_duration, adj_runcode)
                data_dict["AEP"] = safe_apply(check_string_aep, adj_runcode)

                df: pd.DataFrame = pd.DataFrame([data_dict])
                col_dtypes: dict[str, str] = {
                    "RunTime": "float64",
                    "CPU_Time": "float64",
                    "ModelTime": "float64",
                    "ModelStart": "float64",
                    "Final Cumulative ME pct": "float64",
                }

                for col, dtype in col_dtypes.items():
                    if col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                                dtype
                            )
                        except ValueError:
                            logging.warning(
                                f"Failed to convert column {col} to {dtype} in {runcode}"
                            )
                            df[col] = pd.NA

                return df
            except Exception as e:
                logging.error(f"Error finalizing data for {runcode}: {e}")
                return pd.DataFrame()
        else:
            logging.warning(f"{runcode} ({success}) did not complete, skipping")
            return pd.DataFrame()  # Returning an empty DataFrame for consistency
    else:
        logging.warning(f"{runcode} did not complete, skipping")
        return pd.DataFrame()


def merge_and_sort_data(
    frames: list[pd.DataFrame], sort_column: str = "StartDate"
) -> pd.DataFrame:
    """Merge data frames and sort by a specified column."""
    if not frames:
        return pd.DataFrame()

    merged_df: pd.DataFrame = pd.concat(frames, ignore_index=True)
    if sort_column in merged_df.columns:
        merged_df.sort_values(by=sort_column, ascending=False, inplace=True)
    else:
        logging.warning(f"Sort column '{sort_column}' not found in DataFrame.")
    return merged_df


def reorder_columns(
    data_frame: pd.DataFrame,
    first_column: str = "Runcode",
    second_column: str = "_tcf",
    prefix_order: list[str] = ["-e", "-s"],
) -> pd.DataFrame:
    """Reorder DataFrame columns based on specified prefixes and initial columns."""
    ordered_columns = []
    if first_column in data_frame.columns:
        ordered_columns.append(first_column)
    if second_column in data_frame.columns:
        ordered_columns.append(second_column)

    # Add columns with specified prefixes
    for prefix in prefix_order:
        prefixed_cols = sorted(
            [col for col in data_frame.columns if col.startswith(prefix)]
        )
        ordered_columns.extend(prefixed_cols)

    # Add remaining columns
    remaining_cols = [col for col in data_frame.columns if col not in ordered_columns]
    ordered_columns.extend(sorted(remaining_cols))

    return data_frame[ordered_columns]


def main_processing() -> None:
    """Main processing function to be called by the wrapper script.

    Returns:
        int: Number of successful runs.
    """
    # Create a logging queue
    log_queue: Queue = multiprocessing.Queue()

    # Start the log listener process
    listener = Process(
        target=log_listener_process,
        args=(log_queue, logging.INFO, "processing.log"),
        daemon=True,
    )
    listener.start()

    # Configure the root logger to send logs to the queue
    configure_multiprocessing_logging(log_queue, logging.INFO)

    logger = logging.getLogger()
    logger.info("Starting log file processing...")

    # Find log files
    root_dir = Path.cwd()
    files: list[str] = find_files_parallel(
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
        with Pool(
            processes=pool_size,
            initializer=configure_multiprocessing_logging,
            initargs=(log_queue, logging.INFO),
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
                # Merge and sort data
                merged_df = merge_and_sort_data(results)

                # Reorder columns
                merged_df = reorder_columns(merged_df)

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

    # Shutdown log listener
    log_queue.put_nowait(None)
    listener.join()

    # Return the number of successful runs
    return successful_runs
