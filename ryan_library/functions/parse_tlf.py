# ryan_library/functions/parse_tlf.py

from pathlib import Path
from typing import Any, Optional
import re
from collections import deque
from datetime import datetime
import pandas as pd
from loguru import logger
from ryan_library.functions.data_processing import (
    check_string_aep,
    check_string_duration,
    check_string_TP,
    safe_apply,
)
import numpy as np


def search_for_completion(
    line: str, data_dict: dict[str, Any], sim_complete: int
) -> tuple[dict[str, Any], int]:
    final_me_match: re.Match[str] | None = re.search(
        pattern=r"Final Cumulative ME:\s*([\d.]+)%", string=line
    )
    if final_me_match:
        data_dict["Final Cumulative ME pct"] = float(final_me_match.group(1))

    if "Simulation FINISHED" in line:
        data_dict["EndStatus"] = line.strip()
        sim_complete = 1

    if sim_complete == 1:
        clock_time_match: re.Match[str] | None = re.search(
            pattern=r"Clock Time:\s*\[(.*?)\]", string=line
        )
        cpu_time_match: re.Match[str] | None = re.search(
            pattern=r"CPU Time:\s*\[(.*?)\]", string=line
        )
        model_time_match: re.Match[str] | None = re.search(
            pattern=r"End Time \(h\):\s*(\d+\.?\d*)", string=line
        )
        model_start_match: re.Match[str] | None = re.search(
            pattern=r"Start Time \(h\):\s*(\d+\.?\d*)", string=line
        )

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
    if match := re.match(pattern=r"Build:\s*(.*)", string=line):
        data_dict["TUFLOW_version"] = match.group(1).strip()
    elif match := re.match(
        pattern=r"Simulations Log Folder == .*\\([^\\]+)$", string=line
    ):
        data_dict["username"] = match.group(1).strip()
    elif match := re.match(pattern=r"Computer Name:\s*(.*)", string=line):
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
            logger.warning(f"Failed to parse StartDate from line: {line}")
    elif spec_events:
        if len(line.strip()) == 0:
            spec_events = False
            success += 1
        else:
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                key, value = parts
                data_dict[key] = value.strip()
            else:
                logger.warning(f"Unexpected event format: {line}")
    elif "Specified Events:" in line:
        spec_events = True
    elif spec_scen:
        if len(line.strip()) == 0:
            spec_scen = False
            success += 1
        else:
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                key, value = parts
                data_dict[key] = value.strip()
            else:
                logger.warning(f"Unexpected scenario format: {line}")
    elif "Specified Scenarios:" in line:
        spec_scen = True
    elif "No Specified Scenarios." in line or "No Specified Events." in line:
        success += 1
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
        spec_var = True
    elif spec_var:
        if len(line.strip()) == 0:
            spec_var = False
        else:
            parts = line.split("==")
            if len(parts) != 2:
                logger.warning(f"Unexpected variable format: {line}")
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
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return list(deque(f, maxlen=n))
    except Exception as e:
        logger.error(f"Error reading last {n} lines from {file_path}: {e}")
        return []


def find_initialisation_info(
    line: str,
    initialisation: bool,
    final: bool,
    data_dict: dict[str, Any],
) -> tuple[bool, bool, dict[str, Any]]:
    try:
        if "Initialisation Times" in line:
            initialisation = True
        elif "Final Times" in line:
            initialisation = False
            final = True
        elif initialisation:
            clock_match: re.Match[str] | None = re.search(
                pattern=r"Clock Time:\s*\[(.*?)\]", string=line
            )
            proc_match: re.Match[str] | None = re.search(
                pattern=r"Processor Time:\s*\[(.*?)\]", string=line
            )
            if clock_match:
                data_dict["Clock_init"] = clock_match.group(1).strip()
            if proc_match:
                data_dict["Proc_init"] = proc_match.group(1).strip()
        elif final:
            clock_match = re.search(pattern=r"Clock Time:\s*\[(.*?)\]", string=line)
            proc_match = re.search(pattern=r"Processor Time:\s*\[(.*?)\]", string=line)
            if clock_match:
                data_dict["Clock_final"] = clock_match.group(1).strip()
            if proc_match:
                data_dict["Proc_final"] = proc_match.group(1).strip()
    except Exception as e:
        logger.error(f"Error parsing initialisation info: {e}")

    return initialisation, final, data_dict


def remove_e_s_from_runcode(
    runcode: str, data_dict: dict[str, Any], delimiters: str = "_+"
) -> str:
    for delim in delimiters:
        runcode = runcode.replace(delim, "_")
    parts: list[str] = runcode.split("_")

    patterns_to_remove: set[str] = {
        str(value).lower()
        for key, value in data_dict.items()
        if key.startswith("-e") or key.startswith("-s")
    }
    logger.debug(f"Patterns to remove: {patterns_to_remove}")

    filtered_parts: list[str] = [
        part
        for part in parts
        if part.lower() not in patterns_to_remove and part.strip() != ""
    ]
    cleaned_runcode: str = "_".join(filtered_parts)
    logger.debug(f"Original RunCode: {runcode}, Cleaned RunCode: {cleaned_runcode}")
    return cleaned_runcode


# Backward Compatibility Functions
def merge_and_sort_data(
    frames: list[pd.DataFrame], sort_column: str = "StartDate"
) -> pd.DataFrame:
    """
    Wrapper for backward compatibility.
    """
    from ryan_library.functions.dataframe_helpers import merge_and_sort_data as masd

    return masd(frames=frames, sort_column=sort_column)


def reorder_columns(
    data_frame: pd.DataFrame,
    first_column: str = "Runcode",
    second_column: str = "_tcf",
    prefix_order: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Wrapper for backward compatibility.
    """
    from ryan_library.functions.dataframe_helpers import reorder_columns as rc

    return rc(
        data_frame=data_frame,
        prioritized_columns=[first_column, second_column],
        prefix_order=prefix_order,
    )


def read_log_file(logfile_path: Path, is_large_file: bool) -> list[str]:
    """
    Reads the log file based on its size.

    Args:
        logfile_path (Path): Path to the log file.
        is_large_file (bool): Flag indicating if the file is large.

    Returns:
        list[str]: List of lines from the log file.
    """
    try:
        if is_large_file:
            logger.info(f"Processing large file: {logfile_path}")
            lines = read_last_n_lines(logfile_path, n=10000)
            lines_reversed = list(reversed(lines))
        else:
            lines = logfile_path.read_text(encoding="utf-8").splitlines()
            lines_reversed = list(reversed(lines))
        return lines_reversed
    except Exception as e:
        logger.error(f"Error reading {logfile_path}: {e}")
        return []


def process_top_lines(
    logfile_path: Path,
    lines: list[str],
    data_dict: dict[str, Any],
    success: int,
    spec_events: bool,
    spec_scen: bool,
    spec_var: bool,
    is_large_file: bool,
    runcode: str,
    relative_logfile_path: Path,
) -> tuple[dict[str, Any], int, bool, bool, bool]:
    """
    Processes the top lines of the log file to extract relevant data.

    Args:
        logfile_path (Path): Path to the log file.
        lines (list[str]): Lines to process.
        data_dict (dict[str, Any]): Dictionary to store extracted data.
        success (int): Success counter.
        spec_events (bool): Spec events flag.
        spec_scen (bool): Spec scenario flag.
        spec_var (bool): Spec variable flag.
        is_large_file (bool): Flag indicating if the file is large.
        runcode (str): Run code identifier.
        relative_logfile_path (Path): Relative path of the log file.

    Returns:
        tuple[dict[str, Any], int, bool, bool, bool]: Updated data dictionary and status flags.
    """
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
                        return data_dict, success, spec_events, spec_scen, spec_var
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
                    return data_dict, success, spec_events, spec_scen, spec_var
                data_dict, success, spec_events, spec_scen, spec_var = result
                if success == 4 and counter > 4000:
                    logger.debug(
                        f"Early termination after {counter} lines for {runcode}"
                    )
                    break
        return data_dict, success, spec_events, spec_scen, spec_var
    except Exception as e:
        logger.error(f"Error processing top lines in {relative_logfile_path}: {e}")
        return data_dict, success, spec_events, spec_scen, spec_var


def finalise_data(
    runcode: str, data_dict: dict[str, Any], logfile_path, is_large_file: bool
) -> pd.DataFrame:
    """
    Finalizes the data dictionary and creates a DataFrame.

    Args:
        runcode (str): Run code identifier.
        data_dict (dict[str, Any]): Dictionary containing extracted data.
        logfile_path (Path): Path to the log file.
        is_large_file (bool): Flag indicating if the file is large.

    Returns:
        pd.DataFrame: DataFrame containing the processed data.
    """
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
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
                except ValueError:
                    logger.warning(
                        f"Failed to convert column {col} to {dtype} in {runcode}"
                    )
                    df[col] = pd.NA

        return df
    except Exception as e:
        logger.error(f"Error finalizing data for {runcode}: {e}")
        return pd.DataFrame()