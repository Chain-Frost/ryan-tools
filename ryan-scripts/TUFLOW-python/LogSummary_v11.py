import os
from datetime import datetime
import pandas as pd
import logging
import multiprocessing
from traitlets import Bool
from ryan_functions.misc_functions import setup_logging, calculate_pool_size, save_to_excel
from ryan_functions.data_processing import check_string_TP, check_string_duration, check_string_aep
from ryan_functions.file_utils import find_files_parallel
from typing import Any, Hashable
from pathlib import Path

# updated Sep 2024

# https://pypi.org/project/file-read-backwards/ - log files aren't big enough to need this, can load the file fully into memory

# It won't find the TEF file if there is no ECF file - just the way TUFLOW outputs data.


def search_for_completion(
    line: str, data_dict: dict[str, str | datetime], sim_complete: int
) -> tuple[dict[str, str | datetime], int]:
    """Search log line for simulation completion markers."""
    if "Final Cumulative ME:" in line:
        data_dict["Final Cumulative ME pct"] = line.split(r"%", 1)[0].split(":", 1)[1].strip()
    if "Simulation FINISHED" in line:
        data_dict["EndStatus"] = line.strip()
        sim_complete = 1
    if sim_complete == 1:
        if "Clock Time:" in line:
            data_dict["RunTime"] = line.split("[", 1)[1][:-3].strip()
        if "CPU Time: " in line:
            data_dict["CPU_Time"] = line.split("[", 1)[1][:-3].strip()
        if "End Time (h):" in line:
            data_dict["ModelTime"] = line.split(":", 1)[1].strip()
        if "Start Time (h):" in line:
            data_dict["ModelStart"] = line.split(":", 1)[1].strip()
            sim_complete = 2
    return data_dict, sim_complete


def search_from_top(
    line: str, data_dict: dict[str, str | datetime], success: int, spec_events: bool, spec_scen: bool, spec_var: bool
) -> tuple[dict[str, str | datetime], int, bool, bool, bool]:
    """Search log line for key simulation details."""
    if "Build: " in line:
        data_dict["TUFLOW_version"] = line.split(" ", 1)[1].strip()
    elif "Simulations Log Folder == " in line:
        data_dict["username"] = line.split("\\")[-1].strip()
    elif "Computer Name:" in line:
        data_dict["ComputerName"] = line.split(":", 1)[1].strip()
        success += 1
    elif "! GPU Solver from 2016-03 Release or earlier invoked." in line:
        data_dict["Version_note"] = "! GPU Solver from 2016-03 Release or earlier invoked."
    elif "Simulation Started" in line:
        dt = line.split(":", 1)[1][1:].strip()[:-1]
        data_dict["StartDate"] = datetime.strptime(dt, "%Y-%b-%d %H:%M")
        # 2018-Dec-09 07:45  %Y-%b-%d %H:%M
        success += 1
    elif spec_events:
        linesplit = line.split()
        if len(line) == 1:  # no more events
            spec_events = False
            success += 1
        else:
            key = linesplit[0]
            data_dict[key] = linesplit[1]
    elif "Specified Events:" in line:
        spec_events = True  # next line is events
    elif spec_scen:
        linesplit = line.split()
        if len(line) == 1:  # no more scenarios
            spec_scen = False
            success += 1
        else:
            key = linesplit[0]
            data_dict[key] = linesplit[1]
    elif "Specified Scenarios:" in line:
        spec_scen = True  # next line is scenarios
    elif "No Specified Scenarios." in line or "No Specified Events." in line:
        success += 1  # nothing to process
    elif "Reading .tcf File .. " in line:
        data_dict["TCF"] = line[line.rindex("\\") + 1 : -1]
        data_dict["orig_TCF_path"] = line.split("..", 1)[1].strip()
    elif "BC Database == " in line:
        data_dict["BC_dbase"] = line[line.rindex("\\") + 1 : -1]
    elif "Geometry Control File == " in line:
        data_dict["TGC"] = line[line.rindex("\\") + 1 : -1]
    elif "BC Control File == " in line:
        data_dict["TBC"] = line[line.rindex("\\") + 1 : -1]
    elif "ESTRY Control File == " in line:
        data_dict["ECF"] = line[line.rindex("\\") + 1 : line.rindex("...")]
    elif "BC Event File == " in line:
        data_dict["TEF"] = line[line.rindex("\\") + 1 : line.rindex("...")]
    # need to escape \ with another \
    elif "Number of defined variables:" in line:
        spec_var = True  # next lines will be custom variables
    elif spec_var:  # handle custom variables
        if len(line) == 1:  # no more variables
            spec_var = False
        else:
            linesplit = line[15:].split("==")
            key = linesplit[0].strip()
            # check if it is a built in ~e1~ etc and skip
            if not (
                f"-{key.replace('~','').lower()}" in data_dict
                or f"-{key.replace('~','').upper()}" in data_dict
                or key in ["~E~", "~S~"]
            ):
                data_dict[key] = linesplit[1].strip()
    elif "Output Files to be Pre-fixed by: " in line:
        data_dict["orig_results_path"] = line.split(":", 1)[1].strip()
    elif "Log and message files to be pre-fixed by: " in line:
        data_dict["orig_log_path"] = line.split(":", 1)[1].strip()

    return data_dict, success, spec_events, spec_scen, spec_var


def process_log_file(logfile: str) -> pd.DataFrame:
    """Process a single log file and return the extracted data as a DataFrame."""
    sim_complete: int = 0
    success: int = 0
    spec_events: bool = False
    spec_scen: bool = False
    spec_var: bool = False
    data_dict: dict[str, str | datetime] = {}

    try:
        with open(file=logfile, mode="r", encoding="utf-8") as lfile:
            lines: list[str] = lfile.readlines()
    except Exception as e:
        logging.error(f"Error reading {logfile}: {e}")
        return pd.DataFrame()

    runcode: str = Path(logfile).stem
    logging.info(f"Processing {runcode} : {logfile}")

    for line in reversed(lines):
        data_dict, sim_complete = search_for_completion(line=line, data_dict=data_dict, sim_complete=sim_complete)
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break
            # break the loop as we have extracted what we wanted from the bottom. start processing lines from the top.

    if sim_complete == 2:
        counter = 0
        for line in lines:
            counter += 1
            data_dict, success, spec_events, spec_scen, spec_var = search_from_top(
                line,
                data_dict,
                success,
                spec_events,
                spec_scen,
                spec_var,
            )
            if success == 4 and counter > 4000:
                # we don't want to go too far into the file in case it has lots of errors.
                # Everything should be in the first few hundred lines.
                # But have seen some where it is over 1000 lines to get the required bits
                break
    if success == 4:
        _2023initialisation, _2023final = False, False
        for line in lines[-100:]:
            # this is to get the extra initialisation  times in the 2023 version at the end of the file. Don't want to run through whole file from top.
            _2023initialisation, _2023final, dataDict = find_initialisation_info(
                line, _2023initialisation, _2023final, data_dict
            )

        adj_runcode: str = runcode.replace("+", "_")
        for idx, elem in enumerate(adj_runcode.split("_"), start=1):
            data_dict[f"R{idx}"] = elem
        data_dict["_tcf"] = remove_e_s_from_runcode(runcode, data_dict)
        data_dict["TP"] = safe_apply(check_string_TP, adj_runcode)
        data_dict["Duration"] = safe_apply(check_string_duration, adj_runcode)
        data_dict["AEP"] = safe_apply(check_string_aep, adj_runcode)
        # pprint.pprint(dataDict, width=500)
        df: pd.DataFrame = pd.DataFrame(data=data_dict, index=[0])
        col_dtypes: dict[str, str] = {
            "RunTime": "float64",
            "CPU_Time": "float64",
            "ModelTime": "float64",
            "ModelStart": "float64",
            "Final Cumulative ME pct": "float64",
        }
        df = df.astype(dtype={k: col_dtypes[k] for k in col_dtypes if k in df.columns})
        return df
    else:
        logging.warning(msg=f"{runcode} ({success}) did not complete, skipping")
        return pd.DataFrame()  # Returning an empty DataFrame for consistency


# Calculate TP, Duration, and AEP using the provided functions with error handling applied to runcode
def safe_apply(func, value):
    try:
        return func(value)
    except Exception:
        return ""


def find_initialisation_info(
    line: str, _2023initialisation: bool, _2023final: bool, data_dict: dict[str, str | datetime]
) -> tuple[bool, bool, dict[str, str | datetime]]:
    """Extract initialisation and final times from the log file."""
    if "Initialisation Times" in line:
        _2023initialisation = True
    elif "Final Times" in line:
        _2023initialisation = False
        _2023final = True
    elif _2023initialisation:
        if "Clock Time:" in line:
            data_dict["Clock_init"] = line.split("[", 1)[1][:-3].strip()
        elif "Processor Time:" in line:
            data_dict["Proc_init"] = line.split("[", 1)[1][:-3].strip()
    elif _2023final:
        if "Clock Time:" in line:
            data_dict["Clock_final"] = line.split("[", 1)[1][:-3].strip()
        elif "Processor Time:" in line:
            data_dict["Proc_final"] = line.split("[", 1)[1][:-3].strip()
    return _2023initialisation, _2023final, data_dict


def remove_e_s_from_runcode(runcode: str, data_dict: dict[str, str | datetime]) -> str:
    """
    Removes -e and -s variables based on their values in dataDict from Runcode, treating + as _.

    Parameters:
    - runcode: A string representing the Runcode with possible + signs.
    - dataDict: A dictionary containing -e and -s keys with their corresponding values.

    Returns:
    - A string with -e and -s variables (based on dataDict values) removed from the Runcode.
    """
    # Standardize the separator by replacing + with _
    runcode = runcode.replace("+", "_")

    # Split Runcode into parts
    parts: list[str] = runcode.split(sep="_")

    # Gather all -e and -s values from dataDict to identify which to remove
    patterns_to_remove: list[str | datetime] = [
        value for key, value in data_dict.items() if key.startswith("-e") or key.startswith("-s")
    ]

    # Filter out parts that match the values to remove
    filtered_parts: list[str] = [part for part in parts if part not in patterns_to_remove]

    # Reconstruct the Runcode without the removed parts
    _tcf: str = "_".join(filtered_parts)

    return _tcf


def merge_and_sort_data(frames: list[pd.DataFrame], sort_column: str = "StartDate") -> pd.DataFrame:
    """Merge data frames and sort by a specified column."""
    merged_df: pd.DataFrame = pd.concat(frames)
    merged_df.sort_values(by=sort_column, ascending=False, inplace=True)
    return merged_df


def reorder_columns(
    data_frame: pd.DataFrame,
    first_column: str = "Runcode",
    second_column: str = "_tcf",
    prefix_order: list[str] = ["-e", "-s"],
) -> pd.DataFrame:
    """Reorder DataFrame columns based on specified prefixes and initial column."""
    columns: list[str] = (
        [first_column]
        + [second_column]
        + [col for p in prefix_order for col in sorted(data_frame.columns) if col.startswith(p)]
    )
    remaining_cols: list[Hashable] = [col for col in data_frame if col not in columns]
    return data_frame[columns + remaining_cols]


def main() -> None:
    setup_logging()
    root_dir: str = os.getcwd()
    logging.info(msg="Starting log file processing...")

    files: list[str] = find_files_parallel(root_dir=root_dir, pattern=".tlf", exclude=(".hpc.tlf", ".gpu.tlf"))
    num_files: int = calculate_pool_size(num_files=len(files))
    logging.info(f"Processing {len(files)} files over {num_files} threads")

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results: list[pd.DataFrame] = pool.map(process_log_file, files)

    # Filtering out empty DataFrames
    results = [res for res in results if not res.empty]

    if results:
        merged_data: pd.DataFrame = merge_and_sort_data(results)
        reordered_data: pd.DataFrame = reorder_columns(merged_data)
        save_to_excel(data_frame=reordered_data, file_name_prefix="ModellingLog", sheet_name="Log Summary")
        logging.info("Log file processing completed successfully.")
    else:
        logging.warning("No completed logs found - no output generated.")


if __name__ == "__main__":
    start_time: datetime = datetime.now()
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    logging.info(f"Current working directory: {os.getcwd()}")

    main()

    logging.info(f"Run time: {datetime.now() - start_time}")
    os.system("PAUSE")
