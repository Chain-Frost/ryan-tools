# ryan_library/functions/parse_tlf.py

from pathlib import Path
from typing import Any
from datetime import datetime
import re
from loguru import logger
import pandas as pd
from ryan_library.classes.tuflow_string_classes import TuflowStringParser

# Precompile regex patterns at the module level for efficiency and thread safety
REGEX_PATTERNS: dict[str, re.Pattern] = {
    # Looks for the literal heading ``Initialisation Times`` anywhere in the log.
    "initialisation_times": re.compile(r"Initialisation Times"),
    # Matches ``Final Times`` so the parser knows the log has moved to the summary section.
    "final_times": re.compile(r"Final Times"),
    # Capture Final Cumulative ME Percentage with optional minus sign and decimals.
    # Example: ``Final Cumulative ME:   -0.45%`` -> stores ``-0.45``.
    "final_me": re.compile(r"Final Cumulative ME:\s*(-?[\d.]+)%"),
    # Flags the line ``Simulation FINISHED`` to indicate a successful run completion.
    "simulation_finished": re.compile(r"Simulation FINISHED"),
    # Captures a number (integer or decimal) before ``h`` inside square brackets.
    # Example: ``Clock Time: [1.25 h]`` -> ``1.25``.
    "clock_time": re.compile(r"Clock Time:.*\[(?P<time>[-+]?\d*\.\d+|\d+)\s*h\]"),
    # Same structure as ``clock_time`` but for CPU usage.
    "processor_time": re.compile(r"Processor Time:.*\[(?P<time>[-+]?\d*\.\d+|\d+)\s*h\]"),
    # Grabs the numeric end time after ``End Time (h):`` such as ``End Time (h): 24``.
    "model_end_time": re.compile(r"End Time \(h\):\s*(\d+\.?\d*)"),
    # Grabs the numeric start time after ``Start Time (h):``.
    "model_start_time": re.compile(r"Start Time \(h\):\s*(\d+\.?\d*)"),
    # Captures the full path to the ``.tcf`` file reported as ``Input File: path/to/model.tcf``.
    "input_file": re.compile(r"Input File:\s*(.+\.tcf)"),
    # Captures any characters following ``Log File:`` so the log file path can be recorded.
    "log_path": re.compile(r"Log File:\s*(.+)"),
    # Collects comma-separated GPU identifiers, e.g. ``GPU Device IDs == 0, 1``.
    "gpu_device_ids": re.compile(r"GPU Device IDs\s*==\s*(?P<ids>[\d,\s]+)"),
    # Extracts the variable/value pair described on ``BC Event Source == variable | value`` lines, ignoring case.
    # Example: ``BC Event Source == ~E1~ | rainfall.tsf``.
    "bc_event_source": re.compile(
        r"BC Event Source\s*==\s*(?P<variable>[^|]+?)\s*\|\s*(?P<value>.+)$",
        flags=re.IGNORECASE,
    ),
}

# Define excluded variable patterns globally for efficiency
EXCLUDED_VARIABLES: set[str] = {
    "~E~",
    "~E1~",
    "~E2~",
    "~E3~",
    "~E4~",
    "~E5~",
    "~E6~",
    "~E7~",
    "~E8~",
    "~E9~",
    "~S~",
    "~S1~",
    "~S2~",
    "~S3~",
    "~S4~",
    "~S5~",
    "~S6~",
    "~S7~",
    "~S8~",
    "~S9~",
}
# Precompile regex patterns
SET_VARIABLE_PATTERN: re.Pattern[str] = re.compile(
    pattern=r"^Set Variable\s+(?P<var>~[ES]\d*~|\w+)\s*==\s*(?P<val>.+)$",
    flags=re.IGNORECASE,
)
# ``SET_VARIABLE_PATTERN`` recognises configuration lines such as ``Set Variable ~E1~ == inflow.csv``
# and stores the variable name (``~E1~``) plus the assigned value (``inflow.csv``).


def _normalise_bcdbase_variable(variable: str) -> str:
    """Normalise the BC Database variable name for consistent column naming."""

    cleaned_variable: str = variable.strip()
    if cleaned_variable.startswith("~") and cleaned_variable.endswith("~"):
        cleaned_variable = cleaned_variable[1:-1]

    # ``r"[eEsS]\d+"`` accepts placeholders like ``E1`` or ``s12`` so they can be converted into ``-e1`` style keys.
    if re.fullmatch(pattern=r"[eEsS]\d+", string=cleaned_variable):
        return f"-{cleaned_variable.lower()}"

    return cleaned_variable


def _extract_bcdbase_pair(line: str) -> tuple[str, str] | None:
    """Extract key-value pairs from BC Database event source lines."""

    if match := REGEX_PATTERNS["bc_event_source"].search(string=line):
        variable: str = _normalise_bcdbase_variable(variable=match.group("variable"))
        value: str = match.group("value").strip()
        key: str = f"bcdbase: {variable}"
        return key, value
    return None


def extract_float(match: re.Match) -> float | None:
    """
    Extracts and converts the first captured group of a regex match to a float.

    Args:
        match (re.Match): The regex match object.

    Returns:
        float | None: The extracted float if conversion is successful; otherwise, None.
    """
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            logger.warning(f"Failed to convert '{match.group(1)}' to float.")
    return None


def search_for_completion(
    line: str,
    data_dict: dict[str, str | float],
    sim_complete: int,
    current_section: str | None = None,
) -> tuple[dict[str, str | float], int, str | None]:
    """
    Parses a line to extract simulation completion status, timing information,
    and additional file path details from the log file.

    Args:
        line (str): The current line from the log file.
        data_dict (dict[str, str | float]): Dictionary to store extracted data.
        sim_complete (int): Status flag indicating simulation completion.
        current_section (str | None): Current section being parsed ('init', 'final', or None).

    Returns:
        tuple[dict[str, str | float], int, str | None]: Updated data_dict, sim_complete flag, and current_section.
    """

    # logger.debug(f"Processing line: {line.strip()}")  # Added for tracing

    if match := REGEX_PATTERNS["input_file"].match(string=line):
        full_path: str = match.group(1).strip()
        filename: str = Path(full_path).name
        data_dict["TCF"] = filename
        data_dict["orig_TCF_path"] = full_path
        logger.debug(f"Extracted TCF: {filename}")
    elif match := REGEX_PATTERNS["log_path"].match(string=line):
        orig_log_path: str = match.group(1).strip()
        data_dict["orig_log_path"] = orig_log_path
    elif match := REGEX_PATTERNS["model_end_time"].search(string=line):
        model_end_time: float | None = extract_float(match=match)
        if model_end_time is not None:
            data_dict["Model_End_Time"] = model_end_time
    elif match := REGEX_PATTERNS["model_start_time"].search(string=line):
        model_start_time: float | None = extract_float(match=match)
        if model_start_time is not None:
            data_dict["Model_Start_Time"] = model_start_time
    elif match := REGEX_PATTERNS["initialisation_times"].search(string=line):
        current_section = "init"
    elif match := REGEX_PATTERNS["final_times"].search(string=line):
        current_section = "final"
    elif match := REGEX_PATTERNS["simulation_finished"].search(string=line):
        data_dict["EndStatus"] = line.strip()
        sim_complete = 1  # Simulation completed
        # We need to have found this item.
    elif match := REGEX_PATTERNS["final_me"].search(string=line):
        final_me: float | None = extract_float(match=match)
        if final_me is not None:
            data_dict["Final_Cumulative_ME_pct"] = final_me
            if sim_complete == 1:
                sim_complete = 2  # This is the last item we grab
    elif bcdbase_result := _extract_bcdbase_pair(line=line):
        key, value = bcdbase_result
        data_dict[key] = value

    # within init/final sections capture times
    elif current_section:
        # Handle Clock Time
        if match := REGEX_PATTERNS["clock_time"].search(string=line):
            clock_time = float(match.group("time"))
            key = "Final_RunTime" if current_section == "final" else "Initialise_RunTime"
            data_dict[key] = clock_time
        # Handle Processor Time
        elif match := REGEX_PATTERNS["processor_time"].search(string=line):
            processor_time = float(match.group("time"))
            key = "Final_CPU_Time" if current_section == "final" else "Initialise_CPU_Time"
            data_dict[key] = processor_time

    return data_dict, sim_complete, current_section


def search_from_top(
    line: str,
    data_dict: dict[str, Any],
    success: int,
    spec_events: bool,
    spec_scen: bool,
    spec_var: bool,
) -> tuple[dict[str, Any], int, bool, bool, bool]:
    """Parses the top of the log file for build info, variables, file references, etc."""
    # The following ``re.match`` calls look for simple phrases like ``Build:``, ``Simulations Log Folder ==``
    # or ``Computer Name:`` so we can capture the descriptive text that follows each label.
    if match := re.match(pattern=r"Build:\s*(.*)", string=line):
        data_dict["TUFLOW_version"] = match.group(1).strip()
    elif match := re.match(pattern=r"Simulations Log Folder == .*\\([^\\]+)$", string=line):
        data_dict["username"] = match.group(1).strip()
    elif match := re.match(pattern=r"Computer Name:\s*(.*)", string=line):
        data_dict["ComputerName"] = match.group(1).strip()
        success += 1
    elif "! GPU Solver from 2016-03 Release or earlier invoked." in line:
        data_dict["Version_note"] = "! GPU Solver from 2016-03 Release or earlier invoked."
    elif match := re.match(r"Simulation Started\s*:\s*(.+)", line):
        dt_str: str = match.group(1).strip().rstrip(".")
        try:
            data_dict["StartDate"] = datetime.strptime(dt_str, "%Y-%b-%d %H:%M")
            success += 1
        except ValueError:
            logger.warning(f"Failed to parse StartDate from line: {line}")
    elif match := REGEX_PATTERNS["gpu_device_ids"].search(string=line):
        ids_str = match.group("ids").strip()
        data_dict["GPU_Device_IDs"] = ids_str
    elif bcdbase_result := _extract_bcdbase_pair(line=line):
        key, value = bcdbase_result
        data_dict[key] = value
    elif spec_events:
        if len(line.strip()) == 0:
            spec_events = False
            success += 1
        else:
            parts: list[str] = line.split(maxsplit=1)
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
    elif match := re.search(pattern=r"BC Database == .*\\([^\\]+)", string=line):
        data_dict["BC_dbase"] = match.group(1).strip()
    elif match := re.search(pattern=r"Geometry Control File == .*\\([^\\]+)", string=line):
        data_dict["TGC"] = match.group(1).strip()
    elif match := re.search(pattern=r"BC Control File == .*\\([^\\]+)", string=line):
        data_dict["TBC"] = match.group(1).strip()
    elif match := re.search(pattern=r"ESTRY Control File == .*\\([^\\.]+)", string=line):
        data_dict["ECF"] = match.group(1).strip()
    elif match := re.search(pattern=r"BC Event File == .*\\([^\\.]+)", string=line):
        data_dict["TEF"] = match.group(1).strip()
    elif match := re.search(
        pattern=r"Trying to open \(I\) file .*\\([^\\]+\.tef)\.\.\.OK\.  File Unit:",
        string=line,
    ):
        data_dict["TEF"] = match.group(1).strip()
    elif "Number of defined variables:" in line:
        spec_var = True
    elif spec_var:
        if len(line.strip()) == 0:
            spec_var = False
        else:
            m: re.Match[str] | None = SET_VARIABLE_PATTERN.match(string=line.strip())
            if m:
                key: str = m.group("var").strip()
                value: str = m.group("val").strip()
                # Exclude redundant variables
                if key in EXCLUDED_VARIABLES or re.match(pattern=r"^~[ES]\d*~$", string=key):
                    # logger.debug(f"Excluded redundant variable: {key}")
                    pass
                else:
                    data_dict[key] = value
            else:
                logger.warning(f"Unexpected variable format: {line}")
    elif "Output Files to be Pre-fixed by:" in line:
        data_dict["orig_results_path"] = line.split(sep=":", maxsplit=1)[1].strip()

    return data_dict, success, spec_events, spec_scen, spec_var


def remove_e_s_from_runcode(runcode: str, data_dict: dict[str, Any], delimiters: str = "_+") -> str:
    for delim in delimiters:
        runcode = runcode.replace(delim, "_")
    parts: list[str] = runcode.split(sep="_")

    patterns_to_remove: set[str] = {
        str(value).lower() for key, value in data_dict.items() if key.startswith("-e") or key.startswith("-s")
    }
    logger.debug(f"Patterns to remove: {patterns_to_remove}")

    filtered_parts: list[str] = [
        part for part in parts if part.lower() not in patterns_to_remove and part.strip() != ""
    ]
    cleaned_runcode: str = "_".join(filtered_parts)
    logger.debug(f"Original RunCode: {runcode}, Cleaned RunCode: {cleaned_runcode}")
    return cleaned_runcode


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
        lines: list[str] = logfile_path.read_text(encoding="utf-8").splitlines()
        return lines
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
                    result: tuple[dict[str, Any], int, bool, bool, bool] = search_from_top(
                        line=line,
                        data_dict=data_dict,
                        success=success,
                        spec_events=spec_events,
                        spec_scen=spec_scen,
                        spec_var=spec_var,
                    )
                    if result is None:
                        logger.error(f"search_from_top returned None for file: {relative_logfile_path}")
                        return data_dict, success, spec_events, spec_scen, spec_var
                    data_dict, success, spec_events, spec_scen, spec_var = result
                    if success == 4 and counter > 4000:
                        logger.debug(f"Early termination after {counter} lines for {runcode}")
                        break
        else:
            for counter, line in enumerate(lines, 1):
                result = search_from_top(
                    line=line,
                    data_dict=data_dict,
                    success=success,
                    spec_events=spec_events,
                    spec_scen=spec_scen,
                    spec_var=spec_var,
                )
                if result is None:
                    logger.error(f"search_from_top returned None for file: {relative_logfile_path}")
                    return data_dict, success, spec_events, spec_scen, spec_var
                data_dict, success, spec_events, spec_scen, spec_var = result
                if success == 4 and counter > 4000:
                    logger.debug(f"Early termination after {counter} lines for {runcode}")
                    break
        return data_dict, success, spec_events, spec_scen, spec_var
    except Exception as e:
        logger.error(f"Error processing top lines in {relative_logfile_path}: {e}")
        return data_dict, success, spec_events, spec_scen, spec_var


def finalise_data(runcode: str, data_dict: dict[str, Any]) -> pd.DataFrame:
    """
    Finalizes the data dictionary and creates a DataFrame.

    Args:
        runcode (str): Run code identifier.
        data_dict (dict[str, Any]): Dictionary containing extracted data.

    Returns:
        pd.DataFrame: DataFrame containing the processed data.
    """
    try:
        parser = TuflowStringParser(file_path=runcode)

        clean_run_code: str = parser.clean_run_code
        data_dict["Runcode"] = clean_run_code

        data_dict["trim_run_code"] = parser.trim_run_code
        data_dict["trim_tcf"] = remove_e_s_from_runcode(clean_run_code, data_dict)

        data_dict.update(parser.run_code_parts)

        data_dict["TP"] = str(parser.tp.numeric_value) if parser.tp else None
        data_dict["Duration"] = str(parser.duration.numeric_value) if parser.duration else None
        data_dict["AEP"] = str(parser.aep.numeric_value) if parser.aep else None

        df: pd.DataFrame = pd.DataFrame([data_dict])
        return df
    except Exception as e:
        logger.error(f"Error finalizing data for {runcode}: {e}")
        return pd.DataFrame()
