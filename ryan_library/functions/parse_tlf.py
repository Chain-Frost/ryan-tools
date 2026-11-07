# ryan_library/functions/parse_tlf.py

__lazy_modules__: list[str] = ["pandas"]

import math
import re
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any

import pandas as pd
from loguru import logger

from ..classes.tuflow_string_classes import TuflowStringParser

# Match whole signed decimal/exponent tokens; float() alone accepts non-finite values.
TLF_FLOAT_TOKEN: re.Pattern[str] = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?")

# Precompile regex patterns at the module level for efficiency and thread safety
REGEX_PATTERNS: dict[str, re.Pattern[str]] = {
    # Looks for the literal heading ``Initialisation Times`` anywhere in the log.
    "initialisation_times": re.compile(r"Initialisation Times"),
    # Modern logs use ``Final Times``; 2016-era logs use ``SIMULATION SUMMARY``.
    "final_times": re.compile(r"(?:Final Times|SIMULATION SUMMARY)"),
    # Preserve an entire percentage token and validate it before float conversion.
    "final_me": re.compile(r"Final Cumulative ME:\s*(?P<value>\S+)\s*%"),
    # Flags the line ``Simulation FINISHED`` to indicate a successful run completion.
    "simulation_finished": re.compile(r"Simulation FINISHED"),
    # Capture the whole bracketed token; validation rejects partial and non-finite numbers.
    "clock_time": re.compile(r"Clock Time:.*\[\s*(?P<time>\S+?)\s*h\]"),
    # Modern logs use Processor Time; 2016-era logs use CPU Time.
    "processor_time": re.compile(r"(?:Processor|CPU) Time:.*\[\s*(?P<time>\S+?)\s*h\]"),
    "model_end_time": re.compile(r"End Time \(h\):\s*(?P<value>\S+)"),
    "model_start_time": re.compile(r"Start Time \(h\):\s*(?P<value>\S+)"),
    # Captures the full path to the ``.tcf`` file reported as ``Input File: path/to/model.tcf``.
    "input_file": re.compile(r"Input File:\s*(.+\.tcf)"),
    # Captures any characters following ``Log File:`` so the log file path can be recorded.
    "log_path": re.compile(r"Log File:\s*(.+)"),
    # Collects comma-separated GPU identifiers, e.g. ``GPU Device IDs == 0, 1``.
    "gpu_device_ids": re.compile(r"GPU Device IDs\s*==\s*(?P<ids>[\d,\s]+)"),
    # The path before the explanatory comment may end at ``log`` for legacy global folders or contain a username.
    "simulations_log_folder": re.compile(r"^Simulations Log Folder\s*==\s*(?P<path>.*?)(?:\s+!\s+|$)"),
    # In 2016 GPU logs, the unlabelled timing pair immediately before this line is the initialisation duration.
    "legacy_solver_start": re.compile(r"^Writing GPU Output at:\s*0:00:00\b"),
    # Extracts the variable/value pair described on ``BC Event Source == variable | value`` lines, ignoring case.
    # Example: ``BC Event Source == ~E1~ | rainfall.tsf``.
    "bc_event_source": re.compile(
        r"BC Event Source\s*==\s*(?P<variable>[^|]+?)\s*\|\s*(?P<value>.+)$",
        flags=re.IGNORECASE,
    ),
}

CONTROL_FILE_PATTERN: re.Pattern[str] = re.compile(
    r"^\s*(?P<label>BC Database|Geometry Control File|BC Control File|ESTRY Control File|BC Event File)"
    r"\s*==\s*(?P<path>.+?)\s*$",
    flags=re.IGNORECASE,
)
TEF_OPEN_PATTERN: re.Pattern[str] = re.compile(
    r"Trying to open \(I\) file\s+(?P<path>.+?\.tef)\.{3}OK\.\s+File Unit:",
    flags=re.IGNORECASE,
)
CONTROL_FILE_FIELDS: dict[str, str] = {
    "bc database": "BC_dbase",
    "geometry control file": "TGC",
    "bc control file": "TBC",
    "estry control file": "ECF",
    "bc event file": "TEF",
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
    pattern=r"^Set Variable\s+(?P<var>~[ES]\d*~|\w+(?:-\w+)*)\s*==\s*(?P<val>.+)$",
    flags=re.IGNORECASE,
)
# ``SET_VARIABLE_PATTERN`` recognises configuration lines such as ``Set Variable ~E1~ == inflow.csv``
# and stores the variable name (``~E1~``) plus the assigned value (``inflow.csv``).
# User variable names may contain hyphen-separated words such as ``IL-PEBBLE``.


def _extract_control_file(line: str) -> tuple[str, str] | None:
    """Extract a control-file basename (or the ECF/TEF stem) across path styles."""
    match = CONTROL_FILE_PATTERN.match(line)
    if match:
        field = CONTROL_FILE_FIELDS[match.group("label").casefold()]
        raw_path = match.group("path")
    else:
        match = TEF_OPEN_PATTERN.search(line)
        if not match:
            return None
        field = "TEF"
        raw_path = match.group("path")

    # Deliberate TUFLOW explanatory/redirection suffixes, not spaces or dots in filenames.
    path_text = re.split(r"\s+!\s+|\s+->\s+|\s+=>\s+|\s+\|\s+", raw_path, maxsplit=1)[0].strip().strip('"')
    # Existing TUFLOW ECF logs may append ...redirecting directly to the
    # control-file extension. Only remove this explicit suffix, not valid dots.
    path_text = re.sub(r"(\.(?:ecf|tef|tgc|tbc|csv))\.{3}redirecting\b.*$", r"\1", path_text, flags=re.IGNORECASE)
    filename = PureWindowsPath(path_text.replace("/", "\\")).name
    if not filename:
        return None
    return field, PureWindowsPath(filename).stem if field in {"ECF", "TEF"} else filename


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


def _uses_legacy_tuflow_format(data_dict: dict[str, Any]) -> bool:
    """Return whether the parsed build identifies a TUFLOW release from 2016 or earlier."""
    version: Any = data_dict.get("TUFLOW_version")
    if not isinstance(version, str):
        return False
    match: re.Match[str] | None = re.match(pattern=r"(?P<year>\d{4})-", string=version)
    return bool(match and int(match.group("year")) <= 2016)


def _extract_log_username(line: str) -> str | None:
    """Extract a username only when it is a child folder of the TUFLOW log directory."""
    match: re.Match[str] | None = REGEX_PATTERNS["simulations_log_folder"].match(string=line)
    if not match:
        return None

    folder_text: str = match.group("path").strip().strip('"').rstrip("\\/")
    parts: tuple[str, ...] = PureWindowsPath(folder_text).parts
    for index, part in enumerate(parts):
        if part.casefold() == "log" and index + 1 < len(parts):
            return parts[index + 1]
    return None


def _capture_legacy_initialisation_times(
    line: str,
    data_dict: dict[str, Any],
    candidate_cpu_time: float | None,
    candidate_clock_time: float | None,
) -> tuple[float | None, float | None]:
    """Capture the unlabelled pre-solver timing pair used by 2016-era GPU logs."""
    if not _uses_legacy_tuflow_format(data_dict=data_dict) or "Initialise_RunTime" in data_dict:
        return candidate_cpu_time, candidate_clock_time

    if match := REGEX_PATTERNS["processor_time"].search(string=line):
        candidate_cpu_time = _parse_finite_float(match.group("time"), label="CPU Time")
    elif match := REGEX_PATTERNS["clock_time"].search(string=line):
        candidate_clock_time = _parse_finite_float(match.group("time"), label="Clock Time")
    elif REGEX_PATTERNS["legacy_solver_start"].search(string=line):
        if candidate_cpu_time is not None:
            data_dict["Initialise_CPU_Time"] = candidate_cpu_time
        if candidate_clock_time is not None:
            data_dict["Initialise_RunTime"] = candidate_clock_time

    return candidate_cpu_time, candidate_clock_time


def _parse_finite_float(value: str, *, label: str) -> float | None:
    """Accept only complete finite TLF numeric tokens, including signed exponents."""
    if not TLF_FLOAT_TOKEN.fullmatch(value):
        logger.warning("Invalid {} numeric token: {}", label, value)
        return None
    number = float(value)
    if not math.isfinite(number):
        logger.warning("Non-finite {} numeric token: {}", label, value)
        return None
    return number


def extract_float(match: re.Match[str]) -> float | None:
    """Convert the first captured TLF numeric token without accepting a partial value."""
    return _parse_finite_float(match.group(1), label="TLF")


def search_for_completion(
    line: str,
    data_dict: dict[str, str | float],
    sim_complete: int,
    current_section: str | None = None,
) -> tuple[dict[str, str | float], int, str | None]:
    """Parses a line to extract simulation completion status, timing information,
    and additional file path details from the log file.

    Args:
        line (str): The current line from the log file.
        data_dict (dict[str, str | float]): Dictionary to store extracted data.
        sim_complete (int): Status flag indicating simulation completion.
        current_section (str | None): Current section being parsed ('init', 'final', or None).

    Returns:
        tuple[dict[str, str | float], int, str | None]: Updated data_dict, sim_complete flag, and current_section.
    """
    if match := REGEX_PATTERNS["input_file"].match(string=line):
        full_path: str = match.group(1).strip()
        normalized_path = Path(full_path.replace("\\", "/"))
        filename: str = normalized_path.name
        data_dict["TCF"] = filename
        data_dict["orig_TCF_path"] = full_path
        logger.debug("Extracted TCF: {}", filename)
    elif match := REGEX_PATTERNS["log_path"].match(string=line):
        orig_log_path: str = match.group(1).strip()
        data_dict["orig_log_path"] = orig_log_path
    elif match := REGEX_PATTERNS["model_end_time"].search(string=line):
        model_end_time = _parse_finite_float(match.group("value"), label="End Time (h)")
        if model_end_time is not None:
            data_dict["Model_End_Time"] = model_end_time
    elif match := REGEX_PATTERNS["model_start_time"].search(string=line):
        model_start_time = _parse_finite_float(match.group("value"), label="Start Time (h)")
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
        final_me = _parse_finite_float(match.group("value"), label="Final Cumulative ME (%)")
        if final_me is not None:
            data_dict["Final_Cumulative_ME_pct"] = final_me
            if sim_complete == 1:
                sim_complete = 2  # This is the last item we grab
    elif "Final Cumulative ME:" in line:
        logger.warning("Invalid Final Cumulative ME line: {}", line.strip())
    elif bcdbase_result := _extract_bcdbase_pair(line=line):
        key, value = bcdbase_result
        data_dict[key] = value

    # within init/final sections capture times
    elif current_section:
        # Handle Clock Time
        if match := REGEX_PATTERNS["clock_time"].search(string=line):
            clock_time = _parse_finite_float(match.group("time"), label="Clock Time")
            if clock_time is not None:
                key = "Final_RunTime" if current_section == "final" else "Initialise_RunTime"
                data_dict[key] = clock_time
        # Handle Processor Time
        elif match := REGEX_PATTERNS["processor_time"].search(string=line):
            processor_time = _parse_finite_float(match.group("time"), label="Processor Time")
            if processor_time is not None:
                key = "Final_CPU_Time" if current_section == "final" else "Initialise_CPU_Time"
                data_dict[key] = processor_time

    return data_dict, sim_complete, current_section


def is_complete_tlf(data_dict: dict[str, Any], sim_complete: int) -> bool:
    """Return True when a TLF has enough footer evidence to treat it as complete."""
    if sim_complete == 2:
        return True
    return sim_complete == 1 and "Final_RunTime" in data_dict


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
    elif username := _extract_log_username(line=line):
        data_dict["username"] = username
    elif match := re.match(pattern=r"Computer Name:\s*(.*)", string=line):
        data_dict["ComputerName"] = match.group(1).strip()
        success += 1
    elif "! GPU Solver from 2016-03 Release or earlier invoked." in line:
        data_dict["Version_note"] = "! GPU Solver from 2016-03 Release or earlier invoked."
    elif match := re.match(r"Simulation Started\s*:\s*(.+)", line):
        dt_str: str = match.group(1).strip().rstrip(".")
        # TUFLOW releases use month abbreviations, with optional seconds; numeric ISO dates also occur.
        for date_format in ("%Y-%b-%d %H:%M", "%Y-%b-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                data_dict["StartDate"] = datetime.strptime(dt_str, date_format)
                success += 1
                break
            except ValueError:
                continue
        else:
            logger.warning("Failed to parse StartDate from line: {}", line)
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
                logger.warning("Unexpected event format: {}", line)
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
                logger.warning("Unexpected scenario format: {}", line)
    elif "Specified Scenarios:" in line:
        spec_scen = True
    elif "No Specified Scenarios." in line or "No Specified Events." in line:
        success += 1
    elif control_file := _extract_control_file(line):
        field, name = control_file
        data_dict[field] = name
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
                if key not in EXCLUDED_VARIABLES and not re.match(pattern=r"^~[ES]\d*~$", string=key):
                    data_dict[key] = value
            else:
                logger.warning("Unexpected variable format: {}", line)
    elif "Output Files to be Pre-fixed by:" in line:
        data_dict["orig_results_path"] = line.split(sep=":", maxsplit=1)[1].strip()

    return data_dict, success, spec_events, spec_scen, spec_var


def remove_e_s_from_runcode(runcode: str, data_dict: dict[str, Any], delimiters: str = "_+") -> str:
    for delim in delimiters:
        runcode = runcode.replace(delim, "_")
    parts: list[str] = runcode.split(sep="_")

    patterns_to_remove: set[str] = {
        str(value).lower() for key, value in data_dict.items() if key.startswith(("-e", "-s"))
    }
    logger.debug("Patterns to remove: {}", patterns_to_remove)

    filtered_parts: list[str] = [
        part for part in parts if part.lower() not in patterns_to_remove and part.strip() != ""
    ]
    cleaned_runcode: str = "_".join(filtered_parts)
    logger.debug("Original RunCode: {}, Cleaned RunCode: {}", runcode, cleaned_runcode)
    return cleaned_runcode


def _decode_tlf_line(raw_line: bytes) -> str:
    """Decode each complete TLF line as UTF-8 or legacy Windows-1252.

    Line-wise fallback avoids whole-file buffering and handles mixed-encoding logs
    consistently on both sides of the 10 MiB threshold.
    """
    try:
        return raw_line.decode("utf-8")
    except UnicodeDecodeError:
        return raw_line.decode("cp1252", errors="replace")


def get_log_lines(logfile_path: Path, is_large_file: bool) -> tuple[list[str], list[str]]:
    """Reads the log file efficiently based on its size.

    Args:
        logfile_path (Path): Path to the log file.
        is_large_file (bool): Flag indicating if the file is large.

    Returns:
        tuple[list[str], list[str]]: A tuple of (all_lines, last_100_lines).
        For large files, all_lines is empty to save memory.
    """
    try:
        if is_large_file:
            # Read only a bounded tail over the network; skip an initial partial line
            # so UTF-8 multibyte sequences cannot begin in the middle of a codepoint.
            chunk_size = 30000
            with logfile_path.open("rb") as stream:
                stream.seek(0, 2)
                file_size = stream.tell()
                start = max(0, file_size - chunk_size)
                stream.seek(start)
                tail_data = stream.read()
            if start:
                first_newline = tail_data.find(b"\n")
                if first_newline != -1:
                    tail_data = tail_data[first_newline + 1 :]
            tail_lines = [_decode_tlf_line(raw) for raw in tail_data.splitlines()]
            return [], tail_lines[-100:]

        lines = [_decode_tlf_line(raw) for raw in logfile_path.read_bytes().splitlines()]
        return lines, lines[-100:]
    except Exception as e:
        logger.error("Error reading {}: {}", logfile_path, e)
        return [], []


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
    """Processes the top lines of the log file to extract relevant data.

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
    legacy_cpu_time: float | None = None
    legacy_clock_time: float | None = None
    try:
        if is_large_file:
            # Binary streaming plus per-line decode preserves CP1252/UTF-8 metadata
            # without loading the full >10 MiB log into memory.
            with logfile_path.open("rb") as lfile:
                for counter, raw_line in enumerate(lfile, 1):
                    line = _decode_tlf_line(raw_line)
                    result: tuple[dict[str, Any], int, bool, bool, bool] = search_from_top(
                        line=line,
                        data_dict=data_dict,
                        success=success,
                        spec_events=spec_events,
                        spec_scen=spec_scen,
                        spec_var=spec_var,
                    )
                    data_dict, success, spec_events, spec_scen, spec_var = result
                    legacy_cpu_time, legacy_clock_time = _capture_legacy_initialisation_times(
                        line=line,
                        data_dict=data_dict,
                        candidate_cpu_time=legacy_cpu_time,
                        candidate_clock_time=legacy_clock_time,
                    )
                    if success == 4 and counter > 4000:
                        logger.debug("Early termination after {} lines for {}", counter, runcode)
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
                data_dict, success, spec_events, spec_scen, spec_var = result
                legacy_cpu_time, legacy_clock_time = _capture_legacy_initialisation_times(
                    line=line,
                    data_dict=data_dict,
                    candidate_cpu_time=legacy_cpu_time,
                    candidate_clock_time=legacy_clock_time,
                )
                if success == 4 and counter > 4000:
                    logger.debug("Early termination after {} lines for {}", counter, runcode)
                    break
        return data_dict, success, spec_events, spec_scen, spec_var
    except Exception as e:
        logger.error("Error processing top lines in {}: {}", relative_logfile_path, e)
        return data_dict, success, spec_events, spec_scen, spec_var


def finalise_data(runcode: str, data_dict: dict[str, Any], logfile_path: Path | str | None = None) -> pd.DataFrame:
    """Finalizes the data dictionary and creates a DataFrame.

    Args:
        runcode (str): Run code identifier.
        data_dict (dict[str, Any]): Dictionary containing extracted data.
        logfile_path (Path | str | None): Optional hint of the original log file path.

    Returns:
        pd.DataFrame: DataFrame containing the processed data.
    """
    try:
        parser_source: Path | str = logfile_path if logfile_path is not None else runcode
        parser_path = Path(parser_source)
        if not parser_path.suffix:
            parser_path: Path = parser_path.with_suffix(".tlf")
        parser = TuflowStringParser(file_path=parser_path)

        clean_run_code: str = parser.clean_run_code
        data_dict["Runcode"] = clean_run_code

        if "TCF" in data_dict:
            data_dict["TCF"] = Path(str(data_dict["TCF"])).name
        data_dict["trim_run_code"] = parser.trim_run_code
        data_dict["trim_tcf"] = remove_e_s_from_runcode(clean_run_code, data_dict)

        data_dict.update(parser.run_code_parts)

        data_dict["TP"] = str(parser.tp.numeric_value) if parser.tp else None
        data_dict["Duration"] = str(parser.duration.numeric_value) if parser.duration else None
        data_dict["AEP"] = str(parser.aep.numeric_value) if parser.aep else None

        df: pd.DataFrame = pd.DataFrame([data_dict])
        return df
    except Exception as e:
        logger.error("Error finalizing data for {}: {}", runcode, e)
        return pd.DataFrame()
