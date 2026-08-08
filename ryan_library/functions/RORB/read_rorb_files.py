"""Read RORB batch summaries and calculate hydrograph exceedance durations."""

from collections.abc import Iterable
from pathlib import Path
import re

from loguru import logger
import pandas as pd
from pandas import DataFrame

_NUMBER_PATTERN = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?"


def find_batch_files(paths: Iterable[Path]) -> list[Path]:
    """Return ``batch.out`` files found recursively under ``paths``."""

    files: list[Path] = []
    for root in paths:
        files.extend(path for path in root.rglob("*batch.out") if path.is_file())
    return sorted(files)


def _numeric_prefix(value: str) -> float | str:
    """Return the leading RORB numeric value, or the original text."""

    match: re.Match[str] | None = re.match(
        pattern=_NUMBER_PATTERN,
        string=value,
    )
    return float(match.group()) if match else value


def _parse_run_line(line: str, batchout_file: Path) -> list[float | int | str] | None:
    """Parse one run-table row from a RORB ``batch.out`` file."""

    raw: list[str] = line.split()
    if len(raw) < 8:
        logger.warning("Invalid RORB run row skipped: {}", line.strip())
        return None

    try:
        # raw[3] = raw[3].strip("%")
        # unit_value: str = raw[2]
        # duration_part: str = raw[1] + unit_value
        # aep_part: str = f"aep{raw[3]}"
        # raw[6] = "1" if raw[6].upper() == "Y" else "0"
        # if unit_value.lower() != "hour":
        #     raw[1] = str(float(raw[1]) / 60)
        #     unit_value = "hour"
        # raw.pop(2)
        # tp_value: int | None = None
        # processed_line: list[float | int | str] = []
        # for i, el in enumerate(iterable=raw):
        #     if i in (0, 3):
        #         # run-number and TP should be ints
        #         val = int(el)
        #         processed_line.append(val)
        #         if i == 3:
        #             tp_value = val
        #     elif i == 2:
        #         # AEP comes in as something like '0.2EY'—keep it as a string
        #         processed_line.append(el)
        #     else:
        #         # everything else should be numeric; strip any trailing letters
        #         # The pattern matches an optional sign, decimal number, and optional scientific exponent.
        #         # Example matches include ``-12.5`` and ``3.1E-03``.
        #         m: re.Match[str] | None = re.match(
        #             pattern=r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?",
        #             string=el,
        #         )
        #         if m:
        #             processed_line.append(float(m.group()))
        #         else:
        #             # fallback to raw in case it really isn’t numeric
        #             processed_line.append(el)

        #     # A few notes:
        #     # Index 0 is your “Run” column (int),
        #     # Index 2 is the AEP label (e.g. "0.2EY") that you probably want to preserve in your
        #     # file-naming logic, Index 3 (after the pop(2)) is TP, which stays an int, and all
        #     # other columns you now attempt to parse to float, but first use a regex to pull off a
        #     # clean numeric prefix. With that change you’ll never try to do float("0.2EY") again,
        #     # so the ValueError goes away and you keep the original “EY” suffix in your CSV-naming
        #     # logic.

        # tp_value = tp_value if tp_value is not None else int(processed_line[3])
        run = int(raw[0])
        duration_value = float(raw[1])
        duration_unit = raw[2]
        duration_part = f"{raw[1]}{duration_unit}"
        aep = raw[3].removesuffix("%")
        tp = int(raw[4])
        # RORB uses Y/N in some older run tables. Preserve the numeric output
        # shape expected by the corresponding header.
        remaining_values: list[float | int | str] = [
            1 if value.upper() == "Y" else 0 if value.upper() == "N" else _numeric_prefix(value) for value in raw[5:]
        ]

        if duration_unit.casefold() != "hour":
            duration_value /= 60.0

        csv_path: Path = _construct_csv_path(
            batchout=batchout_file,
            aep_part=f"aep{aep}",
            duration_part=duration_part,
            tpat=tp,
        )
        return [run, duration_value, aep, tp, *remaining_values, str(csv_path)]
    except IndexError, TypeError, ValueError:
        logger.exception("Error parsing RORB run row: {}", line.rstrip())
        return None


def _construct_csv_path(batchout: Path, aep_part: str, duration_part: str, tpat: int) -> Path:
    """Construct the hydrograph CSV path referenced by a batch run row."""

    aep: str = aep_part.replace(".", "p")
    duration: str = duration_part.replace(".", "_")
    base_name: str = batchout.name.removesuffix("batch.out")
    return batchout.parent / f"{base_name} {aep}_du{duration}tp{tpat}.csv"


def parse_batch_output(batchout_file: Path) -> pd.DataFrame:
    """Return the run table from ``batchout_file``.

    Durations are reported in hours. AEP labels such as ``0.2EY`` are retained
    as text so their meaning and associated hydrograph filenames are preserved.
    """

    runs: list[list[float | int | str]] = []
    headers: list[str] = []
    reading_runs = False
    parameters = {"IL": 0.0, "CL": 0.0, "m": 0.0, "kc": 0.0, "ROC": 1.0}
    pending_loss_parameter: str | None = None

    try:
        with batchout_file.open(encoding="utf-8", errors="replace") as batch_stream:
            for line in batch_stream:
                if pending_loss_parameter is not None and line.strip():
                    loss_values = re.findall(_NUMBER_PATTERN, line)
                    if len(loss_values) >= 2:
                        parameters["IL"] = float(loss_values[0])
                        parameters[pending_loss_parameter] = float(loss_values[1])
                    else:
                        logger.warning("Invalid RORB loss parameters in {}: {}", batchout_file, line.strip())
                    pending_loss_parameter = None
                    continue
                if "Parameters:" in line:
                    kc_match = re.search(rf"\bkc\s*=\s*({_NUMBER_PATTERN})", line)
                    m_match = re.search(rf"\bm\s*=\s*({_NUMBER_PATTERN})", line)
                    if kc_match is not None:
                        parameters["kc"] = float(kc_match.group(1))
                    if m_match is not None:
                        parameters["m"] = float(m_match.group(1))
                    continue
                if "Loss parameters" in line:
                    pending_loss_parameter = "CL" if "Cont" in line else "ROC"
                    continue
                if "Run,    Representative hydrograph" in line:
                    break
                if " Run        Duration" in line:
                    headers = [header for header in line.split() if header != "Unit"]
                    headers.append("csv")
                    reading_runs = True
                    continue
                if not reading_runs or not line.strip():
                    continue

                run = _parse_run_line(line=line, batchout_file=batchout_file)
                if run is not None:
                    runs.append(run)
    except OSError:
        logger.exception("Failed reading RORB batch output {}", batchout_file)
        return pd.DataFrame()

    if not runs or not headers:
        logger.warning("No RORB run table found in {}", batchout_file)
        return pd.DataFrame()
    if any(len(run) != len(headers) for run in runs):
        logger.error("RORB run-table columns do not match the header in {}", batchout_file)
        return pd.DataFrame()

    result = pd.DataFrame(data=runs, columns=headers)
    for name, value in parameters.items():
        result[name] = value
    result["file"] = batchout_file.name
    result["folder"] = str(batchout_file.parent)
    try:
        result["Path"] = batchout_file.relative_to(Path.cwd()).as_posix()
    except ValueError:
        result["Path"] = str(batchout_file)
    return result


def read_hydrograph_csv(filepath: Path) -> pd.DataFrame:
    """Return a normalized hydrograph table from a RORB CSV.

    The RORB sample counter (``Inc``) is metadata rather than a hydrograph and
    is removed when present. Column whitespace is also normalized.
    """

    try:
        result: DataFrame = pd.read_csv(filepath_or_buffer=filepath, skiprows=2)
    except OSError, pd.errors.ParserError, UnicodeError:
        logger.exception("Error reading RORB hydrograph {}", filepath)
        return pd.DataFrame()

    result.columns = [str(column).strip() for column in result.columns]
    if "Inc" in result.columns:
        result = result.drop(columns="Inc")
    return result


def _location_name(column: str) -> str:
    """Return a concise, non-empty location label from a RORB header."""

    prefix = "Calculated hydrograph:"
    label = column.strip()
    if label.startswith(prefix):
        label = label.removeprefix(prefix).strip()
        return label or "Calculated hydrograph"
    return label


def analyze_hydrograph(
    aep: str,
    duration: str,
    tp: int,
    csv_path: Path,
    out_path: Path,
    thresholds: list[float],
) -> pd.DataFrame:
    """Return durations exceeding ``thresholds`` for one hydrograph file.

    One row is emitted for every location and threshold, including a duration
    of zero when the threshold is never exceeded. This retains the full
    temporal-pattern population needed by the summary statistics.
    """

    hydrograph: DataFrame = read_hydrograph_csv(filepath=csv_path)
    if hydrograph.empty:
        return pd.DataFrame()
    if "Time (hrs)" not in hydrograph.columns:
        logger.error("Missing 'Time (hrs)' in {}", csv_path)
        return pd.DataFrame()

    time_values = pd.to_numeric(hydrograph["Time (hrs)"], errors="coerce")
    time_steps = time_values.diff().dropna()
    if len(time_values.index) < 2 or time_steps.empty or time_steps.isna().any() or (time_steps <= 0).any():
        logger.error("Invalid time values in {}", csv_path)
        return pd.DataFrame()
    timestep = float(time_steps.iloc[0])
    if not (time_steps - timestep).abs().le(1e-9).all():
        logger.error("Non-uniform time steps in {}", csv_path)
        return pd.DataFrame()

    hydrograph_columns = [column for column in hydrograph.columns if column != "Time (hrs)"]
    if not hydrograph_columns:
        logger.error("No hydrograph columns found in {}", csv_path)
        return pd.DataFrame()

    records: list[dict[str, float | str | int]] = []
    for column in hydrograph_columns:
        flows = pd.to_numeric(hydrograph[column], errors="coerce")
        if flows.notna().sum() != len(flows.index):
            logger.error("Non-numeric hydrograph values in column {!r} of {}", column, csv_path)
            continue
        location = _location_name(column)
        for threshold in thresholds:
            exceedance_duration = float((flows > threshold).sum()) * timestep
            records.append(
                {
                    "AEP": aep,
                    "Duration": duration,
                    "TP": tp,
                    "Location": location,
                    "ThresholdFlow": float(threshold),
                    "Duration_Exceeding": exceedance_duration,
                    "out_path": str(out_path),
                }
            )
    return pd.DataFrame.from_records(records)
