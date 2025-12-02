from loguru import logger
from pathlib import Path
from collections.abc import Iterable
import re

import pandas as pd
from pandas import DataFrame


def find_batch_files(paths: Iterable[Path]) -> list[Path]:
    """Return list of ``batch.out`` files found recursively under ``paths``."""
    files: list[Path] = []
    for root in paths:
        files.extend([p for p in root.rglob("*batch.out") if p.is_file()])
    return files


def _parse_run_line(line: str, batchout_file: Path) -> list[float | int | str] | None:
    raw: list[str] = line.strip().split()
    if len(raw) < 8:
        logger.warning("Invalid run line skipped: {}", line.strip())
        return None

    try:
        raw[3] = raw[3].strip("%")
        unit_value: str = raw[2]
        duration_part: str = raw[1] + unit_value
        aep_part: str = f"aep{raw[3]}"
        raw[6] = "1" if raw[6].upper() == "Y" else "0"
        if unit_value.lower() != "hour":
            raw[1] = str(float(raw[1]) / 60)
            unit_value = "hour"
        raw.pop(2)
        tp_value: int | None = None
        processed_line: list[float | int | str] = []
        for i, el in enumerate(iterable=raw):
            if i in (0, 3):
                # run-number and TP should be ints
                val = int(el)
                processed_line.append(val)
                if i == 3:
                    tp_value = val
            elif i == 2:
                # AEP comes in as something like '0.2EY'—keep it as a string
                processed_line.append(el)
            else:
                # everything else should be numeric; strip any trailing letters
                # The pattern matches an optional sign, decimal number, and optional scientific exponent.
                # Example matches include ``-12.5`` and ``3.1E-03``.
                m: re.Match[str] | None = re.match(
                    pattern=r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?",
                    string=el,
                )
                if m:
                    processed_line.append(float(m.group()))
                else:
                    # fallback to raw in case it really isn’t numeric
                    processed_line.append(el)

            # A few notes:
            # Index 0 is your “Run” column (int),
            # Index 2 is the AEP label (e.g. "0.2EY") that you probably want to preserve in your
            # file-naming logic, Index 3 (after the pop(2)) is TP, which stays an int, and all
            # other columns you now attempt to parse to float, but first use a regex to pull off a
            # clean numeric prefix. With that change you’ll never try to do float("0.2EY") again,
            # so the ValueError goes away and you keep the original “EY” suffix in your CSV-naming
            # logic.

        tp_value = tp_value if tp_value is not None else int(processed_line[3])
        csv_path: Path = _construct_csv_path(
            batchout=batchout_file, aep_part=aep_part, duration_part=duration_part, tpat=tp_value
        )
        processed_line.append(str(csv_path))
        return processed_line
    except Exception as exc:  # pragma: no cover - parsing errors are logged
        logger.exception("Error parsing run line: {}", line)
        return None


def _construct_csv_path(batchout: Path, aep_part: str, duration_part: str, tpat: int) -> Path:
    aep: str = aep_part.replace(".", "p")
    du: str = duration_part.replace(".", "_")
    base_name: str = batchout.name.replace("batch.out", "")
    second_part: str = f" {aep}_du{du}tp{tpat}.csv"
    return batchout.parent / (base_name + second_part)


def parse_batch_output(batchout_file: Path) -> pd.DataFrame:
    """Return DataFrame describing runs defined in ``batchout_file``."""
    basename: str = batchout_file.name
    rorb_runs: list[list[float | int | str]] = []
    headers: list[str] = []
    try:
        with batchout_file.open("r") as f:
            found_results = 0
            for line in f:
                if found_results == 20:
                    continue
                if found_results == 0:
                    if "Peak  Description" in line:
                        found_results = 1
                elif found_results == 1:
                    if "Run,    Representative hydrograph" in line:
                        found_results = 20
                    elif " Run        Duration" in line:
                        headers = [h for h in line.strip().split() if h != "Unit"]
                        headers.append("csv")
                    else:
                        run: list[float | int | str] | None = _parse_run_line(line=line, batchout_file=batchout_file)
                        if run:
                            rorb_runs.append(run)
        if not rorb_runs or not headers:
            return pd.DataFrame()
        df = pd.DataFrame(data=rorb_runs, columns=headers)
        df["file"] = basename
        df["folder"] = str(object=batchout_file.parent)
        # df["Path"] = str(object=batchout_file)
        cwd: Path = Path.cwd()
        try:
            df["Path"] = batchout_file.relative_to(cwd).as_posix()
        except ValueError:
            # if the file lives outside cwd, fall back to just its name:
            df["Path"] = batchout_file.name
        # for item in rorb_runs:
        #     print(item)
        return df
    except Exception as exc:  # pragma: no cover - logs handle detail
        logger.exception("Failed parsing {}", batchout_file)
        return pd.DataFrame()


def read_hydrograph_csv(filepath: Path) -> pd.DataFrame:
    """Return hydrograph DataFrame from RORB CSV."""
    try:
        df: DataFrame = pd.read_csv(filepath_or_buffer=filepath, sep=",", skiprows=2, header=0)
        return df
    except Exception as exc:  # pragma: no cover - file errors
        logger.exception("Error reading hydrograph {}", filepath)
        return pd.DataFrame()


def analyze_hydrograph(
    aep: str,
    duration: str,
    tp: int,
    csv_path: Path,
    out_path: Path,
    thresholds: list[float],
) -> pd.DataFrame:
    """Return durations exceeding ``thresholds`` for a single hydrograph file."""
    df: DataFrame = read_hydrograph_csv(filepath=csv_path)
    if df.empty:
        return pd.DataFrame()

    df.columns = [c.replace("Calculated hydrograph:  ", "") for c in df.columns]
    if "Time (hrs)" not in df.columns or len(df["Time (hrs)"]) < 2:
        logger.error("Missing 'Time (hrs)' in {}", csv_path)
        return pd.DataFrame()
    timestep = df["Time (hrs)"].iloc[1] - df["Time (hrs)"].iloc[0]

    records: list[dict[str, float | str | int]] = []
    for thresh in thresholds:
        counts = (df.iloc[:, 1:] > thresh).sum()
        locations = counts[counts > 0].index.tolist()
        dur_exc = (counts[counts > 0] * timestep).tolist()
        for loc, dur_exc_val in zip(locations, dur_exc):
            records.append(
                {
                    "AEP": aep,
                    "Duration": duration,
                    "TP": tp,
                    "Location": loc,
                    "ThresholdFlow": thresh,
                    "Duration_Exceeding": dur_exc_val,
                    "out_path": str(out_path),
                }
            )
    return pd.DataFrame(records)
