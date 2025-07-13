import logging
from pathlib import Path
from collections.abc import Iterable

import pandas as pd


logger = logging.getLogger(__name__)


def find_batch_files(paths: Iterable[Path]) -> list[Path]:
    """Return list of ``batch.out`` files found recursively under ``paths``."""
    files: list[Path] = []
    for root in paths:
        files.extend([p for p in root.rglob("*batch.out") if p.is_file()])
    return files


def _parse_run_line(line: str, batchout_file: Path) -> list[float | int | str] | None:
    raw = line.strip().split()
    if len(raw) < 8:
        logger.warning("Invalid run line skipped: %s", line.strip())
        return None

    try:
        raw[3] = raw[3].strip("%")
        duration_part = raw[1] + raw[2]
        aep_part = f"aep{raw[3]}"
        raw[6] = "1" if raw[6].upper() == "Y" else "0"
        if raw[2].lower() != "hour":
            raw[1] = str(float(raw[1]) / 60)
        raw.pop(2)
        processed_line: list[float | int | str] = []
        for i, el in enumerate(raw):
            processed_line.append(int(el) if i in (0, 3) else float(el))
        csv_path = _construct_csv_path(batchout_file, aep_part, duration_part, int(processed_line[3]))
        processed_line.append(str(csv_path))
        return processed_line
    except Exception as exc:  # pragma: no cover - parsing errors are logged
        logger.exception("Error parsing run line: %s", line)
        return None


def _construct_csv_path(batchout: Path, aep_part: str, duration_part: str, tpat: int) -> Path:
    aep = aep_part.replace(".", "p")
    du = duration_part.replace(".", "_")
    base_name = batchout.name.replace("batch.out", "")
    second_part = f" {aep}_du{du}tp{tpat}.csv"
    return batchout.parent / (base_name + second_part)


def parse_batch_output(batchout_file: Path) -> pd.DataFrame:
    """Return DataFrame describing runs defined in ``batchout_file``."""
    basename = batchout_file.name
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
                        headers = line.strip().split()
                        headers.append("csv")
                    else:
                        run = _parse_run_line(line, batchout_file)
                        if run:
                            rorb_runs.append(run)
        if not rorb_runs or not headers:
            return pd.DataFrame()
        df = pd.DataFrame(rorb_runs, columns=headers)
        df["file"] = basename
        df["folder"] = str(batchout_file.parent)
        df["Path"] = str(batchout_file)
        return df
    except Exception as exc:  # pragma: no cover - logs handle detail
        logger.exception("Failed parsing %s", batchout_file)
        return pd.DataFrame()


def read_hydrograph_csv(filepath: Path) -> pd.DataFrame:
    """Return hydrograph DataFrame from RORB CSV."""
    try:
        df = pd.read_csv(filepath, sep=",", skiprows=2, header=0)
        df.drop(df.columns[0], axis=1, inplace=True)
        return df
    except Exception as exc:  # pragma: no cover - file errors
        logger.exception("Error reading hydrograph %s", filepath)
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
    df = read_hydrograph_csv(csv_path)
    if df.empty:
        return pd.DataFrame()

    df.columns = [c.replace("Calculated hydrograph:  ", "") for c in df.columns]
    if "Time (hrs)" not in df.columns or len(df["Time (hrs)"]) < 2:
        logger.error("Missing 'Time (hrs)' in %s", csv_path)
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
