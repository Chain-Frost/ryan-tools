from __future__ import annotations

from pathlib import Path
from collections.abc import Iterable
import csv
import re

import pandas as pd
from pandas import DataFrame
from loguru import logger

from ryan_library.functions.file_utils import find_files_parallel, is_non_zero_file
from ryan_library.classes.tuflow_string_classes import TuflowStringParser


def find_po_files(paths: Iterable[Path]) -> list[Path]:
    """Return list of non-empty ``*_PO.csv`` files under ``paths``."""
    roots: list[Path] = [p for p in paths if p.is_dir()]
    files: list[Path] = find_files_parallel(root_dirs=roots, patterns="*_PO.csv")
    return [f for f in files if is_non_zero_file(f)]


def parse_metadata(file_path: Path) -> dict[str, str]:
    """Extract AEP, Duration and TP strings from ``file_path``."""
    parser = TuflowStringParser(file_path=file_path)
    aep: str = parser.aep.raw_value if parser.aep else ""
    duration: str = parser.duration.raw_value if parser.duration else ""
    if not duration:
        m = re.search(r"(?:[_+]|^)(\d+(?:\.\d+)?hr)(?:[_+]|$)", file_path.name, flags=re.IGNORECASE)
        if m:
            duration = m.group(1).replace("_", "")
    tp: str = parser.tp.raw_value if parser.tp else ""
    return {"AEP": aep, "Duration": duration, "TP": tp}


def read_po_csv(
    filepath: Path,
    *,
    data_type: str = "Flow",
    allowed_locations: set[str] | None = None,
) -> DataFrame:
    """Read a TUFLOW ``*_PO.csv`` file filtered to ``data_type`` columns."""
    dtype_low = data_type.lower()
    try:
        with filepath.open() as file:
            reader = csv.reader(file)
            first_row = list(next(reader))
            second_row = list(next(reader))
    except Exception:
        logger.exception(f"Error reading header {filepath}")
        return DataFrame()

    usecols: list[int] = []
    col_names: list[str] = []
    for idx, (top, second) in enumerate(zip(first_row, second_row)):
        top_clean = top.strip().lower()
        second_clean = second.strip()
        if top_clean == "location" and second_clean == "Time":
            usecols.append(idx)
            col_names.append("Time")
        elif top_clean == dtype_low:
            if allowed_locations and second_clean not in allowed_locations:
                continue
            usecols.append(idx)
            col_names.append(second_clean)
    if not usecols or "Time" not in col_names:
        logger.error("No matching columns in %s", filepath)
        return DataFrame()
    try:
        df = pd.read_csv(filepath, skiprows=1, usecols=usecols, header=0)
        df.columns = col_names
        return df
    except Exception:
        logger.exception(f"Error reading {filepath}")
        return DataFrame()


def analyze_po_file(
    csv_path: Path,
    *,
    thresholds: list[float],
    data_type: str = "Flow",
    allowed_locations: set[str] | None = None,
) -> DataFrame:
    """Return durations exceeding ``thresholds`` for a single PO CSV file."""
    df: DataFrame = read_po_csv(filepath=csv_path, data_type=data_type, allowed_locations=allowed_locations)
    if df.empty or "Time" not in df.columns or len(df["Time"]) < 2:
        return DataFrame()
    timestep: float = float(df["Time"].iloc[1] - df["Time"].iloc[0])
    meta = parse_metadata(file_path=csv_path)
    records: list[dict[str, float | str]] = []
    for thresh in thresholds:
        counts = (df.iloc[:, 1:] > thresh).sum()
        for loc_obj, count in counts[counts > 0].items():
            loc: str = str(loc_obj)
            records.append(
                {
                    "AEP": meta["AEP"],
                    "Duration": meta["Duration"],
                    "TP": meta["TP"],
                    "Location": loc,
                    "ThresholdFlow": thresh,
                    "Duration_Exceeding": float(count) * timestep,
                    "out_path": str(csv_path.parent),
                }
            )
    return DataFrame(records)
