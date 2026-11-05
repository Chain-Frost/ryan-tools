# ryan_library/functions/RORB/read_rorb_files.py
"""Read RORB batch summaries and calculate hydrograph exceedance durations."""

__lazy_modules__ = ["pandas"]

from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from loguru import logger
from pandas import DataFrame

from .associations import associate_batch_csvs
from .readers import read_rorb_file


def find_batch_files(paths: Iterable[Path]) -> list[Path]:
    """Return ``batch.out`` files found recursively under ``paths``."""
    files: list[Path] = []
    for root in paths:
        files.extend(path for path in root.rglob("*batch.out") if path.is_file())
    return sorted(files)


def parse_batch_output(batchout_file: Path) -> pd.DataFrame:
    """Return strictly validated runs with corroborated CSV paths.

    Invalid tables raise ValueError instead of silently returning a prefix.
    Missing or ambiguous associations remain null; downstream workflows must
    reject them. Absent and inapplicable parameters remain null, never zero.
    """
    document = read_rorb_file(batchout_file)
    if document.kind != "batch_output" or document.status == "invalid":
        msg = f"Invalid or unsupported RORB batch output: {batchout_file}; {document.diagnostics}"
        raise ValueError(msg)
    runs = document.section("runs")
    # The established workflow processes the full table; only MCP applies the fixed 500-run ceiling.
    associations = associate_batch_csvs(document, max_runs=len(runs.rows))
    result = pd.DataFrame.from_records(runs.rows)
    result["csv"] = [row["path"] for row in associations.rows]
    result["csv_status"] = [row["status"] for row in associations.rows]
    parameters = document.section("parameters")
    for name in ("IL", "CL", "m", "kc", "ROC"):
        values = {row[name] for row in parameters.rows if name in row}
        if len(values) > 1:
            msg = f"Multiple {name} parameter values require scoped interpretation"
            raise ValueError(msg)
        result[name] = next(iter(values)) if values else None
    result["file"] = batchout_file.name
    result["folder"] = str(batchout_file.parent)
    result["Path"] = str(batchout_file.absolute())
    result.attrs["rorb_status"] = document.status
    result.attrs["diagnostics"] = [d.message for d in document.diagnostics]
    return result


def read_hydrograph_csv(filepath: Path) -> pd.DataFrame:
    """Return a validated RORB hydrograph table, excluding Inc for existing callers.

    The richer read_rorb_file API retains Inc, provenance and diagnostics.
    Malformed data raises ValueError rather than producing partial results.
    """
    document = read_rorb_file(filepath)
    if document.kind != "hydrograph_csv" or document.status == "invalid":
        msg = f"Invalid or unsupported RORB hydrograph: {filepath}; {document.diagnostics}"
        raise ValueError(msg)
    table = document.section("hydrographs")
    return pd.DataFrame.from_records(table.rows).drop(columns=["line", "Inc"])


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
