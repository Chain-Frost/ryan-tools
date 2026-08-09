# ryan_library/orchestrators/tuflow/tlf_missing_runs.py
"""Orchestrator for checking missing TUFLOW runs."""

__lazy_modules__: list[str] = ["pandas"]
import pandas as pd
from pathlib import Path
from loguru import logger

from ryan_library.functions.tlf_missing_runs import summarize_for_cli


def orchestrate_missing_runs_check(input_path: Path, sheet_name: str | int = 0) -> Path:
    """
    Read run-tracking data, check for missing sets, log a summary, and export a CSV.

    Args:
        input_path: Path to the tracking table (CSV or Excel).
        sheet_name: If Excel, which sheet to read from.

    Returns:
        The Path to the generated CSV summary.
    """
    input_str = str(input_path)
    if input_str.lower().endswith((".xlsx", ".xls")):
        df: pd.DataFrame = pd.read_excel(
            io=input_path, sheet_name=sheet_name
        )  # pyright: ignore[reportUnknownMemberType]
    else:
        df = pd.read_csv(filepath_or_buffer=input_path)

    text, table = summarize_for_cli(df)

    # Log the summary text
    for line in text.splitlines():
        if line.strip():
            logger.info(line)
        else:
            logger.info("")

    base: str = input_str.rsplit(sep=".", maxsplit=1)[0]
    out_csv = Path(f"{base}__missing_runs_summary.csv")
    table.to_csv(path_or_buf=out_csv, index=False)

    logger.success("Wrote missing runs summary to: {}", out_csv)
    return out_csv
