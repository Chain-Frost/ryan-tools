# ryan_library/orchestrators/tuflow/peak_check_po_csvs.py
"""
Peak checks for TUFLOW PO timeseries CSVs.

This module scans PO CSVs, identifies peak timing relative to the end of the series,
and exports a summary table.
"""

from __future__ import annotations

import concurrent.futures as cf
from pathlib import Path
from collections.abc import Sequence
from typing import Literal

import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.tuflow.po_timeseries_checks import (
    PeakCheckConfig,
    analyze_peak_csv,
    flatten_peak_results,
)


def _collect_files(paths_to_process: Sequence[Path], csv_glob: str) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for root in paths_to_process:
        path = Path(root)
        if not path.is_dir():
            logger.warning(f"Skipping non-directory path: {path}")
            continue
        for match in path.rglob(csv_glob):
            if match in seen:
                continue
            seen.add(match)
            files.append(match)
    return sorted(files)


def _analyze_peak_worker(path_str: str, config: PeakCheckConfig) -> list[dict[str, object]]:
    results = analyze_peak_csv(path=Path(path_str), config=config)
    return flatten_peak_results(results=results)


def main_processing(
    *,
    paths_to_process: Sequence[Path],
    csv_glob: str = "**/*_PO.csv",
    datatype_include: Sequence[str] = ("Flow",),
    datatype_case_sensitive: bool = False,
    location_include: Sequence[str] = (),
    location_exclude: Sequence[str] = (),
    location_case_sensitive: bool = False,
    warn_2hours: float = 2.0,
    warn_1hour: float = 1.0,
    flat_tol: float = 1e-6,
    max_workers: int | None = None,
    chunksize: int = 1,
    console_log_level: str = "INFO",
    output_dir: Path | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """
    Run peak checks for PO timeseries CSV files and export a summary.

    Args:
        paths_to_process: Directories to scan for PO CSVs.
        csv_glob: Glob pattern to match PO CSV files.
        datatype_include: Measurement types to include (e.g., "Flow").
        datatype_case_sensitive: Whether datatype filtering is case-sensitive.
        location_include: Optional location allow-list.
        location_exclude: Optional location block-list.
        location_case_sensitive: Whether location filtering is case-sensitive.
        warn_2hours: Threshold (hours) for WARN_2H.
        warn_1hour: Threshold (hours) for WARN_1H.
        flat_tol: Tolerance for treating peak deviations as flat.
        max_workers: Process pool size for parallel analysis.
        chunksize: Task chunk size for the process pool.
        console_log_level: Loguru console log level.
        output_dir: Optional output directory for exports.
        export_mode: "excel", "parquet", or "both".
    """
    config = PeakCheckConfig(
        datatype_include=datatype_include,
        datatype_case_sensitive=datatype_case_sensitive,
        location_include=location_include,
        location_exclude=location_exclude,
        location_case_sensitive=location_case_sensitive,
        warn_2hours=warn_2hours,
        warn_1hour=warn_1hour,
        flat_tol=flat_tol,
    )

    with setup_logger(console_log_level=console_log_level):
        files = _collect_files(paths_to_process=paths_to_process, csv_glob=csv_glob)
        if not files:
            logger.info(f"No files matched '{csv_glob}' in the provided directories.")
            return

        logger.info(f"Processing {len(files)} PO CSV file(s) for peak checks.")
        all_rows: list[dict[str, object]] = []
        with cf.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for rows in executor.map(
                _analyze_peak_worker,
                (str(path) for path in files),
                (config for _ in files),
                chunksize=chunksize,
            ):
                if rows:
                    all_rows.extend(rows)

        if not all_rows:
            logger.info("No matching data columns after filters. Skipping export.")
            return

        out_df = pd.DataFrame(data=all_rows)
        first_cols: list[str] = [
            "run_code",
            "status",
            "datatype",
            "location",
            "peak_kind",
            "peak_value",
            "peak_time",
            "end_time",
            "hours_from_end",
            "start_value",
            "end_value",
            "end_minus_start",
            "peak_above_start",
            "end_pct_of_peak",
            "raw_run_code",
            "trim_run_code",
            "data_type",
            "TP",
            "TP_num",
            "Duration",
            "Duration_m",
            "AEP",
            "AEP_value",
            "file",
        ]
        ordered: list[str] = [c for c in first_cols if c in out_df.columns] + [
            c for c in out_df.columns if c not in first_cols
        ]
        out_df = out_df.reindex(columns=ordered)

        ExcelExporter().save_to_excel(
            data_frame=out_df,
            file_name_prefix="peaks_summary",
            sheet_name="Summary",
            output_directory=output_dir,
            export_mode=export_mode,
            parquet_compression="gzip",
        )
        logger.info("Peak summary export complete.")
