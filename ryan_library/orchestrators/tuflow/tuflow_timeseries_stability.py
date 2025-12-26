# ryan_library/orchestrators/tuflow/tuflow_timeseries_stability.py
"""
Timeseries stability checks for TUFLOW outputs.

Currently this supports PO CSV files only; 1D Q support can be added later.
"""

from __future__ import annotations

import concurrent.futures as cf
from pathlib import Path
from collections.abc import Sequence
from typing import Literal

import pandas as pd
from loguru import logger
from pandas import DataFrame

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.tuflow.po_timeseries_checks import (
    StabilityCheckConfig,
    StabilityCheckResult,
    analyze_stability_csv,
    flatten_stability_results,
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


def _analyze_stability_worker(path_str: str, config: StabilityCheckConfig) -> list[dict[str, object]]:
    results: list[StabilityCheckResult] = analyze_stability_csv(path=Path(path_str), config=config)
    return flatten_stability_results(results=results)


def main_processing(
    *,
    paths_to_process: Sequence[Path],
    csv_glob: str = "**/*_PO.csv",
    datatype_include: Sequence[str] = ("Flow",),
    datatype_case_sensitive: bool = False,
    location_include: Sequence[str] = (),
    location_exclude: Sequence[str] = (),
    location_case_sensitive: bool = False,
    flat_tol: float = 1e-6,
    diff_rel_tol: float = 0.01,
    diff_abs_tol: float = 1e-6,
    max_sign_changes: int = 2,
    min_points: int = 5,
    max_workers: int | None = None,
    chunksize: int = 1,
    console_log_level: str = "INFO",
    output_dir: Path | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """
    Run stability checks for PO timeseries CSV files and export a summary.

    Args:
        paths_to_process: Directories to scan for PO CSVs.
        csv_glob: Glob pattern to match PO CSV files.
        datatype_include: Measurement types to include (e.g., "Flow").
        datatype_case_sensitive: Whether datatype filtering is case-sensitive.
        location_include: Optional location allow-list.
        location_exclude: Optional location block-list.
        location_case_sensitive: Whether location filtering is case-sensitive.
        flat_tol: Range threshold for considering a series flat.
        diff_rel_tol: Relative tolerance (of range) for ignoring step noise.
        diff_abs_tol: Absolute tolerance for ignoring step noise.
        max_sign_changes: Maximum sign changes allowed before flagging unstable.
        min_points: Minimum points required for stability evaluation.
        max_workers: Process pool size for parallel analysis.
        chunksize: Task chunk size for the process pool.
        console_log_level: Loguru console log level.
        output_dir: Optional output directory for exports.
        export_mode: "excel", "parquet", or "both".
    """
    config = StabilityCheckConfig(
        datatype_include=datatype_include,
        datatype_case_sensitive=datatype_case_sensitive,
        location_include=location_include,
        location_exclude=location_exclude,
        location_case_sensitive=location_case_sensitive,
        flat_tol=flat_tol,
        diff_rel_tol=diff_rel_tol,
        diff_abs_tol=diff_abs_tol,
        max_sign_changes=max_sign_changes,
        min_points=min_points,
    )

    with setup_logger(console_log_level=console_log_level):
        files: list[Path] = _collect_files(paths_to_process=paths_to_process, csv_glob=csv_glob)
        if not files:
            logger.info(f"No files matched '{csv_glob}' in the provided directories.")
            return

        logger.info(f"Processing {len(files)} PO CSV file(s) for stability checks.")
        all_rows: list[dict[str, object]] = []
        with cf.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for rows in executor.map(
                _analyze_stability_worker,
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
            "points",
            "sign_changes",
            "nonzero_steps",
            "value_range",
            "delta_tol",
            "value_min",
            "value_max",
            "start_value",
            "end_value",
            "start_time",
            "end_time",
            "time_span",
            "max_abs_step",
            "mean_abs_step",
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
        out_df: DataFrame = out_df.reindex(columns=ordered)

        ExcelExporter().save_to_excel(
            data_frame=out_df,
            file_name_prefix="timeseries_stability",
            sheet_name="Summary",
            output_directory=output_dir,
            export_mode=export_mode,
            parquet_compression="gzip",
        )
        logger.info("Timeseries stability export complete.")
