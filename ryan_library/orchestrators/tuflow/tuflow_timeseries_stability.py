# ryan_library/scripts/tuflow/tuflow_timeseries_stability.py
"""
Timeseries stability checks for TUFLOW outputs.

Supports PO CSV files and 1D Q CSV timeseries files.
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
    analyze_stability_q_csv,
    analyze_stability_csv,
    flatten_stability_results,
)

DEFAULT_RESULT_TYPES: tuple[str, ...] = ("PO",)
ACCEPTED_RESULT_TYPES: tuple[str, ...] = ("PO", "Q")
RESULT_TYPE_GLOBS: dict[str, str] = {
    "PO": "**/*_PO.csv",
    "Q": "**/*_1d_Q.csv",
}


def _normalize_result_types(result_types: Sequence[str] | None) -> tuple[str, ...]:
    if not result_types:
        return DEFAULT_RESULT_TYPES

    normalized: list[str] = []
    accepted_lookup: dict[str, str] = {value.lower(): value for value in ACCEPTED_RESULT_TYPES}
    for raw_value in result_types:
        value = str(raw_value).strip()
        if not value:
            continue
        if value.lower() == "all":
            for accepted in ACCEPTED_RESULT_TYPES:
                if accepted not in normalized:
                    normalized.append(accepted)
            continue
        canonical = accepted_lookup.get(value.lower())
        if canonical is None:
            logger.warning(f"Skipping unsupported result type '{value}'. Accepted values: PO, Q, all.")
            continue
        if canonical not in normalized:
            normalized.append(canonical)

    return tuple(normalized) or DEFAULT_RESULT_TYPES


def _collect_files(
    paths_to_process: Sequence[Path],
    result_types: Sequence[str],
    result_type_globs: dict[str, str],
) -> list[tuple[Path, str]]:
    files: list[tuple[Path, str]] = []
    seen: set[tuple[Path, str]] = set()
    for root in paths_to_process:
        path = Path(root)
        if not path.is_dir():
            logger.warning(f"Skipping non-directory path: {path}")
            continue
        for result_type in result_types:
            csv_glob: str = result_type_globs[result_type]
            for match in path.rglob(csv_glob):
                key: tuple[Path, str] = (match, result_type)
                if key in seen:
                    continue
                seen.add(key)
                files.append(key)
    return sorted(files)


def _analyze_stability_worker(path_str: str, result_type: str, config: StabilityCheckConfig) -> list[dict[str, object]]:
    path = Path(path_str)
    if result_type == "Q":
        results: list[StabilityCheckResult] = analyze_stability_q_csv(path=path, config=config)
    else:
        results = analyze_stability_csv(path=path, config=config)
    return flatten_stability_results(results=results)


def main_processing(
    *,
    paths_to_process: Sequence[Path],
    csv_glob: str = "**/*_PO.csv",
    result_types: Sequence[str] | None = None,
    datatype_include: Sequence[str] = ("Flow", "Q"),
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
    Run stability checks for selected TUFLOW timeseries CSV files and export a summary.

    Args:
        paths_to_process: Directories to scan for timeseries CSVs.
        csv_glob: Deprecated PO glob argument retained for compatibility.
        result_types: Result file families to process: "PO", "Q", or "all".
        datatype_include: Measurement types to include (e.g., "Flow", "Q").
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
        effective_result_types: tuple[str, ...] = _normalize_result_types(result_types=result_types)
        result_type_globs: dict[str, str] = {**RESULT_TYPE_GLOBS, "PO": csv_glob}

        files: list[tuple[Path, str]] = _collect_files(
            paths_to_process=paths_to_process,
            result_types=effective_result_types,
            result_type_globs=result_type_globs,
        )
        if not files:
            selected_patterns: list[str] = [result_type_globs[result_type] for result_type in effective_result_types]
            logger.info(f"No files matched {selected_patterns} in the provided directories.")
            return

        logger.info(
            f"Processing {len(files)} timeseries CSV file(s) for stability checks "
            f"({', '.join(effective_result_types)})."
        )
        all_rows: list[dict[str, object]] = []
        with cf.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for rows in executor.map(
                _analyze_stability_worker,
                (str(path) for path, _ in files),
                (result_type for _, result_type in files),
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
