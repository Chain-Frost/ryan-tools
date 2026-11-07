# ryan_library/orchestrators/tuflow/peak_check_po_csvs.py
"""Peak checks for TUFLOW PO timeseries CSVs.

This module scans PO CSVs, identifies peak timing relative to the end of the series,
and exports a summary table.
"""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import concurrent.futures as cf
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

from loguru import logger
from pandas import DataFrame

from ryan_library.functions.excel_export import ExcelExporter
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowStatus
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.po_timeseries_checks import (
    PeakCheckConfig,
    PeakCheckResult,
    analyze_peak_csv,
    collect_po_csv_files,
    flatten_peak_results,
    order_dataframe_columns,
)


def _analyze_peak_worker(path_str: str, config: PeakCheckConfig) -> list[dict[str, object]]:
    results: list[PeakCheckResult] = analyze_peak_csv(path=Path(path_str), config=config)
    return flatten_peak_results(results=results)


_MALFORMED_INPUT_STATUSES: frozenset[str] = frozenset(
    {"DECODE_FAIL", "CSV_PARSE_FAIL", "BAD_HEADER", "TIME_PARSE_FAIL"}
)
_NO_DATA_STATUSES: frozenset[str] = frozenset({"EMPTY_FILE", "NO_COLUMNS", "NO_DATA"})


def _file_outcome(rows: list[dict[str, object]]) -> WorkflowStatus:
    """Separate parser failures from successful QA findings and valid empty inputs."""
    if not rows:
        return "SKIP"
    statuses = {str(row.get("status", "")) for row in rows}
    if statuses & _MALFORMED_INPUT_STATUSES:
        return "FAIL"
    if statuses <= _NO_DATA_STATUSES:
        return "SKIP"
    return "OK"


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
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Run peak checks for PO timeseries CSV files and export a summary.

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

    dashboard = LiveWorkflowDashboard(
        title="TUFLOW PO Peak Checks",
        subtitle="PO CSV timeseries",
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
    )
    with setup_logger(console_log_level=console_log_level):
        with dashboard:
            try:
                dashboard.set_phase("Discovering")
                files: list[Path] = collect_po_csv_files(paths_to_process=paths_to_process, csv_glob=csv_glob)
                dashboard.set_tasks(labels=[str(file) for file in files])
                if not files:
                    logger.info("No files matched '{}' in the provided directories.", csv_glob)
                    dashboard.finish(outcome="SKIPPED", detail="no matching PO CSV files")
                    return True

                logger.info("Processing {} PO CSV file(s) for peak checks.", len(files))
                dashboard.set_phase("Processing")
                all_rows: list[dict[str, object]] = []
                status_counts: Counter[WorkflowStatus] = Counter()
                with cf.ProcessPoolExecutor(max_workers=max_workers) as executor:
                    rows_iter = iter(
                        executor.map(
                            _analyze_peak_worker,
                            (str(path) for path in files),
                            (config for _ in files),
                            chunksize=chunksize,
                        )
                    )
                    for index in range(1, len(files) + 1):
                        dashboard.mark_running(index=index)
                        rows = next(rows_iter)
                        status = _file_outcome(rows)
                        status_counts[status] += 1
                        dashboard.mark_finished(
                            index=index,
                            status=status,
                            detail=f"{len(rows)} diagnostic/result row(s)",
                        )
                        if rows:
                            all_rows.extend(rows)

                dashboard.set_phase("Aggregating")
                if not all_rows:
                    logger.info("No matching data columns after filters. Skipping export.")
                    dashboard.finish(outcome="SKIPPED", detail="no matching series")
                    return True

                out_df = DataFrame(data=all_rows)
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
                out_df: DataFrame = order_dataframe_columns(df=out_df, first_cols=first_cols)

                dashboard.set_phase("Exporting")
                ExcelExporter().save_to_excel(
                    data_frame=out_df,
                    file_name_prefix="peaks_summary",
                    sheet_name="Summary",
                    output_directory=output_dir,
                    export_mode=export_mode,
                    parquet_compression="gzip",
                    include_data_dictionary=True,
                    data_dictionary_metadata={"Workflow": "Timeseries peak checks"},
                )
                logger.info("Peak summary export complete.")

                if status_counts["FAIL"]:
                    logger.error(
                        "{} input file(s) had malformed data; diagnostic rows were retained in the report.",
                        status_counts["FAIL"],
                    )
                    dashboard.finish(
                        outcome="FAILED", detail=f"{status_counts['FAIL']} malformed file(s); partial report exported"
                    )
                    return False
                if not status_counts["OK"]:
                    dashboard.finish(outcome="SKIPPED", detail="diagnostic-only report exported")
                    return True

                dashboard.finish(outcome="SUCCESS", detail=f"{len(all_rows)} checked row(s) exported")
                return True
            except KeyboardInterrupt:
                dashboard.finish(outcome="INTERRUPTED", detail="processing interrupted")
                raise
            except Exception:
                logger.exception("PO peak-check workflow failed.")
                dashboard.finish(outcome="FAILED", detail="processing or export failed")
                raise
