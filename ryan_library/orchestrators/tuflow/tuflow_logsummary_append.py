# ryan_library/orchestrators/tuflow/tuflow_logsummary_append.py
"""Append completed TUFLOW log-summary rows to an existing master workbook."""

__lazy_modules__ = ["pandas"]

from pathlib import Path

import pandas as pd
from loguru import logger

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.logsummary_excel_append import (
    append_dataframe_to_workbook_table,
    filter_new_log_files,
    load_existing_log_summary_rows,
)
from ryan_library.orchestrators.tuflow.tuflow_logsummary import (
    LOG_SUMMARY_DASHBOARD_COLUMNS,
    LogFileProcessingResult,
    build_log_summary_dataframe,
    discover_log_files,
    process_log_files,
)


def append_to_master_log_summary(
    *,
    master_workbook_path: Path,
    console_log_level: str | None = None,
    sheet_name: str = "Log Summary",
    table_name: str | None = None,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Append newly completed logs, returning False for a partial/failed batch.

    Existing run codes and log paths are skipped before parsing. A batch with
    no new logs, or only incomplete (SKIP) logs, is a non-failure skip.
    If any new log fails or a report includes only some completed logs, the
    completed rows may be appended, but the overall result is FAILED.
    Write and infrastructure exceptions propagate to the wrapper.
    """
    if not sheet_name.strip() or (table_name is not None and not table_name.strip()):
        msg = "Sheet and table names must be non-empty when provided."
        raise ValueError(msg)
    workbook_path = master_workbook_path.expanduser().absolute()
    if workbook_path.suffix.lower() != ".xlsx":
        msg = "Master log append supports .xlsx only; macro-enabled workbooks are not modified."
        raise ValueError(msg)
    if not workbook_path.is_file():
        msg = f"Master log workbook does not exist: {workbook_path}"
        raise FileNotFoundError(msg)

    root_dir = Path.cwd()
    dashboard = LiveWorkflowDashboard(
        title="TUFLOW Master Log Append",
        subtitle=str(workbook_path),
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
        columns=LOG_SUMMARY_DASHBOARD_COLUMNS,
    )
    console_level = (
        console_log_level if console_log_level is not None else ("SUCCESS" if use_live_dashboard else "INFO")
    )

    with setup_logger(console_log_level=console_level) as log_queue:
        with dashboard:
            try:
                dashboard.set_phase("Discovering")
                logger.info("Appending TUFLOW log summary rows into {}", workbook_path)
                logger.info("Searching for TUFLOW logs under {}", root_dir)
                existing_rows = load_existing_log_summary_rows(
                    workbook_path=workbook_path,
                    sheet_name=sheet_name,
                    table_name=table_name,
                )
                files: list[Path] = discover_log_files(root_dir=root_dir)
                files_to_process: list[Path] = filter_new_log_files(files=files, existing_rows=existing_rows)
                duplicate_count = len(files) - len(files_to_process)
                logger.info("Found {} log(s), {} already in master.", len(files), duplicate_count)

                if not files_to_process:
                    logger.success("No new log files found to append.")
                    dashboard.finish(outcome="SKIPPED", detail="no new log files")
                    return True

                dashboard.set_phase("Processing")
                processing_results: list[LogFileProcessingResult] = process_log_files(
                    files=files_to_process,
                    root_dir=root_dir,
                    use_live_dashboard=use_live_dashboard,
                    live_refresh_per_second=live_refresh_per_second,
                    live_max_rows=live_max_rows,
                    log_queue=log_queue,
                    console_log_level=console_level,
                    dashboard=dashboard,
                )
                completed, failed_count, skipped_count = _summarise_processing_outcomes(
                    files=files_to_process, processing_results=processing_results
                )
                dashboard.set_phase("Aggregating")
                merged_df: pd.DataFrame = (
                    build_log_summary_dataframe(files=files_to_process, processing_results=completed)
                    if completed
                    else pd.DataFrame()
                )

                if merged_df.empty:
                    if completed or failed_count:
                        logger.error(
                            "No rows appended: {} failed log(s), {} incomplete log(s), {} empty aggregation(s).",
                            failed_count,
                            skipped_count,
                            len(completed),
                        )
                        dashboard.finish(outcome="FAILED", detail="failed logs or empty aggregation")
                        return False
                    logger.warning("{} new log(s) were incomplete; nothing appended.", skipped_count)
                    dashboard.finish(outcome="SKIPPED", detail="no completed new logs")
                    return True

                dashboard.set_phase("Saving")
                append_dataframe_to_workbook_table(
                    data_frame=merged_df,
                    workbook_path=workbook_path,
                    sheet_name=sheet_name,
                    table_name=table_name,
                )
                if failed_count or skipped_count:
                    logger.error(
                        "Partial master append: {} row(s) added; {} failed and {} incomplete log(s).",
                        len(merged_df),
                        failed_count,
                        skipped_count,
                    )
                    dashboard.finish(outcome="FAILED", detail="partial append: some logs were not included")
                    return False

                logger.success("Appended {} row(s) to {}", len(merged_df), workbook_path)
                dashboard.finish(outcome="SUCCESS", detail=f"{len(merged_df)} row(s) appended")
                return True
            except KeyboardInterrupt:
                dashboard.finish(outcome="INTERRUPTED", detail="master append was interrupted")
                raise
            except Exception:
                logger.exception("Master log append failed; confirm workbook and output state.")
                dashboard.finish(outcome="FAILED", detail="processing or workbook save failed")
                raise

def _summarise_processing_outcomes(
    *, files: list[Path], processing_results: list[LogFileProcessingResult]
) -> tuple[list[LogFileProcessingResult], int, int]:
    """Do not lose failed or missing worker outcomes during aggregation."""
    if len(processing_results) != len(files):
        msg = f"Only {len(processing_results)} of {len(files)} log processing outcomes returned."
        raise RuntimeError(msg)
    if any(result.status not in {"OK", "SKIP", "FAIL"} for result in processing_results):
        msg = "Log processing returned an unsupported status."
        raise RuntimeError(msg)
    completed = [result for result in processing_results if result.status == "OK"]
    failed_count = sum(result.status == "FAIL" for result in processing_results)
    skipped_count = sum(result.status == "SKIP" for result in processing_results)
    return completed, failed_count, skipped_count
