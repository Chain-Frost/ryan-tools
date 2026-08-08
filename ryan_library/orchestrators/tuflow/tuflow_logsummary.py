# ryan_library/scripts/tuflow/tuflow_logsummary.py
"""
TUFLOW Log Summary.

This module parses TUFLOW log files (*.tlf) in parallel to extract simulation metadata and key performance metrics
(timestamps, errors, warnings, durations).
It aggregates this information into a summary Excel report.
"""

from pathlib import Path
from dataclasses import dataclass
from typing import Literal
import pandas as pd
from loguru import logger
from ryan_library.functions.dashboard_workflow import run_dashboard_workflow
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import ExcelExporter, calculate_pool_size
from ryan_library.functions.path_stuff import convert_to_relative_path
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowColumn, WorkflowStatus
from ryan_library.functions.loguru_helpers import LogQueue, setup_logger
from ryan_library.functions.parse_tlf import (
    search_for_completion,
    get_log_lines,
    process_top_lines,
    finalise_data,
    is_complete_tlf,
)
from ryan_library.functions.dataframe_helpers import (
    merge_and_sort_data,
    reorder_columns,
)

LogSummaryStatus = Literal["OK", "SKIP", "FAIL"]

LOG_SUMMARY_DASHBOARD_COLUMNS: tuple[WorkflowColumn, ...] = (
    WorkflowColumn(header="State", source="status", no_wrap=True),
    WorkflowColumn(header="Size", source="metadata", metadata_key="size", justify="right", no_wrap=True),
    WorkflowColumn(header="Duration", source="duration", no_wrap=True),
    WorkflowColumn(header="Log file", source="label", no_wrap=True, overflow="ellipsis"),
)

LOG_SUMMARY_PRIORITIZED_COLUMNS: tuple[str, ...] = (
    "Runcode",
    "clean_run_code",
    "trim_run_code",
    "trim_tcf",
    "StartDate",
)
LOG_SUMMARY_PREFIX_ORDER: tuple[str, ...] = ("-e", "-s")
LOG_SUMMARY_SECOND_PRIORITY_COLUMNS: tuple[str, ...] = (
    "Initialise_RunTime",
    "Final_RunTime",
    "Model_Start_Time",
    "Model_End_Time",
    "TGC",
    "TBC",
    "ECF",
    "TEF",
    "BC_dbase",
    "TCF",
    "TUFLOW_version",
    "ComputerName",
    "username",
    "EndStatus",
    "AEP",
    "Duration",
    "TP",
)
LOG_SUMMARY_COLUMNS_TO_END: tuple[str, ...] = ("orig_TCF_path", "orig_log_path", "orig_results_path")


@dataclass(slots=True)
class LogFileProcessingResult:
    """Result payload returned by multiprocessing workers for dashboard progress."""

    logfile: Path
    data_frame: pd.DataFrame
    status: LogSummaryStatus
    detail: str


def process_log_file(logfile: Path) -> pd.DataFrame:
    """
    Processes a single log file and returns a DataFrame with the extracted data.

    This function attempts to:
      1. Read the log file.
      2. Check for simulation completion status (by looking at the end of the file/last 100 lines).
      3. If complete, parse the header lines using `process_top_lines` to extract variables and settings.
      4. Finalize the data dict into a single-row DataFrame.

    Args:
        logfile (Path): Path object to the log file to process.

    Returns:
        pd.DataFrame: DataFrame containing the processed data, or an empty DataFrame on failure/incomplete run.
    """
    return _process_log_file_dataframe(logfile=logfile)


def process_log_file_for_dashboard(logfile: Path) -> LogFileProcessingResult:
    """Process one log file and return enough metadata to update a live dashboard."""
    try:
        data_frame: pd.DataFrame = _process_log_file_dataframe(logfile=logfile)
    except Exception as exc:
        logger.exception("Unhandled error while processing {}", logfile)
        return LogFileProcessingResult(
            logfile=logfile,
            data_frame=pd.DataFrame(),
            status="FAIL",
            detail=str(exc),
        )

    if data_frame.empty:
        return LogFileProcessingResult(
            logfile=logfile,
            data_frame=data_frame,
            status="SKIP",
            detail="no completed run data",
        )

    return LogFileProcessingResult(
        logfile=logfile,
        data_frame=data_frame,
        status="OK",
        detail=f"{len(data_frame)} row(s)",
    )


def _process_log_file_dataframe(logfile: Path) -> pd.DataFrame:
    logfile_path: Path = logfile
    sim_complete: int = 0
    success: int = 0
    spec_events: bool = False
    spec_scen: bool = False
    spec_var: bool = False
    data_dict: dict[str, str | float] = {}
    current_section = None

    file_size: int = logfile_path.stat().st_size
    is_large_file: bool = file_size > 10 * 1024 * 1024  # 10 MB

    lines, last_lines = get_log_lines(
        logfile_path=logfile_path,
        is_large_file=is_large_file,
    )
    # large file currently does nothing - can be a problem if there are lots of errors logged in the file

    # lines_reversed = list(reversed(lines))

    if not lines and not last_lines:
        return pd.DataFrame()

    runcode: str = logfile_path.stem
    relative_logfile_path: Path = convert_to_relative_path(user_path=logfile_path)
    logger.info("Processing {} : {}", runcode, relative_logfile_path)

    logger.debug("search_for_completion: {}", runcode)
    for line in last_lines:
        data_dict, sim_complete, current_section = search_for_completion(
            line=line,
            data_dict=data_dict,
            sim_complete=sim_complete,
            current_section=current_section,
        )
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break
    logger.debug("search_for_completion: {}", data_dict)

    if is_complete_tlf(data_dict=data_dict, sim_complete=sim_complete):
        data_dict, success, spec_events, spec_scen, spec_var = process_top_lines(
            logfile_path=logfile_path,
            lines=lines,  # if not is_large_file else [],
            data_dict=data_dict,
            success=success,
            spec_events=spec_events,
            spec_scen=spec_scen,
            spec_var=spec_var,
            is_large_file=is_large_file,
            runcode=runcode,
            relative_logfile_path=relative_logfile_path,
        )
        logger.debug("process_top_lines: {}", data_dict)

        if success == 4:
            df: pd.DataFrame = finalise_data(
                runcode=runcode,
                data_dict=data_dict,
                logfile_path=logfile_path,
            )
            if not df.empty:
                logger.debug(df.head())
                return df
            else:
                logger.warning("Finalization failed for {}, skipping", runcode)
                return pd.DataFrame()
        else:
            logger.warning("{} ({}) did not complete, skipping", runcode, success)
            return pd.DataFrame()
    else:
        logger.warning("{} did not complete, skipping", runcode)
        return pd.DataFrame()


def main_processing(
    console_log_level: str | None = None,
    *,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
) -> None:
    """
    Main function to process log files using multiprocessing.

    Finds all *.tlf files in the current working directory (excluding hpc/gpu logs recursively),
    distributes processing across a process pool, and aggregates the results into an Excel report.
    """
    # log_dir = Path.home() / "Documents" / "MyAppLogs"
    # log_file = "tuflow_logsummary.log"

    processing_results: list[LogFileProcessingResult] = []
    successful_runs: int = 0
    if not console_log_level:
        console_log_level = "INFO"
    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting log file processing...")
        logger.info("Built and tested with modern TUFLOW logs; older completed logs use compatibility parsing.")

        root_dir: Path = Path.cwd()
        files: list[Path] = discover_log_files(root_dir=root_dir)

        logger.info(f"Found {len(files)} log files.")

        if not files:
            logger.warning("No log files found to process.")
        else:
            processing_results.extend(
                process_log_files(
                    files=files,
                    root_dir=root_dir,
                    use_live_dashboard=use_live_dashboard,
                    live_refresh_per_second=live_refresh_per_second,
                    live_max_rows=live_max_rows,
                    log_queue=log_queue,
                    console_log_level=console_log_level,
                )
            )

        successful_runs = _count_successful_results(processing_results=processing_results)
        merged_df: pd.DataFrame = build_log_summary_dataframe(
            files=files,
            processing_results=processing_results,
        )
        if not merged_df.empty:
            try:
                ExcelExporter().save_to_excel(
                    data_frame=merged_df,
                    file_name_prefix="ModellingLog",
                    sheet_name="Log Summary",
                    include_data_dictionary=True,
                    data_dictionary_metadata={"Workflow": "TUFLOW log summary"},
                )

                logger.success("Log file processing completed successfully.")
            except Exception:
                logger.exception("Error during merging/saving DataFrames")
        else:
            logger.warning("No completed logs found - no output generated.")

        logger.success(f"Number of successful runs: {successful_runs}")


def discover_log_files(*, root_dir: Path) -> list[Path]:
    return list(
        find_files_parallel(
            root_dirs=[root_dir],
            patterns="*.tlf",
            excludes=["*.hpc.tlf", "*.gpu.tlf"],
        )
    )


def process_log_files(
    *,
    files: list[Path],
    root_dir: Path,
    use_live_dashboard: bool,
    live_refresh_per_second: float,
    live_max_rows: int,
    log_queue: LogQueue | None,
    console_log_level: str,
) -> list[LogFileProcessingResult]:
    pool_size = calculate_pool_size(num_files=len(files))
    logger.info(f"Processing {len(files)} files using {pool_size} processes.")

    dashboard = LiveWorkflowDashboard(
        title="TUFLOW Log Summary",
        subtitle=str(root_dir),
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
        columns=LOG_SUMMARY_DASHBOARD_COLUMNS,
    )
    dashboard.set_tasks(
        labels=[_format_dashboard_label(logfile=file) for file in files],
        metadata=[{"size": _format_bytes(file.stat().st_size)} for file in files],
    )
    dashboard.set_extra_metrics(metrics={"workers": pool_size})

    # LogSummary owns TLF parsing and result shaping; the shared helper
    # owns serial/process-pool execution and dashboard state updates.
    processing_results: list[LogFileProcessingResult] = run_dashboard_workflow(
        items=files,
        process_item=process_log_file_for_dashboard,
        dashboard=dashboard,
        pool_size=pool_size,
        status_for_result=_dashboard_status,
        detail_for_result=_dashboard_detail,
        log_queue=log_queue,
        worker_log_level="ERROR" if use_live_dashboard else console_log_level,
        max_start_events=max(pool_size * 2, live_max_rows),
    )
    _log_processing_results(processing_results=processing_results)
    return processing_results


def build_log_summary_dataframe(
    *,
    files: list[Path],
    processing_results: list[LogFileProcessingResult],
) -> pd.DataFrame:
    if files:
        file_indexes = {file: index for index, file in enumerate(files, start=1)}
        processing_results.sort(key=lambda result: file_indexes[result.logfile])

    results: list[pd.DataFrame] = [result.data_frame for result in processing_results if not result.data_frame.empty]
    if not results:
        return pd.DataFrame()

    merged_df: pd.DataFrame = merge_and_sort_data(frames=results, sort_column="StartDate")
    return _reorder_log_summary_columns(data_frame=merged_df)


def _reorder_log_summary_columns(*, data_frame: pd.DataFrame) -> pd.DataFrame:
    return reorder_columns(
        data_frame=data_frame,
        prioritized_columns=list(LOG_SUMMARY_PRIORITIZED_COLUMNS),
        prefix_order=list(LOG_SUMMARY_PREFIX_ORDER),
        second_priority_columns=list(LOG_SUMMARY_SECOND_PRIORITY_COLUMNS),
        columns_to_end=list(LOG_SUMMARY_COLUMNS_TO_END),
    )


def _count_successful_results(*, processing_results: list[LogFileProcessingResult]) -> int:
    return sum(1 for result in processing_results if not result.data_frame.empty)


def _format_dashboard_label(*, logfile: Path) -> str:
    try:
        return str(logfile.relative_to(Path.cwd()))
    except ValueError:
        return str(logfile)


def _log_processing_results(*, processing_results: list[LogFileProcessingResult]) -> None:
    for result in processing_results:
        relative_logfile_path: str = _format_dashboard_label(logfile=result.logfile)
        if result.status == "OK":
            logger.info(f"Completed {relative_logfile_path}")
        elif result.status == "SKIP":
            logger.warning(f"Skipped {relative_logfile_path}: {result.detail}")
        else:
            logger.error(f"Failed {relative_logfile_path}: {result.detail}")


def _dashboard_status(result: LogFileProcessingResult) -> WorkflowStatus:
    return result.status


def _dashboard_detail(result: LogFileProcessingResult) -> str:
    return result.detail


def _format_bytes(size_bytes: int) -> str:
    value: float = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
        value /= 1024
    return f"{size_bytes} B"


if __name__ == "__main__":
    main_processing(console_log_level="DEBUG")
