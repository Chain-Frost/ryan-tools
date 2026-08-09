"""Append TUFLOW log-summary rows to an existing master workbook."""
__lazy_modules__ = ['pandas']

from pathlib import Path

import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.logsummary_excel_append import (
    append_dataframe_to_workbook_table,
    filter_new_log_files,
    load_existing_log_summary_rows,
)
from ryan_library.orchestrators.tuflow.tuflow_logsummary import (
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
    live_max_rows: int = 25,
) -> None:
    """
    Append newly completed TUFLOW log rows to an existing master log-summary workbook.

    The current working directory is searched recursively for ``*.tlf`` files. Files whose parsed
    run code or log path is already present in the workbook table are skipped before the expensive
    full log parsing step.
    """
    if not console_log_level:
        console_log_level = "INFO"

    with setup_logger(console_log_level=console_log_level) as log_queue:
        root_dir: Path = Path.cwd()
        workbook_path: Path = master_workbook_path.expanduser().absolute()
        logger.info(f"Appending TUFLOW log summary rows into {workbook_path}")
        logger.info(f"Searching for TUFLOW logs under {root_dir}")

        existing_rows = load_existing_log_summary_rows(
            workbook_path=workbook_path,
            sheet_name=sheet_name,
            table_name=table_name,
        )
        files: list[Path] = discover_log_files(root_dir=root_dir)
        logger.info(f"Found {len(files)} log files before duplicate filtering.")

        files_to_process: list[Path] = filter_new_log_files(
            files=files,
            existing_rows=existing_rows,
        )
        skipped_count: int = len(files) - len(files_to_process)
        logger.info(f"Skipping {skipped_count} log files already present in the master workbook.")

        if not files_to_process:
            logger.success("No new log files found to append.")
            return

        processing_results: list[LogFileProcessingResult] = process_log_files(
            files=files_to_process,
            root_dir=root_dir,
            use_live_dashboard=use_live_dashboard,
            live_refresh_per_second=live_refresh_per_second,
            live_max_rows=live_max_rows,
            log_queue=log_queue,
            console_log_level=console_log_level,
        )
        merged_df: pd.DataFrame = build_log_summary_dataframe(
            files=files_to_process,
            processing_results=processing_results,
        )

        if merged_df.empty:
            logger.warning("No completed new logs found - nothing appended.")
            return

        append_dataframe_to_workbook_table(
            data_frame=merged_df,
            workbook_path=workbook_path,
            sheet_name=sheet_name,
            table_name=table_name,
        )
        logger.success(f"Appended {len(merged_df)} row(s) to {workbook_path}")
