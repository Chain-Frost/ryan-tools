# ryan_library/orchestrators/tuflow/tuflow_culverts_timeseries.py
"""Merge TUFLOW Culvert Timeseries.

This module combines "1d_timeseries" style CSV data (e.g., flow over time for culverts).
It leverages `combine_1d_timeseries` from `ProcessorCollection` to produce a consolidated
timeseries dataset.
"""

__lazy_modules__ = ["pandas"]

from collections.abc import Collection
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ryan_library.functions.excel_export import ExcelExporter, ExportContent
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowMetric
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress, bulk_read_and_merge_tuflow_csv
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

DEFAULT_DATA_TYPES: tuple[str, ...] = ("Q", "V", "H", "CF", "Chan", "EOF")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: Collection[str] | None = None,
    console_log_level: str | None = None,
    locations_to_include: Collection[str] | None = None,
    output_dir: Path | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    *,
    use_live_dashboard: bool = False,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> bool:
    """Combine selected results and export them; report unsuccessful inputs explicitly.

    False means an input-processing failure or export failure (which also
    propagates its exception). A normal empty selection is SKIPPED, not SUCCESS.
    Dashboard settings never affect selection, aggregation, or output contents.
    """
    requested_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)
    dashboard = LiveWorkflowDashboard(
        title="Culvert timeseries",
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
    )
    progress = FileBatchProgress()
    console_level = (
        console_log_level if console_log_level is not None else ("SUCCESS" if use_live_dashboard else "INFO")
    )
    with setup_logger(
        console_log_level=console_level,
        log_file=log_file,
        file_log_level=file_log_level,
    ) as log_queue:
        with dashboard:
            try:
                dashboard.set_phase("Discovering")
                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=ACCEPTED_DATA_TYPES,
                    context="Culvert timeseries",
                )
                collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
                    paths_to_process=paths_to_process,
                    include_data_types=requested_types,
                    log_queue=log_queue,
                    console_log_level=console_level,
                    dashboard=dashboard if use_live_dashboard else None,
                    batch_progress=progress,
                )
                if normalized_locations:
                    collection.filter_locations(locations=normalized_locations)
                if not collection.processors:
                    warn_on_invalid_types(
                        invalid_types=invalid_types,
                        accepted_types=ACCEPTED_DATA_TYPES,
                        context="Culvert timeseries completed",
                    )
                    logger.warning("No result files were processed. Skipping export.")
                    dashboard.finish(outcome="FAILED" if progress.failed else "SKIPPED", detail="no results to export")
                    return progress.failed == 0

                dashboard.set_phase("Aggregating")
                timeseries_df: pd.DataFrame = collection.combine_1d_timeseries()
                dashboard.set_metrics(
                    [
                        WorkflowMetric("Files finished", progress.completed, unit="files", total=progress.scheduled),
                        WorkflowMetric("Timeseries rows", len(timeseries_df), unit="rows"),
                    ]
                )
                if timeseries_df.empty:
                    warn_on_invalid_types(
                        invalid_types=invalid_types,
                        accepted_types=ACCEPTED_DATA_TYPES,
                        context="Culvert timeseries completed",
                    )
                    logger.warning("Combined culvert timeseries DataFrame is empty. Skipping export.")
                    dashboard.finish(outcome="FAILED" if progress.failed else "SKIPPED", detail="no combined data")
                    return progress.failed == 0

                dashboard.set_phase("Exporting")
                export_dict: dict[str, ExportContent] = {
                    "1d_timeseries_data": {
                        "dataframes": [timeseries_df],
                        "sheets": ["1d_timeseries_data"],
                    }
                }
                ExcelExporter().export_dataframes(
                    export_dict=export_dict,
                    output_directory=output_dir,
                    export_mode=export_mode,
                    parquet_compression="gzip",
                    include_data_dictionary=True,
                    data_dictionary_metadata={"Workflow": "Culvert timeseries"},
                )
                logger.info("Culvert timeseries export complete.")

                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=ACCEPTED_DATA_TYPES,
                    context="Culvert timeseries completed",
                )
                dashboard.set_phase("Complete")
                dashboard.finish(
                    outcome="FAILED" if progress.failed else "SUCCESS",
                    detail="exported with input failures" if progress.failed else "report exported",
                )
                return progress.failed == 0
            except KeyboardInterrupt:
                dashboard.finish(outcome="INTERRUPTED", detail="processing interrupted")
                raise
            except Exception:
                logger.exception("Culvert timeseries workflow failed.")
                dashboard.finish(outcome="FAILED", detail="processing or export failed")
                raise
