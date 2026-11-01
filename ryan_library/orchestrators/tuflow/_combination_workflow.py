# ryan_library/orchestrators/tuflow/_combination_workflow.py
"""Internal helper for orchestrating TUFLOW combination workflows (PO, POMM, etc.).

This module extracts the shared workflow of parsing directories, executing processors in parallel,
and exporting combined datasets to keep the public orchestrator entry points focused.
"""

__lazy_modules__: list[str] = ["pandas"]

from collections.abc import Callable, Collection, Sequence
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd
from loguru import logger

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.excel_export import ExcelExporter
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowMetric
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress, collect_files, process_files_in_parallel
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


class CombinationResults(Protocol):
    """Minimum result-collection interface required by the shared exporter."""

    @property
    def processors(self) -> Sequence[object]: ...


class CombinationExporter(Protocol):
    """Public combination exporter, with optional display lifecycle support."""

    def __call__(
        self,
        *,
        results: ProcessorCollection,
        export_mode: Literal["excel", "parquet", "both"] = "excel",
        dashboard: LiveWorkflowDashboard | None = None,
    ) -> bool | None: ...


def execute_combination_workflow(
    *,
    paths_to_process: list[Path],
    include_data_types: list[str] | None,
    default_data_types: tuple[str, ...],
    accepted_data_types: frozenset[str],
    context_name: str,
    export_results: CombinationExporter,
    console_log_level: str | None = None,
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    use_live_dashboard: bool = False,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> bool:
    """Discover, process, combine and export with a truthful full-workflow status."""
    requested_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=default_data_types,
        accepted=accepted_data_types,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)
    dashboard = LiveWorkflowDashboard(
        title=context_name,
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
    )
    progress = FileBatchProgress()
    console_level = console_log_level if console_log_level is not None else (
        "SUCCESS" if use_live_dashboard else "INFO"
    )
    with setup_logger(
        console_log_level=console_level, log_file=log_file, file_log_level=file_log_level,
    ) as log_queue:
        with dashboard:
            try:
                dashboard.set_phase("Discovering")
                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=accepted_data_types,
                    context=context_name,
                )
                csv_file_list: list[Path] = collect_files(
                    paths_to_process=paths_to_process,
                    include_data_types=requested_types,
                    suffixes_config=SuffixesConfig.get_instance(),
                )
                if not csv_file_list:
                    warn_on_invalid_types(
                        invalid_types=invalid_types,
                        accepted_types=accepted_data_types,
                        context=f"{context_name} completed",
                    )
                    logger.info("No valid files found to process.")
                    dashboard.finish(outcome="SKIPPED", detail="no inputs")
                    return True

                dashboard.set_phase("Processing")
                results_set: ProcessorCollection = process_files_in_parallel(
                    file_list=csv_file_list,
                    log_queue=log_queue,
                    log_level=console_level,
                    entity_filters=normalized_locations or None,
                    dashboard=dashboard if use_live_dashboard else None,
                    batch_progress=progress,
                )
                dashboard.set_phase("Aggregating")
                # Keep the established public exporter entry point, including
                # calls made by clients without a dashboard.
                if use_live_dashboard:
                    exported = export_results(results=results_set, export_mode=export_mode, dashboard=dashboard)
                else:
                    exported = export_results(results=results_set, export_mode=export_mode)
                logger.info("End of {} processing", context_name)
                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=accepted_data_types,
                    context=f"{context_name} completed",
                )
                dashboard.set_phase("Complete")
                outcome = "FAILED" if progress.failed else ("SKIPPED" if exported is False else "SUCCESS")
                dashboard.finish(outcome=outcome, detail="report exported" if exported is not False else "no output")
                return outcome != "FAILED"
            except KeyboardInterrupt:
                dashboard.finish(outcome="INTERRUPTED", detail="processing interrupted")
                raise
            except Exception:
                logger.exception("{} workflow failed during processing or export.", context_name)
                dashboard.finish(outcome="FAILED", detail="processing or export failed")
                raise


def combine_and_export_results[T: CombinationResults](
    *,
    results: T,
    export_mode: Literal["excel", "parquet", "both"],
    export_prefix: str,
    export_sheet_name: str,
    export_metadata: dict[str, str],
    combine_callable: Callable[[T], pd.DataFrame],
    dashboard: LiveWorkflowDashboard | None = None,
    row_metric_label: str | None = None,
) -> bool:
    """Export the existing DataFrame and return whether an output was produced."""
    if dashboard is not None:
        dashboard.set_phase("Aggregating")
    if not results.processors:
        logger.warning("No results to export.")
        return False

    combined_df: pd.DataFrame = combine_callable(results)
    if dashboard is not None and row_metric_label is not None:
        dashboard.set_metrics([WorkflowMetric(row_metric_label, value=len(combined_df), unit="rows")])
    if combined_df.empty:
        logger.warning("No combined data found. Skipping export.")
        return False

    if dashboard is not None:
        dashboard.set_phase("Exporting")
    ensure_output_directory(output_dir=Path.cwd())
    exporter: ExcelExporter = ExcelExporter()
    exporter.save_to_excel(
        data_frame=combined_df,
        file_name_prefix=export_prefix,
        sheet_name=export_sheet_name,
        output_directory=Path.cwd(),
        export_mode=export_mode,
        parquet_compression="gzip",
        include_data_dictionary=True,
        data_dictionary_metadata=export_metadata,
    )
    return True
