# ryan_library/orchestrators/tuflow/closure_durations.py
"""Orchestrator for computing closure durations from TUFLOW PO processors.

This script:
1. Loads PO files via the TUFLOW processors (`bulk_read_and_merge_tuflow_csv`).
2. Optionally filters locations using `ProcessorCollection`.
3. Calculates exceedance durations per threshold and location.
4. Summarises results (median/mean stats) and exports Excel/Parquet artefacts.

Heavy lifting lives in ``ryan_library.functions.tuflow.closure_durations_functions``;
this module wires the pieces together and handles I/O/logging.
"""

__lazy_modules__ = ["pandas"]

from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Literal

from loguru import logger
from pandas import DataFrame

from ...functions.excel_export import ExcelExporter, ExportContent, RawTableExport
from ...functions.live_dashboard import LiveWorkflowDashboard
from ...functions.loguru_helpers import setup_logger
from ...functions.tuflow.closure_durations_functions import (
    calculate_threshold_durations,
    collect_po_data,
    summarise_results,
)
from ...functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ...processors.tuflow.base_processor import BaseProcessor
from ...processors.tuflow.processor_collection import ProcessorCollection

# Aliases used for testing/mocking
_collect_po_data = collect_po_data
_calculate_threshold_durations = calculate_threshold_durations


def run_closure_durations(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    *,
    data_type: str = "Flow",
    allowed_locations: tuple[str, ...] | None = None,
    log_level: str = "INFO",
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Process PO closure durations with a dashboard that lasts through export.

    The legacy raw tables and selected formatted reports remain one protected
    publication batch. Per-file failures within the shared processor pipeline
    require its separate validity/outcome contract and are not inferred here.
    """
    if paths is None:
        paths = [Path.cwd()]
    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in values]
    normalized_locations = BaseProcessor.normalize_locations(locations=allowed_locations)
    dashboard = LiveWorkflowDashboard(
        title="TUFLOW Closure Durations",
        subtitle=str(Path.cwd()),
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
    )

    with setup_logger(console_log_level=log_level) as log_queue:
        with dashboard:
            dashboard.set_tasks(labels=["Closure-duration report"])
            dashboard.mark_running(index=1)
            try:
                dashboard.set_phase("Processing")
                collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
                    paths_to_process=list(paths),
                    include_data_types=["PO"],
                    log_queue=log_queue,
                )
                if normalized_locations:
                    collection.filter_locations(locations=normalized_locations)
                if not collection.processors:
                    logger.warning("No PO CSV files were processed.")
                    dashboard.mark_finished(index=1, status="SKIP", detail="no PO processors")
                    dashboard.finish(outcome="SKIPPED", detail="no report generated")
                    return True

                dashboard.set_phase("Aggregating")
                po_df: DataFrame = _collect_po_data(collection=collection)
                if po_df.empty:
                    logger.warning("PO processors returned no data. Skipping export.")
                    dashboard.mark_finished(index=1, status="SKIP", detail="no PO rows")
                    dashboard.finish(outcome="SKIPPED", detail="no report generated")
                    return True

                result_df: DataFrame = _calculate_threshold_durations(
                    po_df=po_df,
                    thresholds=thresholds,
                    measurement_type=data_type,
                )
                if result_df.empty:
                    logger.warning("No hydrograph data processed.")
                    dashboard.mark_finished(index=1, status="SKIP", detail="no exceedance rows")
                    dashboard.finish(outcome="SKIPPED", detail="no report generated")
                    return True

                timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
                summary_df: DataFrame = summarise_results(df=result_df)
                summary_df["AEP_sort_key"] = summary_df["AEP"].str.extract(r"([0-9]*\\.?[0-9]+)")[0].astype(dtype=float)
                summary_df = summary_df.sort_values(
                    by=["Path", "Location", "ThresholdFlow", "AEP_sort_key"], ignore_index=True
                )
                summary_df = summary_df.drop(columns="AEP_sort_key")
                dashboard.set_phase("Exporting")
                _export_closure_duration_artifacts(
                    durations_df=result_df,
                    summary_df=summary_df,
                    timestamp=timestamp,
                    export_mode=export_mode,
                )
                logger.info("Closure duration export complete.")
                dashboard.mark_finished(index=1, status="OK", detail=f"{len(result_df)} duration row(s)")
                dashboard.finish(outcome="SUCCESS", detail="closure report exported")
                return True
            except KeyboardInterrupt:
                dashboard.mark_finished(index=1, status="FAIL", detail="interrupted")
                dashboard.finish(outcome="INTERRUPTED", detail="processing interrupted")
                raise
            except Exception:
                logger.exception("Closure duration workflow failed during processing or export.")
                dashboard.mark_finished(index=1, status="FAIL", detail="processing or export failed")
                dashboard.finish(outcome="FAILED", detail="processing or export failed")
                raise


def _export_closure_duration_artifacts(
    *,
    durations_df: DataFrame,
    summary_df: DataFrame,
    timestamp: str,
    export_mode: Literal["excel", "parquet", "both"],
) -> None:
    """Export legacy raw tables and selected reports in one protected batch."""
    frames: list[DataFrame] = []
    sheet_names: list[str] = []
    if not durations_df.empty:
        frames.append(durations_df)
        sheet_names.append("Duration_Exceedance")
    if not summary_df.empty:
        frames.append(summary_df)
        sheet_names.append("Summary")

    if not frames:
        logger.warning("No closure duration results to export.")
        return

    file_label: str = f"{timestamp}_closure_durations"
    export_dict: dict[str, ExportContent] = {file_label: {"dataframes": frames, "sheets": sheet_names}}
    raw_exports = (
        (
            RawTableExport(f"{timestamp}_durex.parquet.gzip", durations_df, "parquet"),
            RawTableExport(f"{timestamp}_durex.csv", durations_df, "csv"),
        )
        if not durations_df.empty
        else ()
    )
    ExcelExporter().export_dataframes(
        export_dict=export_dict,
        output_directory=Path.cwd(),
        export_mode=export_mode,
        parquet_compression="gzip",
        include_data_dictionary=True,
        data_dictionary_metadata={"Workflow": "Closure durations"},
        raw_exports=raw_exports,
    )
