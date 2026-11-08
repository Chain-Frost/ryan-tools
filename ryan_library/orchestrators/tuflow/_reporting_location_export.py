"""Shared protected multi-sheet exporter for reporting locations, not a results parser."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ...functions.excel_export import ExcelExporter, ExportContent
from ...functions.live_dashboard import LiveWorkflowDashboard, WorkflowMetric
from ...processors.tuflow.processor_collection import ProcessorCollection


def export_reporting_location_results(
    *,
    results: ProcessorCollection,
    products: tuple[tuple[str, str], ...],
    export_key: str,
    workflow_name: str,
    metric_label: str,
    measurement_notes: str,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    dashboard: LiveWorkflowDashboard | None = None,
) -> bool:
    """Preserve independent time histories and source summaries on distinct sheets."""
    if dashboard is not None:
        dashboard.set_phase("Aggregating")
    if not results.processors:
        logger.warning("No {} results to export.", workflow_name)
        return False

    frames: list[pd.DataFrame] = []
    sheets: list[str] = []
    for data_type, sheet_name in products:
        subset = results.get_processors_by_data_type(data_type)
        if not subset.processors:
            continue
        combined = subset.combine_raw()
        if not combined.empty:
            frames.append(combined)
            sheets.append(sheet_name)

    if not frames:
        logger.warning("No {} rows to export.", workflow_name)
        return False

    # A result family may be present for one run and missing for another.
    # Diagnose each incomplete run, not just missing products across the full batch.
    requested_types = {data_type for data_type, _ in products}
    available_by_run: dict[str, set[str]] = {}
    for processor in results.processors:
        if processor.data_type in requested_types and not processor.df.empty:
            available_by_run.setdefault(processor.name_parser.raw_run_code, set()).add(processor.data_type)
    for run_code, available in sorted(available_by_run.items()):
        missing = sorted(requested_types - available)
        if missing:
            logger.warning(
                "{}: run '{}' has no usable optional companion(s) {} (available: {}); "
                "publishing available reporting products only. Missing input may be absent, invalid, or filtered.",
                workflow_name,
                run_code,
                ", ".join(f"*_{data_type}.csv" for data_type in missing),
                ", ".join(sorted(available)),
            )

    if dashboard is not None:
        dashboard.set_metrics([WorkflowMetric(metric_label, value=sum(len(frame) for frame in frames), unit="rows")])
        dashboard.set_phase("Exporting")

    export: ExportContent = {"dataframes": frames, "sheets": sheets}
    ExcelExporter().export_dataframes(
        export_dict={export_key: export},
        output_directory=Path.cwd(),
        export_mode=export_mode,
        parquet_compression="gzip",
        include_data_dictionary=True,
        data_dictionary_metadata={
            "Workflow": workflow_name,
            "Times": "Simulation hours",
            "Measurements": measurement_notes,
        },
    )
    return True
