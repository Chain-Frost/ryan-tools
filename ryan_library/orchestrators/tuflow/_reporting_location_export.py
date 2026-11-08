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
