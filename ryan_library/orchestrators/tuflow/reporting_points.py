"""Public combination and protected export for reporting-location point water levels."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

from collections.abc import Collection
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ...functions.excel_export import ExcelExporter, ExportContent
from ...functions.live_dashboard import LiveWorkflowDashboard, WorkflowMetric
from ...processors.tuflow.processor_collection import ProcessorCollection
from ._combination_workflow import execute_combination_workflow

DEFAULT_DATA_TYPES: tuple[str, ...] = ("RLP_H", "RLP_Hmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def export_results(
    *,
    results: ProcessorCollection,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    dashboard: LiveWorkflowDashboard | None = None,
) -> bool:
    """Keep time-series samples and TUFLOW-supplied maxima on separate report sheets."""
    if dashboard is not None:
        dashboard.set_phase("Aggregating")
    if not results.processors:
        logger.warning("No reporting-point results to export.")
        return False

    frames: list[pd.DataFrame] = []
    sheets: list[str] = []
    for data_type, sheet in (("RLP_H", "Water_Level_Series"), ("RLP_Hmx", "Source_Maxima")):
        combined = results.get_processors_by_data_type(data_type).combine_raw()
        if not combined.empty:
            frames.append(combined)
            sheets.append(sheet)

    if not frames:
        logger.warning("No reporting-point rows to export.")
        return False

    if dashboard is not None:
        dashboard.set_metrics([WorkflowMetric("Reporting-point rows", value=sum(map(len, frames)), unit="rows")])
        dashboard.set_phase("Exporting")

    export: ExportContent = {"dataframes": frames, "sheets": sheets}
    ExcelExporter().export_dataframes(
        export_dict={"reporting_points": export},
        output_directory=Path.cwd(),
        export_mode=export_mode,
        parquet_compression="gzip",
        include_data_dictionary=True,
        data_dictionary_metadata={
            "Workflow": "Reporting-location point water levels",
            "Times": "Simulation hours",
            "Measurements": "Source model units; Hmax and dHmax keep their own source times",
        },
    )
    return True


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str | None = None,
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    *,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
    log_file: str | None = None,
    file_log_level: str = "INFO",
) -> bool:
    """Discover, filter and export supported RLP level histories and supplied maximums."""
    return execute_combination_workflow(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        default_data_types=DEFAULT_DATA_TYPES,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context_name="Reporting-point levels",
        export_results=export_results,
        console_log_level=console_log_level,
        locations_to_include=locations_to_include,
        export_mode=export_mode,
        use_live_dashboard=use_live_dashboard,
        live_refresh_per_second=live_refresh_per_second,
        live_max_rows=live_max_rows,
        log_file=log_file,
        file_log_level=file_log_level,
    )
