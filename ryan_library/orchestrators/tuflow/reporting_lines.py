"""Report TUFLOW reporting-location line flow series and source maximums separately."""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import Literal

from ...functions.live_dashboard import LiveWorkflowDashboard
from ...processors.tuflow.processor_collection import ProcessorCollection
from ._combination_workflow import execute_combination_workflow
from ._reporting_location_export import export_reporting_location_results

DEFAULT_DATA_TYPES: tuple[str, ...] = ("RLL_Q", "RLL_Qmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def export_results(
    *,
    results: ProcessorCollection,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    dashboard: LiveWorkflowDashboard | None = None,
) -> bool:
    """Publish signed line-flow samples and existing source Qmax data on separate sheets."""
    return export_reporting_location_results(
        results=results,
        products=(("RLL_Q", "Line_Flow_Series"), ("RLL_Qmx", "Source_Flow_Maxima")),
        export_key="reporting_lines",
        workflow_name="Reporting-location line flows",
        metric_label="Reporting-line rows",
        measurement_notes="Source model flow units; Q/Time in Qmx are source Qmax/Time Qmax; dQ/Time dQ are distinct",
        export_mode=export_mode,
        dashboard=dashboard,
    )


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
    """Discover, select and safely publish reporting-line flow histories and source maximums."""
    return execute_combination_workflow(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        default_data_types=DEFAULT_DATA_TYPES,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context_name="Reporting-line flows",
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
