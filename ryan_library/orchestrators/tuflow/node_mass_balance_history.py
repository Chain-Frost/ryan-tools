"""Protected TUFLOW 1D node mass-balance history workflow."""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import Literal

from ...functions.live_dashboard import LiveWorkflowDashboard
from ...processors.tuflow.processor_collection import ProcessorCollection
from ._combination_workflow import execute_combination_workflow
from ._reporting_location_export import export_reporting_location_results

DEFAULT_DATA_TYPES: tuple[str, ...] = ("TSMB",)
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def export_results(
    *,
    results: ProcessorCollection,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    dashboard: LiveWorkflowDashboard | None = None,
) -> bool:
    """Publish only selected source families with their original measurement meaning."""
    return export_reporting_location_results(
        results=results,
        products=(("TSMB", "Node_Mass_Balance_History"),),
        export_key="node_mass_balance",
        workflow_name="1D node mass-balance history",
        metric_label="Node balance rows",
        measurement_notes="Original 1D node mass-balance histories, separate from model/domain MB and node summaries.",
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
    """Discover, filter and safely export the selected family."""
    return execute_combination_workflow(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        default_data_types=DEFAULT_DATA_TYPES,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context_name="1D node mass-balance history",
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
