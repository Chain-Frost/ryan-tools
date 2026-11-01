# ryan_library/orchestrators/tuflow/pomm_combine.py
"""Modern POMM Combination Utilities.

This module provides the logic for combining "POMM" (Plot Output Maximums/Minimums) CSV files.
It handles finding files, processing them in parallel via `ProcessorCollection`, filtering by data types (POMM, RLL_Qmx),
and exporting the consolidated results to Excel or Parquet.
"""

__lazy_modules__: list[str] = ["pandas"]

from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import pandas as pd

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard

from ._combination_workflow import (
    combine_and_export_results as _combine_and_export_results,
)
from ._combination_workflow import (
    execute_combination_workflow,
)

DEFAULT_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


class PommCombinationResults(Protocol):
    """Minimum result-collection interface needed for POMM export."""

    @property
    def processors(self) -> Sequence[object]: ...
    def pomm_combine(self) -> pd.DataFrame: ...


@runtime_checkable
class RawCombinationResults(Protocol):
    """Result collection that supports the preferred generic combination path."""

    @property
    def processors(self) -> Sequence[object]: ...
    def combine_raw(self) -> pd.DataFrame: ...


def _combine_pomm_results(results: RawCombinationResults | PommCombinationResults) -> pd.DataFrame:
    """Helper to dispatch combination logic based on supported protocols."""
    if isinstance(results, RawCombinationResults):
        return results.combine_raw()
    return results.pomm_combine()


def export_results(
    *,
    results: RawCombinationResults | PommCombinationResults,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    dashboard: LiveWorkflowDashboard | None = None,
) -> bool:
    """Export the POMM results through the established public entry point."""
    if dashboard is not None:
        return _combine_and_export_results(
            results=results,
            export_mode=export_mode,
            export_prefix="combined_POMM",
            export_sheet_name="combined_POMM",
            export_metadata={"Workflow": "POMM combine"},
            combine_callable=_combine_pomm_results,
            dashboard=dashboard,
            row_metric_label="Combined peak rows",
        )
    return _combine_and_export_results(
        results=results,
        export_mode=export_mode,
        export_prefix="combined_POMM",
        export_sheet_name="combined_POMM",
        export_metadata={"Workflow": "POMM combine"},
        combine_callable=_combine_pomm_results,
    )


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str | None = None,
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
    *,
    use_live_dashboard: bool = False,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 25,
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> bool:
    """Combine and export POMM results, preserving the public path."""
    return execute_combination_workflow(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        default_data_types=DEFAULT_DATA_TYPES,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context_name="POMM combination",
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
