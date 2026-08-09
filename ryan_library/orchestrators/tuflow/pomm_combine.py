"""
Modern POMM Combination Utilities.

This module provides the logic for combining "POMM" (Plot Output Maximums/Minimums) CSV files.
It handles finding files, processing them in parallel via `ProcessorCollection`, filtering by data types (POMM, RLL_Qmx),
and exporting the consolidated results to Excel or Parquet.
"""
__lazy_modules__ = ['pandas']

from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import pandas as pd

from ._combination_workflow import execute_combination_workflow

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


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """
    Generate merged culvert data and export the results.

    Orchestrates the workflow:
      1. Normalize and validate input arguments (data types, locations).
      2. Collect target files from `paths_to_process`.
      3. Process files concurrently to extract data.
      4. Combine and export the results.

    Args:
        paths_to_process: Directories to search for POMM files.
        include_data_types: Specific file types to look for (e.g. "POMM", "RLL_Qmx").
        console_log_level: Logging verbosity ("INFO", "DEBUG", etc.).
        locations_to_include: Specific location strings to filter for.
        export_mode: Output format ("excel", "parquet", "both").
    """
    execute_combination_workflow(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        default_data_types=DEFAULT_DATA_TYPES,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context_name="POMM combination",
        export_prefix="combined_POMM",
        export_sheet_name="combined_POMM",
        export_metadata={"Workflow": "POMM combine"},
        combine_callable=_combine_pomm_results,
        console_log_level=console_log_level,
        locations_to_include=locations_to_include,
        export_mode=export_mode,
    )
