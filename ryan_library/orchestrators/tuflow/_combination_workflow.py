# ryan_library/orchestrators/tuflow/_combination_workflow.py
"""
Internal helper for orchestrating TUFLOW combination workflows (PO, POMM, etc.).

This module extracts the shared workflow of parsing directories, executing processors in parallel,
and exporting combined datasets to keep the public orchestrator entry points focused.
"""

__lazy_modules__: list[str] = ["pandas"]

from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Literal, Callable, Protocol

import pandas as pd
from loguru import logger

from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.functions.excel_export import ExcelExporter
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types


class CombinationResults(Protocol):
    """Minimum result-collection interface required by the shared exporter."""

    @property
    def processors(self) -> Sequence[object]: ...


class CombinationExporter(Protocol):
    """Callable interface used to route processed results to a public exporter."""

    def __call__(
        self,
        *,
        results: ProcessorCollection,
        export_mode: Literal["excel", "parquet", "both"] = "excel",
    ) -> None: ...


def execute_combination_workflow(
    *,
    paths_to_process: list[Path],
    include_data_types: list[str] | None,
    default_data_types: tuple[str, ...],
    accepted_data_types: frozenset[str],
    context_name: str,
    export_results: CombinationExporter,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """
    Generate merged data and export the results.

    Args:
        paths_to_process: Directories to search for files.
        include_data_types: Specific file types to look for.
        default_data_types: Default types if none requested.
        accepted_data_types: Types supported by this workflow.
        context_name: Name of workflow for logging (e.g. "PO combination").
        export_results: Public domain-specific exporter for the processed results.
        console_log_level: Logging verbosity.
        locations_to_include: Specific location strings to filter for.
        export_mode: Output format.
    """
    requested_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=default_data_types,
        accepted=accepted_data_types,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)

    with setup_logger(console_log_level=console_log_level) as log_queue:
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
            return

        # Process the file list in parallel
        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=log_queue,
            log_level=console_log_level,
            entity_filters=normalized_locations if normalized_locations else None,
        )

        export_results(results=results_set, export_mode=export_mode)
        logger.info(f"End of {context_name} processing")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=accepted_data_types,
            context=f"{context_name} completed",
        )


def combine_and_export_results[T: CombinationResults](
    *,
    results: T,
    export_mode: Literal["excel", "parquet", "both"],
    export_prefix: str,
    export_sheet_name: str,
    export_metadata: dict[str, str],
    combine_callable: Callable[[T], pd.DataFrame],
) -> None:
    """Export combined DataFrames according to the requested mode."""
    if not results.processors:
        logger.warning("No results to export.")
        return

    combined_df: pd.DataFrame = combine_callable(results)

    if combined_df.empty:
        logger.warning("No combined data found. Skipping export.")
        return

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
