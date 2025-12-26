# ryan_library/orchestrators/tuflow/tuflow_culverts_merge.py
"""
Merge TUFLOW Culvert Maximums.

This module combines "1d_maximums" style CSV data (culvert results) into a single summary.
It supports multiple data types (Nmx, Cmx, Chan, etc.) and exports the raw concatenated data
as well as the specific `combine_1d_maximums` processed view.
"""

from collections.abc import Collection
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

DEFAULT_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: Collection[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    output_dir: Path | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """
    Driver for culvert-merge exports.

    Orchestrates the finding, reading, merging, and exporting of culvert maximums data.
    Exports two sheets/tables:
      1. Maximums: The processed maximums (Logic defined in `combine_1d_maximums`).
      2. raw_data: The raw concatenation of all found files.

    Args:
        paths_to_process: Directories to scan.
        include_data_types: List of file suffixes to include (e.g. "Nmx").
        console_log_level: Logging verbosity.
        locations_to_include: Specific location IDs to filter for.
        output_dir: Destination directory for the export.
        export_mode: "excel", "parquet", or "both".
    """

    requested_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)

    with setup_logger(console_log_level=console_log_level) as log_queue:
        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert maximums",
        )

        collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=paths_to_process,
            include_data_types=requested_types,
            log_queue=log_queue,
            console_log_level=console_log_level,
        )

        if normalized_locations:
            collection.filter_locations(locations=normalized_locations)

        if not collection.processors:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context="Culvert maximums completed",
            )
            logger.warning("No culvert result files were processed. Skipping export.")
            return

        collection.compact_basic_info_columns()

        maximums_df: pd.DataFrame = collection.combine_1d_maximums()
        raw_df: pd.DataFrame = collection.combine_raw()
        raw_df = collection.attach_basic_info(df=raw_df, drop_id=True)
        if maximums_df.empty and raw_df.empty:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context="Culvert maximums completed",
            )
            logger.warning("Combined culvert DataFrames are empty. Skipping export.")
            return

        export_dict: dict[str, dict[str, list[pd.DataFrame] | list[str]]] = {
            "1d_maximums_data": {
                "dataframes": [maximums_df, raw_df],
                "sheets": ["Maximums", "raw_data"],
            }
        }
        logger.info(f"Exporting culvert maximums to {output_dir or Path.cwd()}")
        ExcelExporter().export_dataframes(
            export_dict=export_dict,
            output_directory=output_dir,
            export_mode=export_mode,
            parquet_compression="gzip",
        )
        logger.info("Culvert maximums export complete.")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert maximums completed",
        )
