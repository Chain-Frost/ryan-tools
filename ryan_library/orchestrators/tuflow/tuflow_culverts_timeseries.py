# ryan_library/scripts/tuflow/tuflow_culverts_timeseries.py
"""
Merge TUFLOW Culvert Timeseries.

This module combines "1d_timeseries" style CSV data (e.g., flow over time for culverts).
It leverages `combine_1d_timeseries` from `ProcessorCollection` to produce a consolidated
timeseries dataset.
"""

from collections.abc import Collection
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter, ExportContent
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

DEFAULT_DATA_TYPES: tuple[str, ...] = ("Q", "V", "H", "CF", "Chan", "EOF")
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
    Driver for culvert-timeseries exports.

    Orchestrates the finding, reading, merging, and exporting of culvert timeseries data.

    Args:
        paths_to_process: Directories to scan.
        include_data_types: List of file suffixes to include (e.g. "Q", "V", "H").
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
        logger.info("Starting TUFLOW culvert processing")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert timeseries",
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
                context="Culvert timeseries completed",
            )
            logger.warning("No culvert timeseries files were processed. Skipping export.")
            return

        timeseries_df: pd.DataFrame = collection.combine_1d_timeseries()
        if timeseries_df.empty:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context="Culvert timeseries completed",
            )
            logger.warning("Combined culvert timeseries DataFrame is empty. Skipping export.")
            return

        export_dict: dict[str, ExportContent] = {
            "1d_timeseries_data": {
                "dataframes": [timeseries_df],
                "sheets": ["1d_timeseries_data"],
            }
        }
        ExcelExporter().export_dataframes(
            export_dict=export_dict,
            output_directory=output_dir,
            export_mode=export_mode,
            parquet_compression="gzip",
        )
        logger.info("Culvert timeseries export complete.")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert timeseries completed",
        )
