"""Modern POMM combination utilities."""

from collections.abc import Collection
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger

from ryan_library.functions.tuflow.pomm_utils import process_files_in_parallel
from ryan_library.functions.tuflow.tuflow_common import collect_files
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types

DEFAULT_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    export_mode: Literal["excel", "parquet", "both"] = "excel",
) -> None:
    """Generate merged culvert data and export the results."""

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
            context="POMM combination",
        )

        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=requested_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context="POMM combination completed",
            )
            logger.info("No valid files found to process.")
            return

        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=log_queue,
            location_filter=normalized_locations if normalized_locations else None,
        )

    if normalized_locations:
        results_set.filter_locations(locations=normalized_locations)

    export_results(results=results_set, export_mode=export_mode)
    logger.info("End of POMM results combination processing")

    warn_on_invalid_types(
        invalid_types=invalid_types,
        accepted_types=ACCEPTED_DATA_TYPES,
        context="POMM combination completed",
    )


def export_results(*, results: ProcessorCollection, export_mode: Literal["excel", "parquet", "both"] = "excel") -> None:
    """Export combined DataFrames according to the requested mode."""
    if not results.processors:
        logger.warning("No results to export.")
        return
    combined_df: pd.DataFrame
    if hasattr(results, "combine_raw"):
        combined_df = results.combine_raw()  # type: ignore[attr-defined]
    elif hasattr(results, "pomm_combine"):
        combined_df = results.pomm_combine()  # type: ignore[attr-defined]
    else:
        logger.warning("Results object does not support combine_raw or pomm_combine. Skipping export.")
        return
    if combined_df.empty:
        logger.warning("No combined data found. Skipping export.")
        return

    ensure_output_directory(output_dir=Path.cwd())
    exporter = ExcelExporter()
    exporter.save_to_excel(
        data_frame=combined_df,
        file_name_prefix="combined_POMM",
        sheet_name="combined_POMM",
        output_directory=Path.cwd(),
        export_mode=export_mode,
        parquet_compression="gzip",
    )
