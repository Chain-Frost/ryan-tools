"""Modern POMM combination utilities."""

from pathlib import Path
import pandas as pd
from loguru import logger

from ryan_library.scripts.pomm_utils import (
    collect_files,
    process_files_in_parallel,
)
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] | None = None,
    console_log_level: str = "INFO",
) -> None:
    """Generate merged culvert data and export the results."""

    if include_data_types is None:
        include_data_types = ["POMM"]

    with setup_logger(console_log_level=console_log_level) as log_queue:
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list, log_queue=log_queue
        )

    export_results(results=results_set)
    logger.info("End of POMM results combination processing")


def export_results(results: ProcessorCollection) -> None:
    """Export combined DataFrames to Excel."""
    if not results.processors:
        logger.warning("No results to export.")
        return

    exporter = ExcelExporter()
    combined_df: pd.DataFrame = results.pomm_combine()
    if combined_df.empty:
        logger.warning("No combined data found. Skipping export.")
        return

    ensure_output_directory(output_dir=Path.cwd())
    exporter.save_to_excel(
        data_frame=combined_df,
        file_name_prefix="combined_POMM",
        sheet_name="combined_POMM",
        output_directory=Path.cwd(),
    )
