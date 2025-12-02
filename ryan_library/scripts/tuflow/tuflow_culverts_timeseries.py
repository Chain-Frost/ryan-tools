# ryan_library/scripts/tuflow/tuflow_culverts_timeseries.py
from collections.abc import Collection
from loguru import logger
from pathlib import Path
from datetime import datetime
from pandas import DataFrame

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter, ExportContent
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types

DEFAULT_DATA_TYPES: tuple[str, ...] = ("Q", "V", "H", "CF", "Chan", "EOF")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def main_processing(
    paths_to_process: list[Path],
    include_data_types: Collection[str] | None = None,
    console_log_level: str = "INFO",
    locations_to_include: Collection[str] | None = None,
    output_dir: Path | None = None,
    output_parquet: bool = False,
) -> None:
    """Driver for culvert-timeseries exports."""
    requested_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations_to_include)

    with setup_logger(console_log_level=console_log_level) as log_q:
        logger.info("Starting TUFLOW culvert processing")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert timeseries",
        )

        collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=paths_to_process,
            include_data_types=requested_types,
            log_queue=log_q,
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

        df1: DataFrame = collection.combine_1d_timeseries()

        if output_parquet:
            datetime_string: str = datetime.now().strftime(format="%Y%m%d-%H%M")
            df1.to_parquet(path=f"{datetime_string}_1d_maximums_data.parquet")

        export_dict: dict[str, ExportContent] = {
            "1d_timeseries_data": {
                "dataframes": [df1],
                "sheets": ["1d_timeseries_data"],
            }
        }
        ExcelExporter().export_dataframes(export_dict=export_dict, output_directory=output_dir)
        logger.info("Done.")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="Culvert timeseries completed",
        )
    # tell the queue “no more data” and wait for its feeder thread to finish
    log_q.close()
    log_q.join_thread()
