# ryan_library/scripts/tuflow/tuflow_culverts_timeseries.py
from loguru import logger
from pathlib import Path
from datetime import datetime
from pandas import DataFrame

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.misc_functions import ExcelExporter
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str],
    console_log_level: str = "INFO",
    output_dir: Path | None = None,
    output_parquet: bool = False,
) -> None:
    """Driver for culvert-timeseries exports."""
    with setup_logger(console_log_level=console_log_level) as log_q:
        logger.info("Starting TUFLOW culvert processing")
        collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            log_queue=log_q,
        )

        df1: DataFrame = collection.combine_1d_timeseries()

        if output_parquet:
            datetime_string: str = datetime.now().strftime(format="%Y%m%d-%H%M")
            df1.to_parquet(f"{datetime_string}_1d_maximums_data.parquet")

        export_dict: dict = {
            "1d_timeseries_data": {
                "dataframes": [df1],
                "sheets": ["1d_timeseries_data"],
            }
        }
        ExcelExporter().export_dataframes(
            export_dict=export_dict, output_directory=output_dir
        )
        logger.info("Done.")
    # tell the queue “no more data” and wait for its feeder thread to finish
    log_q.close()
    log_q.join_thread()
