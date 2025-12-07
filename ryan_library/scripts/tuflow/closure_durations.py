# ryan_library/scripts/tuflow/closure_durations.py
"""Orchestrator for computing closure durations from TUFLOW PO processors.

This script:
1. Loads PO files via the TUFLOW processors (`bulk_read_and_merge_tuflow_csv`).
2. Optionally filters locations using `ProcessorCollection`.
3. Calculates exceedance durations per threshold and location.
4. Summarises results (median/mean stats) and writes parquet/CSV artefacts.

Heavy lifting lives in ``ryan_library.functions.tuflow.closure_durations_functions``;
this module wires the pieces together and handles I/O/logging.
"""

from datetime import datetime
from pathlib import Path
from collections.abc import Iterable

from pandas import DataFrame
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.closure_durations_functions import (
    calculate_threshold_durations,
    collect_po_data,
    summarise_results,
)
from ryan_library.functions.tuflow.tuflow_common import bulk_read_and_merge_tuflow_csv
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def run_closure_durations(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    *,
    data_type: str = "Flow",
    allowed_locations: tuple[str, ...] | None = None,
    log_level: str = "INFO",
) -> None:
    """Process PO files under ``paths`` and report closure durations.

    Workflow:
    - Discover and parse PO files via the processor pipeline.
    - Filter by location (if provided).
    - Compute exceedance durations for each threshold/location.
    - Summarise results and export parquet/CSV/Excel artefacts.
    """
    if paths is None:
        paths = [Path.cwd()]
    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in values]
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=allowed_locations)

    with setup_logger(console_log_level=log_level) as log_queue:
        collection: ProcessorCollection = bulk_read_and_merge_tuflow_csv(
            paths_to_process=list(paths),
            include_data_types=["PO"],
            log_queue=log_queue,
        )

        if normalized_locations:
            collection.filter_locations(locations=normalized_locations)

        if not collection.processors:
            logger.warning("No PO CSV files were processed.")
            return

        # Combine PO processor outputs; warns if nothing to work with.
        po_df: DataFrame = collect_po_data(collection=collection)
        if po_df.empty:
            logger.warning("PO processors returned no data. Skipping export.")
            return

        # Calculate exceedance durations per threshold/location.
        result_df: DataFrame = calculate_threshold_durations(
            po_df=po_df,
            thresholds=thresholds,
            measurement_type=data_type,
        )
        if result_df.empty:
            logger.warning("No hydrograph data processed.")
            return

        timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
        result_df.to_parquet(path=f"{timestamp}_durex.parquet.gzip", compression="gzip")
        result_df.to_csv(path_or_buf=f"{timestamp}_durex.csv", index=False)

        summary_df: DataFrame = summarise_results(df=result_df)
        summary_df["AEP_sort_key"] = summary_df["AEP"].str.extract(r"([0-9]*\.?[0-9]+)")[0].astype(dtype=float)
        summary_df.sort_values(
            by=["Path", "Location", "ThresholdFlow", "AEP_sort_key"], ignore_index=True, inplace=True
        )
        summary_df.drop(columns="AEP_sort_key", inplace=True)
        summary_df.to_csv(path_or_buf=f"{timestamp}_QvsTexc.csv", index=False)
        logger.info("Processing complete")
