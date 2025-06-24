# ryan_library/functions/tuflow/tuflow_common.py
from __future__ import annotations
from pathlib import Path
from multiprocessing import Pool
from dataclasses import dataclass, field
from queue import Queue
from typing import Any

import pandas as pd
from loguru import logger

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.loguru_helpers import setup_logger, worker_initializer
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


@dataclass(frozen=True)
class ScenarioConfig:
    """One export scenario."""

    method_name: str  # e.g. "combine_1d_timeseries"
    parquet_prefix: str  # e.g. "1d_timeseries_data"
    excel_sheet: str  # e.g. "Timeseries"
    export_parquet: bool  # only True for timeseries
    column_order: list[str] = field(default_factory=list)


def collect_files(
    paths_to_process: list[Path],
    include_data_types: list[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    data_map: dict[str, list[str]] = suffixes_config.invert_suffix_to_type()
    suffixes: list[str] = []
    for dt in include_data_types:
        if dt in data_map:
            suffixes.extend(data_map[dt])
        else:
            logger.error(f"No suffixes for data type '{dt}'")
    if not suffixes:
        return []

    patterns: list[str] = [f"*{s}" for s in suffixes]
    roots: list[Path] = [p for p in paths_to_process if p.is_dir()]
    for bad in set(paths_to_process) - set(roots):
        logger.warning(f"Skipping non-dir {bad}")

    files: list[Path] = find_files_parallel(root_dirs=roots, patterns=patterns)
    return [f for f in files if is_non_zero_file(f)]


def process_file(file_path: Path) -> BaseProcessor | None:
    try:
        proc: BaseProcessor = BaseProcessor.from_file(file_path=file_path)
        proc.process()
        if proc.validate_data():
            logger.debug(f"Processed {file_path}")
        else:
            logger.warning(f"Validation failed {file_path}")
        return proc
    except Exception:
        logger.exception(f"Error processing {file_path}")
        return None


def process_files_in_parallel(
    file_list: list[Path],
    log_queue: Any,
) -> ProcessorCollection:
    size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Spawning pool with {size} workers")
    coll = ProcessorCollection()
    with Pool(
        processes=size, initializer=worker_initializer, initargs=(log_queue,)
    ) as pool:
        for proc in pool.map(func=process_file, iterable=file_list):
            if proc and proc.processed:
                coll.add_processor(processor=proc)
    return coll


def bulk_read_and_merge_tuflow_csv(
    paths_to_process: list[Path],
    include_data_types: list[str],
    log_queue,
) -> ProcessorCollection:
    logger.info("Starting TUFLOW culvert processing")
    files: list[Path] = collect_files(
        paths_to_process=paths_to_process,
        include_data_types=include_data_types,
        suffixes_config=SuffixesConfig.get_instance(),
    )
    if not files:
        logger.error("No files found")
        return ProcessorCollection()

    results: ProcessorCollection = process_files_in_parallel(
        file_list=files, log_queue=log_queue
    )
    # tell the queue “no more data” and wait for its feeder thread to finish
    return results
