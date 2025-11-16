# ryan_library/functions/tuflow/tuflow_common.py
from __future__ import annotations
from pathlib import Path
from multiprocessing import Pool
from dataclasses import dataclass, field
from collections.abc import Iterable
from typing import Any
from loguru import logger

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.loguru_helpers import worker_initializer
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


def _string_list() -> list[str]:
    return []


@dataclass(frozen=True)
class ScenarioConfig:
    """One export scenario."""

    method_name: str  # e.g. "combine_1d_timeseries"
    parquet_prefix: str  # e.g. "1d_timeseries_data"
    excel_sheet: str  # e.g. "Timeseries"
    export_parquet: bool  # only True for timeseries
    column_order: list[str] = field(default_factory=_string_list)


def collect_files(
    paths_to_process: Iterable[Path],
    include_data_types: Iterable[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """Return all non-empty files matching ``include_data_types`` underneath ``paths_to_process``."""

    normalized_roots: list[Path] = []
    seen_roots: set[Path] = set()
    for candidate in paths_to_process:
        path = Path(candidate)
        if path in seen_roots:
            continue
        seen_roots.add(path)
        normalized_roots.append(path)

    deduped_types: list[str] = []
    seen_types: set[str] = set()
    for data_type in include_data_types:
        if data_type in seen_types:
            continue
        seen_types.add(data_type)
        deduped_types.append(data_type)

    if not deduped_types:
        logger.error("No data types were supplied for file collection.")
        return []

    data_map: dict[str, list[str]] = suffixes_config.invert_suffix_to_type()
    suffixes: list[str] = []
    for data_type in deduped_types:
        dt_suffixes: list[str] | None = data_map.get(data_type)
        if not dt_suffixes:
            logger.error(f"No suffixes for data type '{data_type}'. Skipping.")
            continue
        suffixes.extend(dt_suffixes)

    suffixes = list(dict.fromkeys(suffixes))
    if not suffixes:
        logger.error("No suffixes found for the requested data types.")
        return []

    patterns: list[str] = [f"*{suffix}" for suffix in suffixes]
    roots: list[Path] = [p for p in normalized_roots if p.is_dir()]
    invalid_roots: list[Path] = [p for p in normalized_roots if not p.is_dir()]
    for bad_root in invalid_roots:
        logger.warning(f"Skipping non-directory path {bad_root}")

    if not roots:
        logger.warning("No valid directories were supplied for file collection.")
        return []

    files: list[Path] = find_files_parallel(root_dirs=roots, patterns=patterns)
    filtered_files: list[Path] = []
    seen_files: set[Path] = set()
    for file_path in files:
        if not is_non_zero_file(file_path):
            continue
        if file_path in seen_files:
            continue
        seen_files.add(file_path)
        filtered_files.append(file_path)
    return filtered_files


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
    with Pool(processes=size, initializer=worker_initializer, initargs=(log_queue,)) as pool:
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

    results: ProcessorCollection = process_files_in_parallel(file_list=files, log_queue=log_queue)
    # tell the queue “no more data” and wait for its feeder thread to finish
    return results
