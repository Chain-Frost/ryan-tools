# ryan_library/functions/tuflow/tuflow_common.py
from __future__ import annotations
from pathlib import Path
from multiprocessing import Pool
from multiprocessing.pool import MaybeEncodingError
from collections.abc import Iterable, Mapping, Collection
from typing import Any
from loguru import logger

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.loguru_helpers import LoguruMultiprocessingLogger, worker_initializer
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.classes.tuflow_string_classes import TuflowStringParser


def collect_files(
    paths_to_process: Iterable[Path],
    include_data_types: Iterable[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """Return all non-empty files matching ``include_data_types`` underneath ``paths_to_process``."""

    normalized_roots: list[Path] = []
    seen_roots: set[Path] = set()
    for candidate in paths_to_process:
        path: Path = Path(candidate).expanduser()
        try:
            normalized: Path = path.resolve(strict=False)
        except OSError:
            normalized = path.absolute()
        if normalized in seen_roots:
            continue
        seen_roots.add(normalized)
        normalized_roots.append(normalized)

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
    logger.debug("Collecting files with patterns: {}", patterns)
    roots: list[Path] = [p for p in normalized_roots if p.is_dir()]
    logger.debug("Searching in roots: {}", roots)
    invalid_roots: list[Path] = [p for p in normalized_roots if not p.is_dir()]
    for bad_root in invalid_roots:
        logger.warning(f"Skipping non-directory path {bad_root}")

    if not roots:
        logger.warning("No valid directories were supplied for file collection.")
        return []

    files: list[Path] = find_files_parallel(root_dirs=roots, patterns=patterns)
    logger.debug("find_files_parallel found {} files.", len(files))
    filtered_files: list[Path] = []
    seen_files: set[Path] = set()
    for file_path in files:
        if file_path in seen_files:
            continue
        seen_files.add(file_path)
        if not is_non_zero_file(file_path):
            continue
        filtered_files.append(file_path)
    return filtered_files


def _summarize_file_batch(file_list: list[Path]) -> tuple[int, int, Path | None, int]:
    """Return count, total bytes, largest path, largest size for ``file_list``."""
    total_bytes: int = 0
    largest_file: Path | None = None
    largest_bytes: int = 0
    for path in file_list:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        total_bytes += size
        if size > largest_bytes:
            largest_bytes = size
            largest_file = path
    return len(file_list), total_bytes, largest_file, largest_bytes


def _format_bytes(size: int) -> str:
    """Convert ``size`` to a human-readable string."""
    if size <= 0:
        return "0 B"
    units: tuple[str, ...] = ("B", "KB", "MB", "GB", "TB", "PB")
    idx: int = 0
    value: float = float(size)
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    return f"{value:.1f} {units[idx]}"


def _resolve_entity_filter_for_file(
    file_path: Path, entity_filters: Mapping[str, Collection[str]] | Collection[str] | None
) -> Collection[str] | None:
    """Select the appropriate entity filter for a given file based on its data type."""
    if entity_filters is None:
        return None

    if isinstance(entity_filters, Mapping):
        parser = TuflowStringParser(file_path=file_path)
        data_type: str | None = parser.data_type
        if not data_type:
            return None
        return entity_filters.get(data_type) or entity_filters.get(data_type.lower())

    if isinstance(entity_filters, str):
        return [entity_filters]

    return entity_filters


def process_file(
    file_path: Path, entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None
) -> BaseProcessor | None:
    try:
        entity_filter: Collection[str] | None = _resolve_entity_filter_for_file(
            file_path=file_path, entity_filters=entity_filters
        )
        proc: BaseProcessor = BaseProcessor.from_file(file_path=file_path, entity_filter=entity_filter)
        proc.process()
        if proc.validate_data():
            logger.debug(f"Processed {proc.log_path}")
        else:
            logger.warning(f"Validation failed {proc.log_path}")
        proc.discard_raw_dataframe()
        return proc
    except Exception:
        try:
            log_path = str(file_path.resolve().relative_to(Path.cwd().resolve()))
        except ValueError:
            log_path = str(file_path)
        logger.exception(f"Error processing {log_path}")
        return None


def process_files_in_parallel(
    file_list: list[Path],
    log_queue: Any,
    log_level: str = "INFO",
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
) -> ProcessorCollection:
    size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Spawning pool with {size} workers")
    file_count, total_bytes, largest_file, largest_bytes = _summarize_file_batch(file_list=file_list)
    largest_desc: str = f"{largest_file.name} ({_format_bytes(largest_bytes)})" if largest_file else "n/a"
    logger.info(
        "Preparing to process {count} files (~{total} on disk; largest {largest}).",
        count=file_count,
        total=_format_bytes(total_bytes),
        largest=largest_desc,
    )
    dataset_summary: str = f"{file_count} files (~{_format_bytes(total_bytes)} on disk; largest {largest_desc})"
    if size <= 1:
        logger.info("Pool size is 1; processing files sequentially.")
        return _process_files_serially(file_list=file_list, entity_filters=entity_filters)

    try:
        with Pool(processes=size, initializer=worker_initializer, initargs=(log_queue, log_level)) as pool:
            task_args: list[tuple[Path, Mapping[str, Collection[str]] | Collection[str] | None]] = [
                (file_path, entity_filters) for file_path in file_list
            ]
            coll = ProcessorCollection()
            for proc in pool.starmap(func=process_file, iterable=task_args):
                if proc and proc.processed:
                    coll.add_processor(processor=proc)
            return coll
    except MaybeEncodingError as exc:
        logger.warning(
            "Multiprocessing failed to return processor results ({}). Falling back to sequential execution. Dataset footprint: {}",
            exc,
            dataset_summary,
        )
    except OSError as exc:
        logger.warning(
            "Multiprocessing encountered an OSError ({}). Falling back to sequential execution. Dataset footprint: {}",
            exc,
            dataset_summary,
        )
    return _process_files_serially(file_list=file_list, entity_filters=entity_filters)


def _process_files_serially(
    file_list: list[Path],
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
) -> ProcessorCollection:
    logger.info("Processing {} files sequentially.", len(file_list))
    coll = ProcessorCollection()
    for file_path in file_list:
        proc: BaseProcessor | None = process_file(file_path=file_path, entity_filters=entity_filters)
        if proc and proc.processed:
            coll.add_processor(processor=proc)
    return coll


def bulk_read_and_merge_tuflow_csv(
    paths_to_process: list[Path],
    include_data_types: list[str],
    log_queue: LoguruMultiprocessingLogger,
    console_log_level: str = "INFO",
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
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
        file_list=files, log_queue=log_queue, log_level=console_log_level, entity_filters=entity_filters
    )
    # tell the queue “no more data” and wait for its feeder thread to finish
    return results
