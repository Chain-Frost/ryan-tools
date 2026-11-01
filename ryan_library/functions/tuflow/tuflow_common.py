# ryan_library/functions/tuflow/tuflow_common.py
from __future__ import annotations

import multiprocessing.pool as multiprocessing_pool
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import cast

from loguru import logger

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.dashboard_workflow import WorkflowItemOutcome, run_dashboard_workflow_report
from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
)
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowMetric, WorkflowStatus
from ryan_library.functions.loguru_helpers import LogQueue, worker_initializer
from ryan_library.functions.multiprocessing_helpers import calculate_pool_size
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

MaybeEncodingError: type[Exception] = cast(
    "type[Exception]",
    multiprocessing_pool.MaybeEncodingError,  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
)


@dataclass(slots=True)
class FileBatchProgress:
    """Optional, display-independent accounting for unique final file outcomes."""

    scheduled: int = 0
    succeeded: int = 0
    skipped: int = 0
    failed: int = 0

    def reset(self, scheduled: int) -> None:
        """Reset after an infrastructure retry, avoiding duplicate completions."""
        self.scheduled = scheduled
        self.succeeded = self.skipped = self.failed = 0

    def observe(self, status: WorkflowStatus) -> None:
        """Record one terminal outcome, without changing result selection."""
        if status == "OK":
            self.succeeded += 1
        elif status == "SKIP":
            self.skipped += 1
        elif status == "FAIL":
            self.failed += 1

    @property
    def completed(self) -> int:
        return self.succeeded + self.skipped + self.failed


def _result_status(processor: BaseProcessor | None) -> WorkflowStatus:
    """Classify only; adding processors retains the existing collection rules."""
    if processor is None or not processor.processed:
        return "FAIL"
    if processor.df.empty:
        return "SKIP"
    return "OK"


def _dashboard_process_file(
    request: tuple[Path, Mapping[str, Collection[str]] | Collection[str] | None, bool],
) -> BaseProcessor | None:
    """Spawn-importable adapter using the same process_file as the legacy pool."""
    file_path, filters, include_path_columns = request
    return process_file(
        file_path=file_path,
        entity_filters=filters,
        include_path_columns=include_path_columns,
    )


def _dashboard_status(processor: BaseProcessor | None) -> WorkflowStatus:
    return _result_status(processor)


def _dashboard_detail(processor: BaseProcessor | None) -> str:
    status = _result_status(processor)
    return {"OK": "processed", "SKIP": "empty result", "FAIL": "processing failed"}[status]


def _record_file_outcome(
    *,
    status: WorkflowStatus,
    progress: FileBatchProgress | None,
    dashboard: LiveWorkflowDashboard | None,
) -> None:
    if progress is not None:
        progress.observe(status)
        if dashboard is not None:
            dashboard.set_metrics(
                [
                    WorkflowMetric(
                        label="Files finished", value=progress.completed, total=progress.scheduled, unit="files"
                    )
                ]
            )


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
        normalized: Path = path.absolute()
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
        mapped_filters: Mapping[str, Collection[str]] = cast("Mapping[str, Collection[str]]", entity_filters)
        parser = TuflowStringParser(file_path=file_path)
        data_type: str | None = parser.data_type
        if not data_type:
            return None
        return mapped_filters.get(data_type) or mapped_filters.get(data_type.lower())

    if isinstance(entity_filters, str):
        return [entity_filters]

    return entity_filters


def process_file(
    file_path: Path,
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
    include_path_columns: bool = True,
) -> BaseProcessor | None:
    try:
        entity_filter: Collection[str] | None = _resolve_entity_filter_for_file(
            file_path=file_path, entity_filters=entity_filters
        )
        proc: BaseProcessor = BaseProcessor.from_file(
            file_path=file_path,
            entity_filter=entity_filter,
            include_path_columns=include_path_columns,
        )
        proc.process()
        if proc.validate_data():
            logger.debug("Processed {}", proc.log_path)
        else:
            logger.warning(f"Validation failed {proc.log_path}")
        proc.discard_raw_dataframe()
        return proc
    except Exception:
        try:
            log_path = str(file_path.absolute().relative_to(Path.cwd().absolute()))
        except ValueError:
            log_path = str(file_path)
        logger.exception(f"Error processing {log_path}")
        return None


def process_files_in_parallel(
    file_list: list[Path],
    log_queue: LogQueue,
    log_level: str = "INFO",
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
    *,
    include_path_columns: bool = True,
    dashboard: LiveWorkflowDashboard | None = None,
    batch_progress: FileBatchProgress | None = None,
) -> ProcessorCollection:
    # log_level is retained for caller compatibility. The queue carries
    # the lowest capture level required by its independently filtered sinks.
    del log_level
    size: int = calculate_pool_size(num_files=len(file_list))
    logger.info("Spawning pool with {} workers", size)
    file_count, total_bytes, largest_file, largest_bytes = _summarize_file_batch(file_list=file_list)
    largest_desc: str = f"{largest_file.name} ({_format_bytes(largest_bytes)})" if largest_file else "n/a"
    logger.info(
        "Preparing to process {} files (~{} on disk; largest {}).",
        file_count,
        _format_bytes(total_bytes),
        largest_desc,
    )
    dataset_summary: str = f"{file_count} files (~{_format_bytes(total_bytes)} on disk; largest {largest_desc})"

    progress = (
        batch_progress if batch_progress is not None else (FileBatchProgress() if dashboard is not None else None)
    )
    if progress is not None:
        progress.reset(scheduled=len(file_list))
    if dashboard is not None:
        dashboard.set_tasks(labels=[path.name for path in file_list])
        dashboard.set_metrics([WorkflowMetric(label="Files finished", value=0, total=len(file_list), unit="files")])

    if size <= 1:
        logger.info("Pool size is 1; processing files sequentially.")
        return _process_files_serially(
            file_list=file_list,
            entity_filters=entity_filters,
            include_path_columns=include_path_columns,
            dashboard=dashboard,
            batch_progress=progress,
        )

    try:
        if dashboard is not None:
            # Reuse the foundation adapter for incremental results and worker
            # diagnostics. File content is not read again for presentation.
            requests = [(path, entity_filters, include_path_columns) for path in file_list]

            def on_outcome(outcome: WorkflowItemOutcome[BaseProcessor | None]) -> None:
                _record_file_outcome(status=outcome.status, progress=progress, dashboard=dashboard)

            report = run_dashboard_workflow_report(
                items=requests,
                process_item=_dashboard_process_file,
                dashboard=dashboard,
                pool_size=size,
                status_for_result=_dashboard_status,
                detail_for_result=_dashboard_detail,
                log_queue=log_queue,
                on_outcome=on_outcome,
            )
            coll = ProcessorCollection()
            for proc in report.results:
                if proc and proc.processed:
                    coll.add_processor(processor=proc)
            return coll

        # Preserve the established starmap path when the display is disabled.
        with Pool(processes=size, initializer=worker_initializer, initargs=(log_queue,)) as pool:
            task_args: list[tuple[Path, Mapping[str, Collection[str]] | Collection[str] | None, bool]] = [
                (file_path, entity_filters, include_path_columns) for file_path in file_list
            ]
            coll = ProcessorCollection()
            for proc in pool.starmap(func=process_file, iterable=task_args):
                if progress is not None:
                    _record_file_outcome(status=_result_status(proc), progress=progress, dashboard=None)
                if proc and proc.processed:
                    coll.add_processor(processor=proc)
            return coll
    except (MaybeEncodingError, OSError) as exc:
        logger.warning(
            "Multiprocessing could not return results ({}). Falling back to sequential execution. Dataset footprint: {}",
            exc,
            dataset_summary,
        )

    # The complete original input list is retried, just as before. Reset
    # display counters and rows so partially returned attempts are not counted.
    if progress is not None:
        progress.reset(scheduled=len(file_list))
    if dashboard is not None:
        dashboard.set_phase("Processing (serial fallback)")
        dashboard.set_tasks(labels=[path.name for path in file_list])
        dashboard.set_metrics([WorkflowMetric(label="Files finished", value=0, total=len(file_list), unit="files")])
    return _process_files_serially(
        file_list=file_list,
        entity_filters=entity_filters,
        include_path_columns=include_path_columns,
        dashboard=dashboard,
        batch_progress=progress,
    )


def _process_files_serially(
    file_list: list[Path],
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
    *,
    include_path_columns: bool = True,
    dashboard: LiveWorkflowDashboard | None = None,
    batch_progress: FileBatchProgress | None = None,
) -> ProcessorCollection:
    logger.info("Processing {} files sequentially.", len(file_list))
    coll = ProcessorCollection()
    for index, file_path in enumerate(file_list, start=1):
        if dashboard is not None:
            dashboard.mark_running(index=index)
        proc: BaseProcessor | None = process_file(
            file_path=file_path,
            entity_filters=entity_filters,
            include_path_columns=include_path_columns,
        )
        status = _result_status(proc)
        _record_file_outcome(status=status, progress=batch_progress, dashboard=dashboard)
        if dashboard is not None:
            dashboard.mark_finished(index=index, status=status, detail=_dashboard_detail(proc))
        if proc and proc.processed:
            coll.add_processor(processor=proc)
    return coll


def bulk_read_and_merge_tuflow_csv(
    paths_to_process: list[Path],
    include_data_types: list[str],
    log_queue: LogQueue,
    console_log_level: str = "INFO",
    entity_filters: Mapping[str, Collection[str]] | Collection[str] | None = None,
    *,
    include_path_columns: bool = True,
    dashboard: LiveWorkflowDashboard | None = None,
    batch_progress: FileBatchProgress | None = None,
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

    if dashboard is not None:
        dashboard.set_phase("Processing")
    results: ProcessorCollection = process_files_in_parallel(
        file_list=files,
        log_queue=log_queue,
        log_level=console_log_level,
        entity_filters=entity_filters,
        include_path_columns=include_path_columns,
        dashboard=dashboard,
        batch_progress=batch_progress,
    )
    # tell the queue “no more data” and wait for its feeder thread to finish
    return results
