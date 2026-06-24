"""Execution helper that feeds work progress into :mod:`live_dashboard`.

``live_dashboard.py`` renders task state. This module owns the generic execution
plumbing around that renderer: serial processing, multiprocessing pools, worker
start notifications, result collection, and dashboard state updates. Wrappers
provide domain-specific pieces: items to process, a worker function, and small
adapters that turn each result into a dashboard status and detail string.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from multiprocessing import Pool, Queue
from multiprocessing.pool import ApplyResult
from queue import Empty
import time
from typing import Generic, Protocol, TypeVar, cast

from loguru import logger

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowStatus
from ryan_library.functions.loguru_helpers import LogQueue, worker_initializer

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


class ProgressQueue(Protocol):
    """Minimal queue shape needed for worker-start progress events."""

    def put(self, item: object, block: bool = True, timeout: float | None = None) -> None: ...

    def get_nowait(self) -> object: ...


@dataclass(slots=True, frozen=True)
class IndexedWorkflowItem(Generic[TItem]):
    """One processing request plus its one-based dashboard row index."""

    index: int
    item: TItem


_workflow_start_queue: ProgressQueue | None = None
_workflow_processor: Callable[[object], object] | None = None


def run_dashboard_workflow(
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    pool_size: int,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    log_queue: LogQueue | None = None,
    worker_log_level: str = "ERROR",
    max_start_events: int | None = None,
    poll_interval: float = 0.1,
) -> list[TResult]:
    """Process items and keep a :class:`LiveWorkflowDashboard` in sync.

    The dashboard must already have its task labels and metadata configured with
    one task per item. Results are returned in the same order as *items*.

    ``process_item`` should contain the real work for one item. The status and
    detail adapters keep this runner generic: LogSummary can map empty
    DataFrames to ``SKIP``, while another wrapper could map return codes,
    validation failures, or copied-file counts to different statuses.
    """
    indexed_results: dict[int, TResult] = {}
    effective_pool_size: int = max(pool_size, 1)
    with dashboard:
        if effective_pool_size <= 1:
            _run_serial_dashboard_workflow(
                items=items,
                process_item=process_item,
                dashboard=dashboard,
                status_for_result=status_for_result,
                detail_for_result=detail_for_result,
                indexed_results=indexed_results,
            )
        else:
            _run_parallel_dashboard_workflow(
                items=items,
                process_item=process_item,
                dashboard=dashboard,
                pool_size=effective_pool_size,
                status_for_result=status_for_result,
                detail_for_result=detail_for_result,
                indexed_results=indexed_results,
                log_queue=log_queue,
                worker_log_level=worker_log_level,
                max_start_events=max_start_events,
                poll_interval=poll_interval,
            )
    return [indexed_results[index] for index in sorted(indexed_results)]


def _run_serial_dashboard_workflow(
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    indexed_results: dict[int, TResult],
) -> None:
    """Run work in-process, updating the dashboard around each call."""
    for index, item in enumerate(items, start=1):
        dashboard.mark_running(index=index)
        try:
            result: TResult = process_item(item)
        except Exception as exc:
            logger.exception(f"Error processing workflow item index {index}")
            dashboard.mark_finished(index=index, status="FAIL", detail=str(exc))
            continue
        indexed_results[index] = result
        dashboard.mark_finished(
            index=index,
            status=status_for_result(result),
            detail=detail_for_result(result),
        )


def _run_parallel_dashboard_workflow(
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    pool_size: int,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    indexed_results: dict[int, TResult],
    log_queue: LogQueue | None,
    worker_log_level: str,
    max_start_events: int | None,
    poll_interval: float,
) -> None:
    """Run work in a process pool and poll async results for dashboard updates."""
    completed_indexes: set[int] = set()
    start_queue: ProgressQueue = cast(ProgressQueue, Queue())
    event_limit: int = max_start_events if max_start_events is not None else max(pool_size * 2, dashboard.max_rows)

    with Pool(
        processes=pool_size,
        initializer=_dashboard_worker_initializer,
        initargs=(
            log_queue,
            start_queue,
            worker_log_level,
            cast(Callable[[object], object], process_item),
        ),
    ) as pool:
        pending_results: dict[int, ApplyResult[TResult]] = {}
        for index, item in enumerate(items, start=1):
            request: IndexedWorkflowItem[object] = IndexedWorkflowItem(index=index, item=item)
            pending_results[index] = cast(
                ApplyResult[TResult],
                pool.apply_async(_process_indexed_workflow_item, args=(request,)),
            )

        try:
            while pending_results:
                finished_count: int = _collect_finished_dashboard_results(
                    dashboard=dashboard,
                    pending_results=pending_results,
                    indexed_results=indexed_results,
                    completed_indexes=completed_indexes,
                    status_for_result=status_for_result,
                    detail_for_result=detail_for_result,
                )
                started_count: int = _mark_started_dashboard_rows(
                    dashboard=dashboard,
                    start_queue=start_queue,
                    completed_indexes=completed_indexes,
                    max_events=event_limit,
                )
                dashboard.set_active_count(
                    count=min(pool_size, len(pending_results)),
                    refresh=False,
                )
                dashboard.refresh(force=finished_count > 0 or started_count > 0)
                time.sleep(poll_interval)
        except Exception:
            logger.exception("Error during dashboard workflow multiprocessing")
            dashboard.set_active_count(count=0)


def _dashboard_worker_initializer(
    log_queue: LogQueue | None,
    start_queue: ProgressQueue,
    worker_log_level: str,
    process_item: Callable[[object], object],
) -> None:
    """Initialise each child process with logging and progress callbacks."""
    global _workflow_processor, _workflow_start_queue
    _workflow_start_queue = start_queue
    _workflow_processor = process_item
    if log_queue is not None:
        worker_initializer(queue=log_queue, level=worker_log_level)


def _process_indexed_workflow_item(request: IndexedWorkflowItem[object]) -> object:
    """Worker entry point used by ``Pool.apply_async``."""
    if _workflow_processor is None:
        raise RuntimeError("Dashboard workflow worker was not initialised")
    if _workflow_start_queue is not None:
        _workflow_start_queue.put(request.index)
    return _workflow_processor(request.item)


def _mark_started_dashboard_rows(
    *,
    dashboard: LiveWorkflowDashboard,
    start_queue: ProgressQueue,
    completed_indexes: set[int],
    max_events: int,
) -> int:
    """Drain worker-start events and mark matching dashboard rows as running."""
    started_count: int = 0
    for _ in range(max(max_events, 1)):
        try:
            raw_index: object = start_queue.get_nowait()
        except Empty:
            return started_count
        if isinstance(raw_index, int) and raw_index not in completed_indexes:
            dashboard.mark_running(index=raw_index, refresh=False)
            started_count += 1
    return started_count


def _collect_finished_dashboard_results(
    *,
    dashboard: LiveWorkflowDashboard,
    pending_results: dict[int, ApplyResult[TResult]],
    indexed_results: dict[int, TResult],
    completed_indexes: set[int],
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
) -> int:
    """Collect completed async results and mark matching dashboard rows finished."""
    finished_count: int = 0
    for index, async_result in list(pending_results.items()):
        if not async_result.ready():
            continue
        try:
            result: TResult = async_result.get()
        except Exception as exc:
            logger.exception(f"Error processing workflow item index {index}")
            completed_indexes.add(index)
            dashboard.mark_finished(index=index, status="FAIL", detail=str(exc), refresh=False)
        else:
            indexed_results[index] = result
            completed_indexes.add(index)
            dashboard.mark_finished(
                index=index,
                status=status_for_result(result),
                detail=detail_for_result(result),
                refresh=False,
            )
        finally:
            pending_results.pop(index, None)
            finished_count += 1
    return finished_count
