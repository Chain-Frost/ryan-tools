# ryan_library/functions/dashboard_workflow.py
"""Execution helper that feeds work progress into :mod:`live_dashboard`.

``live_dashboard.py`` renders task state. This module owns the generic execution
plumbing around that renderer: serial processing, multiprocessing pools, worker
start notifications, result collection, and dashboard state updates. Wrappers
provide domain-specific pieces: items to process, a worker function, and small
adapters that turn each result into a dashboard status and detail string.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from multiprocessing import Pool, Queue
from multiprocessing.pool import ApplyResult
from queue import Empty
from typing import Protocol, cast

from loguru import logger

from .live_dashboard import LiveWorkflowDashboard, WorkflowStatus
from .loguru_helpers import LogQueue, normalize_log_level, worker_initializer


class ProgressQueue(Protocol):
    """Minimal queue shape needed for worker-start progress events."""

    def put(self, item: object, block: bool = True, timeout: float | None = None) -> None: ...

    def get_nowait(self) -> object: ...


@dataclass(slots=True, frozen=True)
class IndexedWorkflowItem[TItem]:
    """One processing request plus its one-based dashboard row index."""

    index: int
    item: TItem


_workflow_start_queue: ProgressQueue | None = None
_workflow_processor: Callable[[object], object] | None = None



@dataclass(slots=True, frozen=True)
class WorkflowItemOutcome[TResult]:
    """One-based input index and terminal outcome, including failed requests.

    A successful worker may legitimately return None; has_result distinguishes
    that result from an exception before a result could be produced.
    """

    index: int
    status: WorkflowStatus
    result: TResult | None = None
    has_result: bool = False
    error: Exception | None = None


@dataclass(slots=True, frozen=True)
class WorkflowRunReport[TResult]:
    """Ordered per-input outcomes; infrastructure failures are raised, not hidden."""

    outcomes: tuple[WorkflowItemOutcome[TResult], ...]

    @property
    def results(self) -> list[TResult]:
        """All returned results (including domain FAIL/SKIP), in input order."""
        return [cast("TResult", item.result) for item in self.outcomes if item.has_result]

    @property
    def failures(self) -> tuple[WorkflowItemOutcome[TResult], ...]:
        return tuple(item for item in self.outcomes if item.status == "FAIL")


def run_dashboard_workflow[TItem, TResult](
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    pool_size: int,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    log_queue: LogQueue | None = None,
    worker_log_level: str | None = None,
    max_start_events: int | None = None,
    poll_interval: float = 0.1,
    on_outcome: Callable[[WorkflowItemOutcome[TResult]], None] | None = None,
) -> list[TResult]:
    """Compatibility list API; return values in input order, never silently lose exceptions.

    A returned domain result with status FAIL remains in the list, as before.
    An unexpected per-item exception is re-raised in input order after all
    completed work (so existing OSError/MaybeEncodingError fallback can run).
    Use run_dashboard_workflow_report for explicit partial-results accounting.
    """
    report = run_dashboard_workflow_report(
        items=items,
        process_item=process_item,
        dashboard=dashboard,
        pool_size=pool_size,
        status_for_result=status_for_result,
        detail_for_result=detail_for_result,
        log_queue=log_queue,
        worker_log_level=worker_log_level,
        max_start_events=max_start_events,
        poll_interval=poll_interval,
        on_outcome=on_outcome,
    )
    for outcome in report.outcomes:
        if outcome.error is not None:
            raise outcome.error
    return report.results


def run_dashboard_workflow_report[TItem, TResult](
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    pool_size: int,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    log_queue: LogQueue | None = None,
    worker_log_level: str | None = None,
    max_start_events: int | None = None,
    poll_interval: float = 0.1,
    on_outcome: Callable[[WorkflowItemOutcome[TResult]], None] | None = None,
) -> WorkflowRunReport[TResult]:
    """Run a serial/spawn-safe batch and return explicit ordered item outcomes.

    The caller owns labels/metadata and phase/aggregate metrics. Only one
    parent renders; workers send one integer start event per item. A worker's
    Loguru capture level defaults to the least restrictive configured sink,
    never to the dashboard's visual level. Unexpected worker exceptions are
    represented by FAIL outcomes; pool, transport, callback and interruption
    errors propagate after resource cleanup. An on_outcome callback runs only
    in the parent, once per completed item, and may update cheap metrics.
    """
    if isinstance(pool_size, bool) or not isinstance(pool_size, int) or pool_size < 1:
        msg = "pool_size must be a positive integer."
        raise ValueError(msg)
    if (
        isinstance(poll_interval, bool)
        or not isinstance(poll_interval, (int, float))
        or not math.isfinite(poll_interval)
        or poll_interval <= 0
    ):
        msg = "poll_interval must be a finite number greater than zero."
        raise ValueError(msg)
    if max_start_events is not None and (
        isinstance(max_start_events, bool) or not isinstance(max_start_events, int) or max_start_events < 1
    ):
        msg = "max_start_events must be a positive integer or None."
        raise ValueError(msg)
    if worker_log_level is not None:
        effective_level = normalize_log_level(worker_log_level)
        if log_queue is not None and logger.level(effective_level).no > logger.level(log_queue.capture_log_level).no:
            msg = "worker_log_level would discard records required by the configured logging sinks."
            raise ValueError(msg)

    indexed_results: dict[int, TResult] = {}
    indexed_outcomes: dict[int, WorkflowItemOutcome[TResult]] = {}
    with dashboard:
        if items:
            if pool_size == 1:
                _run_serial_dashboard_workflow(
                    items=items,
                    process_item=process_item,
                    dashboard=dashboard,
                    status_for_result=status_for_result,
                    detail_for_result=detail_for_result,
                    indexed_results=indexed_results,
                    indexed_outcomes=indexed_outcomes,
                    on_outcome=on_outcome,
                )
            else:
                _run_parallel_dashboard_workflow(
                    items=items,
                    process_item=process_item,
                    dashboard=dashboard,
                    pool_size=pool_size,
                    status_for_result=status_for_result,
                    detail_for_result=detail_for_result,
                    indexed_results=indexed_results,
                    indexed_outcomes=indexed_outcomes,
                    log_queue=log_queue,
                    worker_log_level=worker_log_level,
                    max_start_events=max_start_events,
                    poll_interval=poll_interval,
                    on_outcome=on_outcome,
                )
    if len(indexed_outcomes) != len(items):
        msg = f"Dashboard workflow lost {len(items) - len(indexed_outcomes)} item outcome(s); results are incomplete."
        raise RuntimeError(msg)
    return WorkflowRunReport(outcomes=tuple(indexed_outcomes[index] for index in range(1, len(items) + 1)))


def _run_serial_dashboard_workflow[TItem, TResult](
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    indexed_results: dict[int, TResult],
    indexed_outcomes: dict[int, WorkflowItemOutcome[TResult]] | None = None,
    on_outcome: Callable[[WorkflowItemOutcome[TResult]], None] | None = None,
) -> None:
    """Run work in-process, updating the dashboard around each call."""
    for index, item in enumerate(items, start=1):
        dashboard.mark_running(index=index)
        try:
            result: TResult = process_item(item)
            status = _terminal_status(status_for_result(result))
            detail = detail_for_result(result)
        except Exception as exc:
            logger.exception("Error processing workflow item index {}", index)
            dashboard.mark_finished(index=index, status="FAIL", detail=str(exc))
            outcome = WorkflowItemOutcome[TResult](index=index, status="FAIL", error=exc)
        else:
            indexed_results[index] = result
            dashboard.mark_finished(index=index, status=status, detail=detail)
            outcome = WorkflowItemOutcome(index=index, status=status, result=result, has_result=True)
        if indexed_outcomes is not None:
            indexed_outcomes[index] = outcome
        if on_outcome is not None:
            on_outcome(outcome)


def _run_parallel_dashboard_workflow[TItem, TResult](
    *,
    items: Sequence[TItem],
    process_item: Callable[[TItem], TResult],
    dashboard: LiveWorkflowDashboard,
    pool_size: int,
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    indexed_results: dict[int, TResult],
    log_queue: LogQueue | None,
    worker_log_level: str | None,
    max_start_events: int | None,
    poll_interval: float,
    indexed_outcomes: dict[int, WorkflowItemOutcome[TResult]] | None = None,
    on_outcome: Callable[[WorkflowItemOutcome[TResult]], None] | None = None,
) -> None:
    """Run work in a process pool and poll async results for dashboard updates."""
    completed_indexes: set[int] = set()
    raw_queue = Queue()
    start_queue: ProgressQueue = cast("ProgressQueue", raw_queue)
    event_limit: int = max_start_events if max_start_events is not None else max(pool_size * 2, dashboard.max_rows)

    try:
        with Pool(
            processes=pool_size,
            initializer=_dashboard_worker_initializer,
            initargs=(
                log_queue,
                start_queue,
                worker_log_level,
                cast("Callable[[object], object]", process_item),
            ),
        ) as pool:
            pending_results: dict[int, ApplyResult[TResult]] = {}
            for index, item in enumerate(items, start=1):
                request: IndexedWorkflowItem[object] = IndexedWorkflowItem(index=index, item=item)
                pending_results[index] = cast(
                    "ApplyResult[TResult]",
                    pool.apply_async(_process_indexed_workflow_item, args=(request,)),
                )

            while pending_results:
                _collect_finished_dashboard_results(
                    dashboard=dashboard,
                    pending_results=pending_results,
                    indexed_results=indexed_results,
                    completed_indexes=completed_indexes,
                    status_for_result=status_for_result,
                    detail_for_result=detail_for_result,
                    indexed_outcomes=indexed_outcomes,
                    on_outcome=on_outcome,
                )
                _mark_started_dashboard_rows(
                    dashboard=dashboard,
                    start_queue=start_queue,
                    completed_indexes=completed_indexes,
                    max_events=event_limit,
                )
                # Coalesce routine refreshes; Rich has its own bounded cadence.
                dashboard.refresh()
                if pending_results:
                    time.sleep(poll_interval)
            # Graceful normal completion allows workers to flush queued log records.
            # Pool.__exit__ otherwise terminates workers even after results arrive.
            pool.close()
            pool.join()
    except Exception:
        logger.exception("Error during dashboard workflow multiprocessing")
        raise
    finally:
        dashboard.set_active_count(count=0, refresh=False)
        raw_queue.close()
        raw_queue.join_thread()


def _dashboard_worker_initializer(
    log_queue: LogQueue | None,
    start_queue: ProgressQueue,
    worker_log_level: str | None,
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
        msg = "Dashboard workflow worker was not initialised"
        raise RuntimeError(msg)
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


def _collect_finished_dashboard_results[TResult](
    *,
    dashboard: LiveWorkflowDashboard,
    pending_results: dict[int, ApplyResult[TResult]],
    indexed_results: dict[int, TResult],
    completed_indexes: set[int],
    status_for_result: Callable[[TResult], WorkflowStatus],
    detail_for_result: Callable[[TResult], str],
    indexed_outcomes: dict[int, WorkflowItemOutcome[TResult]] | None = None,
    on_outcome: Callable[[WorkflowItemOutcome[TResult]], None] | None = None,
) -> int:
    """Collect completed async results and mark matching dashboard rows finished."""
    finished_count: int = 0
    for index, async_result in list(pending_results.items()):
        if not async_result.ready():
            continue
        try:
            result: TResult = async_result.get()
            status = _terminal_status(status_for_result(result))
            detail = detail_for_result(result)
        except Exception as exc:
            logger.exception("Error processing workflow item index {}", index)
            dashboard.mark_finished(index=index, status="FAIL", detail=str(exc), refresh=False)
            outcome = WorkflowItemOutcome[TResult](index=index, status="FAIL", error=exc)
        else:
            indexed_results[index] = result
            dashboard.mark_finished(index=index, status=status, detail=detail, refresh=False)
            outcome = WorkflowItemOutcome(index=index, status=status, result=result, has_result=True)
        completed_indexes.add(index)
        pending_results.pop(index, None)
        finished_count += 1
        if indexed_outcomes is not None:
            indexed_outcomes[index] = outcome
        if on_outcome is not None:
            on_outcome(outcome)
    return finished_count


def _terminal_status(status: WorkflowStatus) -> WorkflowStatus:
    if status not in ("OK", "SKIP", "FAIL"):
        msg = f"status_for_result must return OK, SKIP or FAIL, received {status!r}."
        raise ValueError(msg)
    return status
