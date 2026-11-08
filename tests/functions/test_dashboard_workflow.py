"""Tests for ryan_library.functions.dashboard_workflow."""

# Unit tests intentionally exercise the implementation helpers under test.
# pyright: reportPrivateUsage=false

import queue
from multiprocessing.pool import ApplyResult
from typing import cast

from ryan_library.functions import dashboard_workflow as dw
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowStatus


def dummy_process_item(item: int) -> str:
    if item == 0:
        msg = "Item cannot be zero"
        raise ValueError(msg)
    return f"processed_{item}"


def status_for_result(res: str) -> WorkflowStatus:
    return "OK"


def detail_for_result(res: str) -> str:
    return res


def test_serial_dashboard_workflow() -> None:
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["Task1", "Task2", "Task3"])
    indexed_results: dict[int, str] = {}

    dw._run_serial_dashboard_workflow(
        items=[1, 2, 0],
        process_item=dummy_process_item,
        dashboard=dashboard,
        status_for_result=status_for_result,
        detail_for_result=detail_for_result,
        indexed_results=indexed_results,
    )

    assert indexed_results == {1: "processed_1", 2: "processed_2"}
    assert dashboard._tasks[1].status == "OK"
    assert dashboard._tasks[2].status == "OK"
    assert dashboard._tasks[3].status == "FAIL"


class DummyProgressQueue:
    def __init__(self) -> None:
        self.q: queue.Queue[object] = queue.Queue()

    def put(self, obj: object, block: bool = True, timeout: float | None = None) -> None:
        self.q.put(obj, block=block, timeout=timeout)

    def get_nowait(self) -> object:
        return self.q.get_nowait()


def test_worker_initializer_and_processor() -> None:
    def process_object(item: object) -> str:
        assert isinstance(item, int)
        return dummy_process_item(item)

    q = DummyProgressQueue()
    dw._dashboard_worker_initializer(
        log_queue=None, start_queue=q, worker_log_level="ERROR", process_item=process_object
    )

    req = dw.IndexedWorkflowItem[object](index=5, item=42)
    res = dw._process_indexed_workflow_item(req)

    assert res == "processed_42"
    assert q.get_nowait() == 5


def test_mark_started_dashboard_rows() -> None:
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["T1", "T2"])
    q = DummyProgressQueue()
    q.put(1)
    q.put(2)

    completed = {2}  # 2 is already completed, should not be marked running
    count = dw._mark_started_dashboard_rows(
        dashboard=dashboard, start_queue=q, completed_indexes=completed, max_events=10
    )

    assert count == 1
    assert dashboard._tasks[1].status == "RUNNING"
    assert dashboard._tasks[2].status == "QUEUED"  # Unchanged because it was completed


class DummyApplyResult:
    def __init__(self, ready: bool, value: str | None = None, exception: Exception | None = None) -> None:
        self._ready = ready
        self._value = value
        self._exception = exception

    def ready(self) -> bool:
        return self._ready

    def get(self) -> str:
        if self._exception:
            raise self._exception
        assert self._value is not None
        return self._value


def test_collect_finished_dashboard_results() -> None:
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["T1", "T2", "T3"])

    # The bounded double implements ready/get, the only ApplyResult API used.
    pending = cast(
        "dict[int, ApplyResult[str]]",
        {
            1: DummyApplyResult(True, "processed_1"),
            2: DummyApplyResult(True, exception=ValueError("fail")),
            3: DummyApplyResult(False),  # not ready
        },
    )

    indexed: dict[int, str] = {}
    completed: set[int] = set()

    count = dw._collect_finished_dashboard_results(
        dashboard=dashboard,
        pending_results=pending,
        indexed_results=indexed,
        completed_indexes=completed,
        status_for_result=status_for_result,
        detail_for_result=detail_for_result,
    )

    assert count == 2
    assert 1 in completed
    assert 2 in completed
    assert 3 not in completed
    assert indexed == {1: "processed_1"}

    assert dashboard._tasks[1].status == "OK"
    assert dashboard._tasks[2].status == "FAIL"
    assert dashboard._tasks[3].status == "QUEUED"

    assert len(pending) == 1
    assert 3 in pending


def test_run_dashboard_workflow_serial() -> None:
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["T1", "T2"])

    results = dw.run_dashboard_workflow(
        items=[10, 20],
        process_item=dummy_process_item,
        dashboard=dashboard,
        pool_size=1,  # Serial
        status_for_result=status_for_result,
        detail_for_result=detail_for_result,
    )

    assert results == ["processed_10", "processed_20"]
    assert dashboard._tasks[1].status == "OK"
