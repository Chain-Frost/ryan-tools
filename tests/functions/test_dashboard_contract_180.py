# pyright: reportPrivateUsage=false
# focused tests inspect internal state and pool collection.
"""Regression coverage for the shared dashboard contract in issue #180."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from io import StringIO
from multiprocessing import pool as multiprocessing_pool
from multiprocessing.pool import ApplyResult
from typing import cast
from unittest.mock import patch

import pytest
from rich.console import Console
from rich.text import Text

from ryan_library.functions import dashboard_workflow as dw
from ryan_library.functions import live_dashboard as ld
from ryan_library.functions.dashboard_workflow import run_dashboard_workflow, run_dashboard_workflow_report
from ryan_library.functions.live_dashboard import (
    LiveWorkflowDashboard,
    WorkflowMetric,
    validate_dashboard_settings,
)


def _worker(item: int) -> int | None:
    if item == 2:
        msg = "synthetic recoverable error"
        raise OSError(msg)
    return None if item == 3 else item * 10


def _status(value: int | None) -> str:
    return "SKIP" if value is None else "OK"


def _detail(value: int | None) -> str:
    return "empty" if value is None else str(value)


def test_optional_metrics_unknown_zero_late_totals_and_alternatives() -> None:
    d = LiveWorkflowDashboard(title="Metrics", enabled=False)
    d.set_tasks(["tile A"], metadata=[{"tile": "A", "unavailable": None}])
    assert d._tasks[1].metadata == {"tile": "A"}
    assert d._format_extra_metrics() == ""
    d.set_metrics(
        [
            WorkflowMetric("Tiles", value=0, unit="tiles", total=None),
            WorkflowMetric("Cells", value=None, unit="cells"),
        ]
    )
    assert "Tiles 0 tiles" in d._format_extra_metrics()
    assert "Cells ? cells" in d._format_extra_metrics()
    assert "0/" not in d._format_extra_metrics()
    d.set_metrics([WorkflowMetric("Tiles", value=2, total=5, unit="tiles", metadata="unique completions")])
    assert d._format_extra_metrics() == "Tiles 2/5 tiles (unique completions)"
    d.set_metrics([])
    assert d._format_extra_metrics() == ""
    d.set_extra_metrics({"Workers": 0})
    assert d._format_extra_metrics() == "Workers 0"


@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf"), -float("inf")])
def test_invalid_refresh_rejected(value: float) -> None:
    with pytest.raises(ValueError, match="live_refresh_per_second"):
        validate_dashboard_settings(refresh_per_second=value, max_rows=3)


@pytest.mark.parametrize("rows", [0, -1, True, 1.4])
def test_invalid_row_count_rejected(rows: object) -> None:
    with pytest.raises(ValueError, match="live_max_rows"):
        validate_dashboard_settings(refresh_per_second=2.0, max_rows=rows)  # type: ignore[arg-type]


def test_invalid_metric_and_other_configuration() -> None:
    with pytest.raises(ValueError, match="finite"):
        WorkflowMetric("Rows", value=math.nan)
    with pytest.raises(ValueError, match="non-negative"):
        WorkflowMetric("Rows", total=-1)
    with pytest.raises(ValueError, match="non-empty"):
        WorkflowMetric("")
    d = LiveWorkflowDashboard(title="x", enabled=False)
    with pytest.raises(ValueError, match="non-negative"):
        d.set_active_count(-1)
    with pytest.raises(ValueError, match="non-empty"):
        d.set_phase(" ")


def test_phase_transitions_force_refresh_while_metrics_remain_throttled() -> None:
    """Long blocking exports must display their phase before the next tick."""
    dashboard = LiveWorkflowDashboard(title="Phases", enabled=False)
    with patch.object(dashboard, "refresh") as refresh:
        dashboard.set_phase("Exporting")
        refresh.assert_called_once_with(force=True)
        refresh.reset_mock()
        dashboard.set_metrics([WorkflowMetric("Rows", value=1)])
        refresh.assert_called_once_with()


def test_bounded_history_and_plain_output(capsys: pytest.CaptureFixture[str]) -> None:
    d = LiveWorkflowDashboard(title="Batch", enabled=True, max_rows=4)
    d.console = Console(force_terminal=False)
    d.set_tasks([f"item {i}" for i in range(100)])
    with d:
        assert d._live is None
        d.set_phase("Exporting")
        for i in range(1, 101):
            d.mark_finished(index=i, status="OK")
        assert len(d._completed_order) == 4
        assert len(d._visible_tasks()) <= 4
        d.finish(outcome="SUCCESS", detail="output written")
        d.finish(outcome="SUCCESS", detail="should not repeat")
    text = capsys.readouterr().out
    assert text.count("Batch: SUCCESS") == 1
    assert "OK 100" in text
    assert "output written" in text
    assert "\x1b[" not in text


def test_nested_serial_work_renders_running_before_blocking_and_keeps_outer_display(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A phase refresh must not hide serial work or close the export display."""
    stream = StringIO()
    monkeypatch.setenv("TERM", "xterm-256color")
    dashboard = LiveWorkflowDashboard(title="Nested workflow", group_by="model")
    dashboard.console = Console(file=stream, force_terminal=True, legacy_windows=False, width=120, height=30)
    dashboard.set_tasks(["result.csv"], metadata=[{"model": "model.tcf"}])

    with patch.object(ld.time, "monotonic", return_value=1.0), dashboard:
        outer_live = dashboard._live
        assert outer_live is not None
        assert outer_live._refresh_thread is None
        dashboard.set_phase("Processing")
        before_work = stream.tell()
        observed_frames: list[str] = []
        reused_live: list[bool] = []

        def process(item: int) -> int:
            observed_frames.append(Text.from_ansi(stream.getvalue()[before_work:]).plain)
            reused_live.append(dashboard._live is outer_live)
            return item * 2

        report = run_dashboard_workflow_report(
            items=[3],
            process_item=process,
            dashboard=dashboard,
            pool_size=1,
            status_for_result=lambda _value: "OK",
            detail_for_result=str,
        )
        assert report.results == [6]
        assert "RUNNING" in observed_frames[0]
        assert "active 1" in observed_frames[0]
        assert "model.tcf" in observed_frames[0]
        assert reused_live == [True]
        assert dashboard._live is outer_live
        assert "finished 1/1" in Text.from_ansi(stream.getvalue()[before_work:]).plain
        before_export = stream.tell()
        dashboard.set_phase("Exporting")
        assert "Exporting" in stream.getvalue()[before_export:]
    assert dashboard._live is None


def test_task_only_consumers_do_not_display_an_unset_queued_phase() -> None:
    """Existing runner/raster callers can rely on task states without phases."""
    dashboard = LiveWorkflowDashboard(title="Task states", enabled=False)
    dashboard.set_tasks(["model.tcf"])
    dashboard.mark_running(index=1)
    stream = StringIO()
    console = Console(file=stream, force_terminal=False, width=120)
    console.print(dashboard._render())
    assert "RUNNING" in stream.getvalue()
    assert "Queued" not in stream.getvalue()
    dashboard.set_phase("Processing")
    console.print(dashboard._render())
    assert "Processing" in stream.getvalue()


def test_report_distinguishes_fail_skip_and_returned_none() -> None:
    d = LiveWorkflowDashboard(title="Results", enabled=False)
    d.set_tasks(["first", "failed", "skipped"])
    callbacks: list[tuple[int, str]] = []
    report = run_dashboard_workflow_report(
        items=[1, 2, 3],
        process_item=_worker,
        dashboard=d,
        pool_size=1,
        status_for_result=_status,  # type: ignore[arg-type]
        detail_for_result=_detail,
        on_outcome=lambda o: callbacks.append((o.index, o.status)),
    )
    assert callbacks == [(1, "OK"), (2, "FAIL"), (3, "SKIP")]
    assert [o.index for o in report.outcomes] == [1, 2, 3]
    assert report.results == [10, None]
    assert report.outcomes[1].error is not None
    assert report.outcomes[2].has_result
    assert report.outcomes[2].result is None
    assert len(report.failures) == 1
    assert d._tasks[2].status == "FAIL"

    d2 = LiveWorkflowDashboard(title="No dashboard", enabled=False)
    d2.set_tasks(["first", "failed", "skipped"])
    with pytest.raises(OSError, match="recoverable"):
        run_dashboard_workflow(
            items=[1, 2, 3],
            process_item=_worker,
            dashboard=d2,
            pool_size=1,
            status_for_result=_status,  # type: ignore[arg-type]
            detail_for_result=_detail,
        )


def test_status_validation_and_empty_batch() -> None:
    d = LiveWorkflowDashboard(title="Empty", enabled=False)
    d.set_tasks([])
    report = run_dashboard_workflow_report(
        items=[],
        process_item=_worker,
        dashboard=d,
        pool_size=2,
        status_for_result=_status,  # type: ignore[arg-type]
        detail_for_result=_detail,
    )
    assert report.outcomes == ()
    assert report.results == []

    with pytest.raises(ValueError, match="pool_size"):
        run_dashboard_workflow_report(
            items=[1],
            process_item=_worker,
            dashboard=d,
            pool_size=0,
            status_for_result=_status,  # type: ignore[arg-type]
            detail_for_result=_detail,
        )


def _interrupt_worker(_item: int) -> int:
    raise KeyboardInterrupt


def test_serial_interrupt_releases_dashboard_context() -> None:
    dashboard = LiveWorkflowDashboard(title="Interrupted", enabled=False)
    dashboard.set_tasks(["running", "pending"])
    with pytest.raises(KeyboardInterrupt):
        run_dashboard_workflow_report(
            items=[1, 2],
            process_item=_interrupt_worker,
            dashboard=dashboard,
            pool_size=1,
            status_for_result=lambda _result: "OK",
            detail_for_result=str,
        )
    assert dashboard._context_depth == 0
    assert dashboard._live is None
    assert dashboard._tasks[2].status == "QUEUED"


def test_many_small_updates_keep_history_bounded(
    record_property: Callable[[str, object], None],
) -> None:
    """Measure lightweight updates without asserting host-dependent throughput."""
    count = 5000
    dashboard = LiveWorkflowDashboard(title="Large batch", enabled=False, max_rows=5)
    dashboard.set_tasks([str(index) for index in range(count)])
    started = time.perf_counter()
    with dashboard:
        for index in range(1, count + 1):
            dashboard.mark_running(index=index)
            dashboard.mark_finished(index=index, status="OK")
    elapsed = time.perf_counter() - started
    record_property("five_thousand_updates_seconds", round(elapsed, 4))
    assert len(dashboard._completed_order) == 5
    assert len(dashboard._visible_tasks()) == 5
    assert dashboard._tasks[count].status == "OK"


class _SyntheticAsyncResult:
    """Deterministic completed pool result, to inject infrastructure faults."""

    def __init__(self, result: object | None = None, *, error: Exception | None = None) -> None:
        self.result = result
        self.error = error

    def ready(self) -> bool:
        return True

    def get(self) -> object:
        if self.error is not None:
            raise self.error
        return self.result


_MaybeEncodingError = cast("type[Exception]", getattr(multiprocessing_pool, "MaybeEncodingError"))  # noqa: B009 - Python 3.14 typing stubs omit this runtime class


@pytest.mark.parametrize(
    "error",
    [
        _MaybeEncodingError(ValueError("pickling failed"), "synthetic failed-result encoding"),
        OSError("synthetic pool transport error"),
    ],
)
def test_pool_transport_errors_propagate_without_becoming_item_failures(error: Exception) -> None:
    dashboard = LiveWorkflowDashboard(title="Pool", enabled=False)
    dashboard.set_tasks(["one"])
    pending = {1: cast("ApplyResult[int]", _SyntheticAsyncResult(error=error))}
    outcomes: dict[int, dw.WorkflowItemOutcome[int]] = {}
    with pytest.raises(type(error)):
        dw._collect_finished_dashboard_results(
            dashboard=dashboard,
            pending_results=pending,
            indexed_results={},
            completed_indexes=set(),
            status_for_result=lambda _value: "OK",
            detail_for_result=str,
            indexed_outcomes=outcomes,
        )
    assert not outcomes
    assert dashboard._tasks[1].status != "FAIL"
    assert 1 in pending


@pytest.mark.parametrize("mode", ["serial", "pool"])
@pytest.mark.parametrize("adapter", ["status", "detail"])
def test_invalid_result_adapter_propagates_in_both_execution_paths(mode: str, adapter: str) -> None:
    dashboard = LiveWorkflowDashboard(title="Adapter", enabled=False)
    dashboard.set_tasks(["one"])
    outcomes: dict[int, dw.WorkflowItemOutcome[int]] = {}

    def bad_status(_value: int) -> dw.WorkflowStatus:
        return "RUNNING" if adapter == "status" else "OK"

    def bad_detail(_value: int) -> str:
        if adapter == "detail":
            msg = "adapter programming error"
            raise RuntimeError(msg)
        return "good"

    def invoke_adapter() -> None:
        if mode == "serial":
            dw._run_serial_dashboard_workflow(
                items=[1],
                process_item=lambda item: item,
                dashboard=dashboard,
                status_for_result=bad_status,
                detail_for_result=bad_detail,
                indexed_results={},
                indexed_outcomes=outcomes,
            )
        else:
            dw._collect_finished_dashboard_results(
                dashboard=dashboard,
                pending_results={1: cast("ApplyResult[int]", _SyntheticAsyncResult(1))},
                indexed_results={},
                completed_indexes=set(),
                status_for_result=bad_status,
                detail_for_result=bad_detail,
                indexed_outcomes=outcomes,
            )

    with pytest.raises((ValueError, RuntimeError)):
        invoke_adapter()
    assert not outcomes
    assert dashboard._tasks[1].status != "FAIL"


@pytest.mark.parametrize("mode", ["list", "report"])
def test_user_on_outcome_callback_propagates(mode: str) -> None:
    dashboard = LiveWorkflowDashboard(title="Callback", enabled=False)
    dashboard.set_tasks(["one"])

    def broken_callback(_outcome: dw.WorkflowItemOutcome[int]) -> None:
        msg = "user callback is faulty"
        raise RuntimeError(msg)

    runner = run_dashboard_workflow if mode == "list" else run_dashboard_workflow_report
    with pytest.raises(RuntimeError, match="user callback is faulty"):
        runner(
            items=[1],
            process_item=lambda item: item,
            dashboard=dashboard,
            pool_size=1,
            status_for_result=lambda _value: "OK",
            detail_for_result=str,
            on_outcome=broken_callback,
        )
    assert dashboard._context_depth == 0


def test_legacy_list_and_report_preserve_order_and_distinct_domain_failures() -> None:
    items = [3, 1, 2]
    for mode in ("list", "report"):
        dashboard = LiveWorkflowDashboard(title="Parity", enabled=False)
        dashboard.set_tasks(["three", "one", "two"])
        runner = run_dashboard_workflow if mode == "list" else run_dashboard_workflow_report
        result = runner(
            items=items,
            process_item=lambda item: item * 10,
            dashboard=dashboard,
            pool_size=1,
            status_for_result=lambda value: "FAIL" if value == 20 else "OK",
            detail_for_result=str,
        )
        if mode == "list":
            assert result == [30, 10, 20]
        else:
            assert isinstance(result, dw.WorkflowRunReport)
            assert result.results == [30, 10, 20]
            assert [outcome.status for outcome in result.outcomes] == ["OK", "OK", "FAIL"]
        assert dashboard._tasks[3].status == "FAIL"
