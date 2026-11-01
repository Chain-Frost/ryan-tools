"""Regression coverage for the shared dashboard contract in issue #180."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from ryan_library.functions.dashboard_workflow import run_dashboard_workflow, run_dashboard_workflow_report
from ryan_library.functions.live_dashboard import (
    LiveWorkflowDashboard,
    WorkflowMetric,
    validate_dashboard_settings,
)


def _worker(item: int) -> int | None:
    if item == 2:
        raise OSError("synthetic recoverable error")
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
    d.set_metrics([
        WorkflowMetric("Tiles", value=0, unit="tiles", total=None),
        WorkflowMetric("Cells", value=None, unit="cells"),
    ])
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


def test_bounded_history_and_plain_output(capsys: pytest.CaptureFixture[str]) -> None:
    d = LiveWorkflowDashboard(title="Batch", enabled=True, max_rows=4)
    d.console = d.console.__class__(force_terminal=False)
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
    assert "OK 100" in text and "output written" in text
    assert "\x1b[" not in text


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
    assert report.outcomes[2].has_result and report.outcomes[2].result is None
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
    assert report.outcomes == () and report.results == []

    with pytest.raises(ValueError, match="pool_size"):
        run_dashboard_workflow_report(
            items=[1],
            process_item=_worker,
            dashboard=d,
            pool_size=0,
            status_for_result=_status,  # type: ignore[arg-type]
            detail_for_result=_detail,
        )
