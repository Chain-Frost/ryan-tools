"""Tests for ryan_library.functions.live_dashboard."""

import pytest
import datetime
from typing import Mapping
from ryan_library.functions import live_dashboard as ld


def test_format_duration() -> None:
    assert ld._format_duration(0) == "00:00:00"
    assert ld._format_duration(61.5) == "00:01:01"
    assert ld._format_duration(3600) == "01:00:00"
    assert ld._format_duration(-5) == "00:00:00"


def test_empty_row_values() -> None:
    cols = [ld.WorkflowColumn(header="A", source="label"), ld.WorkflowColumn(header="B", source="detail")]
    assert ld._empty_row_values(columns=cols, message="Msg") == ["Msg", ""]
    assert ld._empty_row_values(columns=[], message="Msg") == []


def test_dashboard_tasks() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)

    dashboard.set_tasks(["Task1", "Task2"], metadata=[{"scenario": "A"}, {"scenario": "B"}])
    assert len(dashboard._tasks) == 2
    assert dashboard._tasks[1].label == "Task1"
    assert dashboard._tasks[1].metadata == {"scenario": "A"}
    assert dashboard._tasks[2].label == "Task2"

    dashboard.mark_running(index=1, detail="starting", metadata={"progress": "10%"})
    assert dashboard._tasks[1].status == "RUNNING"
    assert dashboard._tasks[1].detail == "starting"
    assert dashboard._tasks[1].metadata["progress"] == "10%"
    assert dashboard._tasks[1].started_time is not None

    dashboard.mark_finished(index=1, status="OK", detail="done")
    assert dashboard._tasks[1].status == "OK"
    assert dashboard._tasks[1].finished_time is not None
    assert 1 in dashboard._completed_order


def test_dashboard_visible_tasks() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=3)
    dashboard.set_tasks(["T1", "T2", "T3", "T4", "T5"])

    # All queued initially, max 3 visible
    visible = dashboard._visible_tasks()
    assert len(visible) == 3
    assert visible[0].label == "T1"

    dashboard.mark_running(index=4)
    dashboard.mark_running(index=5)

    visible = dashboard._visible_tasks()
    assert len(visible) == 2
    assert visible[0].label == "T4"
    assert visible[1].label == "T5"

    dashboard.mark_finished(index=4, status="OK")
    dashboard.mark_finished(index=5, status="FAIL")

    visible = dashboard._visible_tasks()
    assert len(visible) == 2
    assert visible[0].status == "OK"
    assert visible[1].status == "FAIL"


def test_dashboard_context_manager() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)
    with dashboard as db:
        assert db is dashboard
        db.print("test message")


def test_metrics_and_summary() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["T1"])
    dashboard.set_active_count(2)
    dashboard.set_extra_metrics({"ETA": "1h"})

    assert dashboard._active_count == 2
    assert dashboard._extra_metrics == {"ETA": "1h"}

    summary = dashboard._build_summary()
    assert summary is not None
