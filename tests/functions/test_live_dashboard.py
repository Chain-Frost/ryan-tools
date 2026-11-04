"""Tests for ryan_library.functions.live_dashboard."""

# These unit tests intentionally inspect internal rendering and task state.
# pyright: reportPrivateUsage=false

from io import StringIO

from rich.console import Console

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


def test_persistent_alert_appears_updates_and_clears() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=3)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=30, color_system=None)
    dashboard.set_tasks(["T1", "T2", "T3"])

    dashboard.set_alert(ld.WorkflowAlert(title="FIRST ALERT", lines=("GPU 0 unavailable",)))
    dashboard.console.print(dashboard._render())
    first = dashboard.console.export_text(clear=True)
    assert "FIRST ALERT" in first
    assert "GPU 0 unavailable" in first
    assert "T1" in first

    dashboard.set_alert(ld.WorkflowAlert(title="UPDATED ALERT", lines=("GPU status unknown", "All work waiting")))
    dashboard.console.print(dashboard._render())
    updated = dashboard.console.export_text(clear=True)
    assert "UPDATED ALERT" in updated
    assert "GPU status unknown" in updated
    assert "FIRST ALERT" not in updated
    assert "T1" in updated

    dashboard.set_alert(None)
    dashboard.console.print(dashboard._render())
    cleared = dashboard.console.export_text(clear=True)
    assert "UPDATED ALERT" not in cleared
    assert "GPU status unknown" not in cleared
    assert "T1" in cleared


def test_observations_preserve_task_lifecycle_and_other_metadata() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.set_tasks(["Model"], metadata=[{"gpu": "-pu0"}])
    dashboard.mark_running(index=1, detail="PID 123")
    started = dashboard._tasks[1].started_time

    dashboard.update_task(index=1, metadata={"model_time": "0.25", "end_time": "2.0"}, refresh=False)
    task = dashboard._tasks[1]
    assert task.status == "RUNNING"
    assert task.started_time == started
    assert task.finished_time is None
    assert task.detail == "PID 123"
    assert task.metadata == {"gpu": "-pu0", "model_time": "0.25", "end_time": "2.0"}

    dashboard.mark_finished(index=1, status="OK", detail="Exit 0")
    finished = task.finished_time
    dashboard.update_task(index=1, metadata={"model_time": "2.0"}, detail="")
    assert task.status == "OK"
    assert task.started_time == started
    assert task.finished_time == finished
    assert task.detail == ""
    assert task.metadata["model_time"] == "2.0"
    assert list(dashboard._completed_order) == [1]


def test_grouped_tables_use_distinct_identities_and_literal_titles() -> None:
    dashboard = ld.LiveWorkflowDashboard(
        title="Test", enabled=False, group_by="model_path", group_label_key="model_name"
    )
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=60, color_system=None)
    dashboard.set_tasks(
        ["First selection", "Second selection"],
        metadata=[
            {"model_path": "project-a/model.tcf", "model_name": "model[red].tcf"},
            {"model_path": "project-b/model.tcf", "model_name": "model[red].tcf"},
        ],
    )
    dashboard.mark_running(index=1)
    dashboard.mark_running(index=2)
    dashboard.console.print(dashboard._render())
    rendered = dashboard.console.export_text()
    assert rendered.count("model[red].tcf") == 2
    assert "First selection" in rendered
    assert "Second selection" in rendered
    assert rendered.count("active 2") == 1


def test_grouped_tables_share_one_row_cap_and_keep_running_priority() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=4, group_by="model")
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=80, color_system=None)
    labels: list[str] = []
    metadata: list[dict[str, str]] = []
    for index in range(1, 13):
        labels.append(f"Selection {index:02d}")
        metadata.append({"model": "A.tcf" if index % 2 else "B.tcf"})
    dashboard.set_tasks(labels, metadata=metadata)
    dashboard.mark_finished(index=1, status="OK")
    dashboard.mark_finished(index=2, status="OK")
    for index in (9, 10, 11):
        dashboard.mark_running(index=index)

    assert [task.index for task in dashboard._visible_tasks()] == [9, 10, 11, 2]
    dashboard.console.print(dashboard._render())
    rendered = dashboard.console.export_text()
    assert rendered.count("Selection ") == 4
    assert rendered.count("A.tcf") == 1
    assert rendered.count("B.tcf") == 1
    assert "Selection 01" not in rendered
    assert "Selection 12" not in rendered


def test_grouped_tables_reserve_height_for_repeated_headers_and_alerts() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=10, group_by="model")
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=26, color_system=None)
    dashboard.set_tasks(
        ["Selection 1", "Selection 2", "Selection 3", "Selection 4"],
        metadata=[{"model": "A.tcf"}, {"model": "B.tcf"}, {"model": "C.tcf"}, {"model": "D.tcf"}],
    )
    for index in range(1, 5):
        dashboard.mark_running(index=index)
    assert [task.index for task in dashboard._visible_tasks()] == [1, 2]
    dashboard.console.print(dashboard._render())
    rendered = dashboard.console.export_text(clear=True)
    assert len(rendered.splitlines()) <= dashboard.console.size.height
    assert "Selection 2" in rendered
    assert "Selection 3" not in rendered

    dashboard.console.height = 20
    dashboard.set_alert(ld.WorkflowAlert(title="GPU warning", lines=("Waiting for GPU 0",)))
    dashboard.console.print(dashboard._render())
    rendered_with_alert = dashboard.console.export_text(clear=True)
    assert [task.index for task in dashboard._visible_tasks()] == [1]
    assert len(rendered_with_alert.splitlines()) <= dashboard.console.size.height
    assert "GPU warning" in rendered_with_alert
    assert "Selection 1" in rendered_with_alert


def test_ungrouped_dashboard_ignores_group_metadata() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=30, color_system=None)
    dashboard.set_tasks(["Task one", "Task two"], metadata=[{"model": "A.tcf"}, {"model": "B.tcf"}])
    dashboard.console.print(dashboard._render())
    rendered = dashboard.console.export_text()
    assert "Task one" in rendered
    assert "Task two" in rendered
    assert "A.tcf" not in rendered
    assert "B.tcf" not in rendered
