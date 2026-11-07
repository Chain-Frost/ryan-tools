"""Tests for ryan_library.functions.live_dashboard."""

# These unit tests intentionally inspect internal rendering and task state.
# pyright: reportPrivateUsage=false

import re
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


def _rendered_selection_indexes(dashboard: ld.LiveWorkflowDashboard) -> tuple[str, list[int]]:
    dashboard.console.print(dashboard._render())
    rendered = dashboard.console.export_text(clear=True)
    return rendered, [int(value) for value in re.findall(r"\bSelection (\d+)\b", rendered)]


def test_newest_first_renders_running_604_then_history_603_through_595() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=10, newest_first=True)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=40, color_system=None)
    dashboard.set_tasks([f"Selection {index:03d}" for index in range(1, 606)])
    for index in range(1, 604):
        dashboard.mark_finished(index=index, status="OK")
    dashboard.mark_running(index=604)

    expected = list(range(604, 594, -1))
    assert [task.index for task in dashboard._visible_tasks()] == expected
    rendered, indexes = _rendered_selection_indexes(dashboard)
    assert indexes == expected
    assert rendered.count("RUNNING") == 1
    assert "Selection 594" not in rendered
    assert "Selection 605" not in rendered


def test_newest_first_keeps_older_active_work_and_ignores_completion_order() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=5, newest_first=True)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=40, color_system=None)
    dashboard.set_tasks([f"Selection {index:03d}" for index in range(1, 11)])
    for index in (1, 2, 3, 4, 8, 9):
        dashboard.mark_running(index=index)
    for index in (2, 8, 3):
        dashboard.mark_finished(index=index, status="OK")

    expected = [9, 8, 4, 3, 1]
    visible = dashboard._visible_tasks()
    assert [task.index for task in visible] == expected
    assert [task.index for task in visible if task.status == "RUNNING"] == [9, 4, 1]
    _, indexes = _rendered_selection_indexes(dashboard)
    assert indexes == expected

    # Completion order changes statuses, not the numeric order of the same
    # selected rows. Older active work remains selected ahead of stale history.
    for index in (9, 4, 1):
        dashboard.mark_finished(index=index, status="OK")
        assert [task.index for task in dashboard._visible_tasks()] == expected
        _, indexes = _rendered_selection_indexes(dashboard)
        assert indexes == expected


def test_newest_first_keeps_queued_preview_in_plan_order() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=3, newest_first=True)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=30, color_system=None)
    dashboard.set_tasks([f"Selection {index:03d}" for index in range(1, 6)])

    assert [task.index for task in dashboard._visible_tasks()] == [1, 2, 3]
    rendered, indexes = _rendered_selection_indexes(dashboard)
    assert indexes == [1, 2, 3]
    assert rendered.count("QUEUED") == 3


def test_newest_first_grouped_panels_keep_priority_and_fit_with_an_alert() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False, max_rows=4, group_by="model", newest_first=True)
    dashboard.console = Console(file=StringIO(), record=True, width=100, height=26, color_system=None)
    dashboard.set_tasks(
        [f"Selection {index:03d}" for index in range(1, 6)],
        metadata=[{"model": "A.tcf" if index % 2 else "B.tcf"} for index in range(1, 6)],
    )
    for index in range(1, 5):
        dashboard.mark_running(index=index)

    assert [task.index for task in dashboard._visible_tasks()] == [4, 3, 2, 1]
    rendered, indexes = _rendered_selection_indexes(dashboard)
    assert indexes == [4, 2, 3, 1]
    assert rendered.index("B.tcf") < rendered.index("A.tcf")
    assert len(rendered.splitlines()) <= dashboard.console.size.height

    dashboard.set_alert(ld.WorkflowAlert(title="GPU warning", lines=("Waiting for GPU 0",)))
    # Height pressure retains the original oldest-active selection priority,
    # then reverses those selected rows and their visible group order.
    assert [task.index for task in dashboard._visible_tasks()] == [3, 2, 1]
    rendered, indexes = _rendered_selection_indexes(dashboard)
    assert indexes == [3, 1, 2]
    assert rendered.index("A.tcf") < rendered.index("B.tcf")
    assert "Selection 004" not in rendered
    assert "GPU warning" in rendered
    assert "Waiting for GPU 0" in rendered
    assert len(rendered.splitlines()) <= dashboard.console.size.height


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


def test_ellipsized_columns_shrink_before_identity_without_wrapping_rows_or_headers() -> None:
    dashboard = ld.LiveWorkflowDashboard(
        title="Test",
        enabled=False,
        columns=(
            ld.WorkflowColumn("Run", "metadata", metadata_key="run", no_wrap=True, min_width=3),
            ld.WorkflowColumn("A selection heading that would wrap", "label", no_wrap=True, overflow="ellipsis"),
            ld.WorkflowColumn("Status", "status", no_wrap=True, min_width=7),
            ld.WorkflowColumn("GPU", "metadata", metadata_key="gpu", no_wrap=True, min_width=4),
            ld.WorkflowColumn("Elapsed", "duration", no_wrap=True, min_width=8),
            ld.WorkflowColumn("A diagnostic heading that would also wrap", "detail", no_wrap=True, overflow="ellipsis"),
        ),
    )
    dashboard.console = Console(file=StringIO(), record=True, width=80, color_system=None)
    dashboard.set_tasks(
        ["A very long event and scenario selection" * 3, "Another very long selection" * 3],
        metadata=[{"run": 101, "gpu": "-pu0"}, {"run": 102, "gpu": "-pu1"}],
    )
    for index in (1, 2):
        dashboard.mark_running(index=index, detail="Writing results; dt 0.674s / ME -2.5e-07%; " * 4)
    dashboard.console.print(dashboard._build_task_table())
    rendered = dashboard.console.export_text()
    rows = [line for line in rendered.splitlines() if "RUNNING" in line]
    assert len(rows) == 2
    for row, run, gpu in zip(rows, ("101", "102"), ("-pu0", "-pu1"), strict=True):
        assert run in row.split()
        assert gpu in row.split()
        assert "00:00:00" in row
    # Simple table: one header, one rule, two data rows, and two spacing lines.
    assert len(rendered.splitlines()) == 6
    assert "…" in rendered


def test_ellipsized_text_keeps_literal_markup_characters() -> None:
    dashboard = ld.LiveWorkflowDashboard(
        title="Test",
        enabled=False,
        columns=(
            ld.WorkflowColumn("Selection [red]", "label", no_wrap=True, overflow="ellipsis"),
            ld.WorkflowColumn("Detail [bold]", "detail", no_wrap=True, overflow="ellipsis"),
        ),
    )
    dashboard.console = Console(file=StringIO(), record=True, width=80, color_system=None)
    dashboard.set_tasks(["model[red].tcf"])
    dashboard.mark_running(index=1, detail="Literal [bold] diagnostic")
    dashboard.console.print(dashboard._build_task_table())
    rendered = dashboard.console.export_text()
    for expected in ("Selection [red]", "Detail [bold]", "model[red].tcf", "Literal [bold] diagnostic"):
        assert expected in rendered


def test_default_columns_still_wrap_complete_detail_text() -> None:
    dashboard = ld.LiveWorkflowDashboard(title="Test", enabled=False)
    dashboard.console = Console(file=StringIO(), record=True, width=70, color_system=None)
    dashboard.set_tasks(["Input"])
    detail = "This detailed explanation remains available across several lines and preserves the final words."
    dashboard.mark_running(index=1, detail=detail)
    dashboard.console.print(dashboard._build_task_table())
    rendered = dashboard.console.export_text()
    assert len(rendered.splitlines()) > 5
    assert "This detailed" in rendered
    assert "final words." in rendered
    assert "…" not in rendered
