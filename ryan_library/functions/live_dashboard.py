# ryan_library/functions/live_dashboard.py
"""Stateful Rich renderer for long-running workflow dashboards.

This module owns display concerns only: task state, summary counts, table
columns, Rich Live lifecycle, and rendering. It does not start worker pools,
read queues, or decide how work items are processed. Use
``dashboard_workflow.run_dashboard_workflow`` when a wrapper needs generic
serial/multiprocessing execution wired into this dashboard.
"""

from __future__ import annotations

import datetime
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Literal

import colorama
from rich import box
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

WorkflowStatus = Literal["QUEUED", "RUNNING", "OK", "SKIP", "FAIL"]
WorkflowColumnSource = Literal["index", "status", "duration", "label", "detail", "metadata"]
WorkflowOverflow = Literal["fold", "crop", "ellipsis", "ignore"]

_LIVE_PANEL_FIXED_ROW_ALLOWANCE = 12
_FIRST_GROUP_EXTRA_ROW_ALLOWANCE = 2
_ADDITIONAL_GROUP_ROW_ALLOWANCE = 6


def _empty_metadata() -> dict[str, str]:
    return {}


@dataclass(slots=True, frozen=True)
class WorkflowColumn:
    """Describe one rendered table column.

    ``source`` chooses which part of a ``WorkflowTask`` is shown. For
    ``source="metadata"``, ``metadata_key`` selects the value from the task's
    metadata dictionary. This lets wrappers add domain-specific columns such as
    file size, scenario, or model name without changing the renderer.

    Combining ``no_wrap=True`` with ``overflow="ellipsis"`` lets a column
    shrink before other no-wrap columns while keeping its header and cells
    on one line. Ordinary wrapping columns retain their configured overflow.
    """

    header: str
    source: WorkflowColumnSource
    metadata_key: str = ""
    justify: Literal["left", "center", "right"] = "left"
    no_wrap: bool = False
    overflow: WorkflowOverflow = "fold"
    min_width: int | None = None


@dataclass(slots=True)
class WorkflowTask:
    """Mutable state for one visible unit of work.

    Wrappers normally create these through ``LiveWorkflowDashboard.set_tasks``.
    The dashboard updates timestamps when tasks are marked running or finished,
    then derives durations and summary counts during rendering.
    """

    index: int
    label: str
    metadata: dict[str, str] = field(default_factory=_empty_metadata)
    status: WorkflowStatus = "QUEUED"
    detail: str = ""
    started_time: datetime.datetime | None = None
    finished_time: datetime.datetime | None = None


@dataclass(slots=True, frozen=True)
class WorkflowAlert:
    """Persistent workflow alert rendered above the normal dashboard panel."""

    title: str
    lines: tuple[str, ...]
    border_style: str = "bright_red"
    title_style: str = "bold bright_red"
    body_style: str = "bold red"


DEFAULT_WORKFLOW_COLUMNS: tuple[WorkflowColumn, ...] = (
    WorkflowColumn(header="#", source="index", justify="right", no_wrap=True),
    WorkflowColumn(header="State", source="status", no_wrap=True),
    WorkflowColumn(header="Duration", source="duration", no_wrap=True),
    WorkflowColumn(header="Item", source="label", overflow="fold"),
    WorkflowColumn(header="Detail", source="detail", overflow="fold"),
)


class LiveWorkflowDashboard:
    """Reusable Rich dashboard for file/process oriented workflows.

    The public methods are intentionally state-oriented: set the task list,
    update task states, set summary metrics, and refresh. That keeps this class
    usable with any execution model, including simple loops, thread pools, or
    the multiprocessing runner in ``dashboard_workflow.py``.

    ``group_by`` optionally names a metadata key identifying groups of rows.
    Each visible group gets a separate table panel under the shared summary.
    ``group_label_key`` supplies its human-readable title; the identity key is
    used when no label is supplied. Row limits apply across all groups.
    """

    def __init__(
        self,
        *,
        title: str,
        subtitle: str = "",
        enabled: bool = True,
        refresh_per_second: float = 2.0,
        max_rows: int = 25,
        columns: Sequence[WorkflowColumn] | None = None,
        transient: bool = True,
        screen: bool = False,
        group_by: str | None = None,
        group_label_key: str | None = None,
    ) -> None:
        if enabled:
            # Rich inspects console capabilities when Console is constructed.
            # Enable native ANSI handling first so Windows terminals can update
            # the live region in place instead of leaving old frames behind.
            colorama.just_fix_windows_console()
        self.title: str = title
        self.subtitle: str = subtitle
        self.enabled: bool = enabled
        self.refresh_per_second: float = max(refresh_per_second, 0.1)
        self.max_rows: int = max(max_rows, 1)
        self.columns: tuple[WorkflowColumn, ...] = tuple(columns or DEFAULT_WORKFLOW_COLUMNS)
        self.transient: bool = transient
        self.screen: bool = screen
        self.group_by: str | None = group_by
        self.group_label_key: str | None = group_label_key
        self.console: Console = Console()
        self._live: Live | None = None
        self._last_refresh: float = 0.0
        self._tasks: dict[int, WorkflowTask] = {}
        self._completed_order: deque[int] = deque()
        self._active_count: int = 0
        self._extra_metrics: dict[str, str] = {}
        self._alert: WorkflowAlert | None = None

    def __enter__(self) -> LiveWorkflowDashboard:
        if self.enabled:
            self._live = Live(
                self._render(),
                console=self.console,
                screen=self.screen,
                # Coordinators already refresh this snapshot. A second writer
                # can race log-driven redraws and leave partial frames behind.
                auto_refresh=False,
                refresh_per_second=self.refresh_per_second,
                transient=self.transient,
                redirect_stdout=True,
                redirect_stderr=True,
                vertical_overflow="crop",
            )
            self._live.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._live is not None:
            self._live.__exit__(exc_type, exc_val, exc_tb)
            self._live = None

    def set_tasks(
        self,
        labels: Sequence[str],
        metadata: Sequence[Mapping[str, object]] | None = None,
    ) -> None:
        """Replace all dashboard tasks using one-based display indexes.

        ``metadata`` is optional per-task context used by configurable columns.
        Missing metadata values render as blank cells.
        """
        self._tasks = {}
        for index, label in enumerate(labels, start=1):
            raw_metadata: Mapping[str, object] = metadata[index - 1] if metadata and index <= len(metadata) else {}
            self._tasks[index] = WorkflowTask(
                index=index,
                label=label,
                metadata={key: str(value) for key, value in raw_metadata.items()},
            )
        self._completed_order.clear()
        self._active_count = 0
        self.refresh(force=True)

    def set_active_count(self, count: int, *, refresh: bool = True) -> None:
        """Set active worker count for workflows that cannot name running tasks."""
        self._active_count = max(count, 0)
        if refresh:
            self.refresh()

    def set_extra_metrics(self, metrics: Mapping[str, object]) -> None:
        """Set short summary metrics displayed above the task table."""
        self._extra_metrics = {key: str(value) for key, value in metrics.items()}
        self.refresh()

    def set_alert(self, alert: WorkflowAlert | None, *, refresh: bool = True) -> None:
        """Show, replace, or clear one persistent alert above the dashboard."""
        self._alert = alert
        if refresh:
            self.refresh(force=True)

    def mark_running(
        self,
        *,
        index: int,
        detail: str = "",
        metadata: Mapping[str, object] | None = None,
        refresh: bool = True,
    ) -> None:
        """Mark one task as running and start/restart its duration timer."""
        task: WorkflowTask | None = self._tasks.get(index)
        if task is None:
            return
        task.status = "RUNNING"
        task.detail = detail
        if metadata:
            task.metadata.update({key: str(value) for key, value in metadata.items()})
        task.started_time = datetime.datetime.now()
        task.finished_time = None
        if refresh:
            self.refresh(force=True)

    def mark_finished(
        self,
        *,
        index: int,
        status: WorkflowStatus,
        detail: str = "",
        metadata: Mapping[str, object] | None = None,
        refresh: bool = True,
    ) -> None:
        """Mark one task as finished and keep it in the recent-history table."""
        task: WorkflowTask | None = self._tasks.get(index)
        if task is None:
            return
        task.status = status
        task.detail = detail
        if metadata:
            task.metadata.update({key: str(value) for key, value in metadata.items()})
        task.finished_time = datetime.datetime.now()
        if task.started_time is None:
            task.started_time = task.finished_time
        self._completed_order.append(index)
        if refresh:
            self.refresh(force=True)

    def update_task(
        self,
        *,
        index: int,
        metadata: Mapping[str, object] | None = None,
        detail: str | None = None,
        refresh: bool = True,
    ) -> None:
        """Update observations without changing a task's status or duration.

        Metadata is merged with the existing values. ``detail=None`` retains
        the current detail, while an empty string deliberately clears it.
        Updates use the normal refresh throttle rather than restarting a task.
        """
        task = self._tasks.get(index)
        if task is None:
            return
        if metadata is not None:
            for key, value in metadata.items():
                task.metadata[key] = str(value)
        if detail is not None:
            task.detail = detail
        if refresh:
            self.refresh()

    def print(self, message: str) -> None:
        """Print through the dashboard console so output coexists with Live."""
        self.console.print(message, markup=False)

    def refresh(self, *, force: bool = False) -> None:
        """Refresh the dashboard, throttling non-forced updates."""
        if not self.enabled or self._live is None:
            return
        now_monotonic: float = time.monotonic()
        min_interval: float = 1.0 / self.refresh_per_second
        if force or now_monotonic - self._last_refresh >= min_interval:
            self._live.update(self._render(), refresh=True)
            self._last_refresh = now_monotonic

    def _render(self) -> RenderableType:
        summary: Table = self._build_summary()
        task_tables: RenderableType = (
            self._build_task_table() if self.group_by is None else self._build_grouped_task_tables()
        )
        dashboard_panel = Panel(
            Group(summary, task_tables),
            title=self.title,
            subtitle=self.subtitle,
            border_style="cyan",
        )
        if self._alert is None:
            return dashboard_panel
        alert_panel = Panel(
            Text("\n".join(self._alert.lines), style=self._alert.body_style),
            title=Text(self._alert.title, style=self._alert.title_style),
            border_style=self._alert.border_style,
            expand=True,
        )
        return Group(alert_panel, dashboard_panel)

    def _build_summary(self) -> Table:
        effective_max_rows: int = self._effective_max_rows()
        total: int = len(self._tasks)
        counts: dict[WorkflowStatus, int] = {
            "QUEUED": 0,
            "RUNNING": 0,
            "OK": 0,
            "SKIP": 0,
            "FAIL": 0,
        }
        for task in self._tasks.values():
            counts[task.status] += 1

        finished: int = counts["OK"] + counts["SKIP"] + counts["FAIL"]
        active: int = self._active_count or counts["RUNNING"]

        summary = Table.grid(expand=True)
        summary.add_column(justify="left")
        summary.add_column(justify="left")
        summary.add_column(justify="left")
        summary.add_column(justify="left")
        summary.add_row(
            f"finished {finished}/{total}",
            f"active {active}",
            f"queued {counts['QUEUED']}",
            self._format_row_limit(effective_max_rows=effective_max_rows),
        )
        summary.add_row(
            f"OK {counts['OK']}",
            f"SKIP {counts['SKIP']}",
            f"FAIL {counts['FAIL']}",
            self._format_extra_metrics(),
        )
        return summary

    def _format_extra_metrics(self) -> str:
        if not self._extra_metrics:
            return ""
        return " | ".join(f"{key} {value}" for key, value in self._extra_metrics.items())

    def _build_task_table(self, rows: Sequence[WorkflowTask] | None = None) -> Table:
        table = Table(box=box.SIMPLE_HEAVY, expand=True)
        for column in self.columns:
            shrinkable = column.no_wrap and column.overflow == "ellipsis"
            header = Text(column.header, no_wrap=True, overflow="ellipsis") if shrinkable else column.header
            table.add_column(
                header,
                justify=column.justify,
                # Rich protects no-wrap columns from preferential shrinking.
                # Keep ellipsized cells on one line through Text instead, so
                # long descriptions give up width before row identity/status.
                no_wrap=column.no_wrap and not shrinkable,
                overflow=column.overflow,
                min_width=column.min_width,
            )

        if rows is None:
            rows = self._visible_tasks()
        if not rows:
            values = _empty_row_values(columns=self.columns, message="Waiting for work")
            table.add_row(
                *(_table_cell(value=value, column=column) for column, value in zip(self.columns, values, strict=True)),
                style="dim",
            )
            return table

        for task in rows:
            table.add_row(
                *(
                    _table_cell(value=self._column_value(task=task, column=column), column=column)
                    for column in self.columns
                )
            )
        return table

    def _build_grouped_task_tables(self) -> RenderableType:
        rows = self._visible_tasks()
        if self.group_by is None or not rows:
            return self._build_task_table(rows)

        groups: dict[str, list[WorkflowTask]] = {}
        for task in rows:
            identity = task.metadata.get(self.group_by, "")
            groups.setdefault(identity, []).append(task)

        panels: list[Panel] = []
        for identity, grouped_rows in groups.items():
            title = identity
            if self.group_label_key is not None:
                title = grouped_rows[0].metadata.get(self.group_label_key, "") or identity
            panels.append(
                Panel(
                    self._build_task_table(grouped_rows),
                    title=Text(title or "Unspecified", no_wrap=True, overflow="ellipsis"),
                    border_style="cyan",
                )
            )
        return Group(*panels)

    def _column_value(self, *, task: WorkflowTask, column: WorkflowColumn) -> str:
        if column.source == "index":
            return f"{task.index:03d}"
        if column.source == "status":
            style: str = self._status_style(status=task.status)
            return f"[{style}]{task.status}[/{style}]"
        if column.source == "duration":
            return self._task_duration(task=task)
        if column.source == "label":
            return escape(task.label)
        if column.source == "detail":
            return escape(task.detail)
        return escape(task.metadata.get(column.metadata_key, ""))

    def _visible_tasks(self) -> list[WorkflowTask]:
        """Return rows to render, prioritising active work over history.

        The dashboard is deliberately compact: running tasks are always shown
        first, recent finished tasks fill any spare rows, and queued tasks are
        shown only while nothing has started yet.
        """
        return self._select_tasks(self._effective_max_rows())

    def _select_tasks(self, max_rows: int) -> list[WorkflowTask]:
        """Select rows globally before assigning them to any group panels."""
        running: list[WorkflowTask] = [
            task for task in sorted(self._tasks.values(), key=lambda item: item.index) if task.status == "RUNNING"
        ]
        if len(running) >= max_rows:
            return running[:max_rows]

        remaining_rows: int = max(max_rows - len(running), 0)
        recent_indexes: list[int] = list(self._completed_order)[-remaining_rows:] if remaining_rows else []
        recent: list[WorkflowTask] = [self._tasks[index] for index in recent_indexes if index in self._tasks]

        if running or recent:
            return [*running, *recent]

        queued: list[WorkflowTask] = [
            task for task in sorted(self._tasks.values(), key=lambda item: item.index) if task.status == "QUEUED"
        ]
        return queued[:max_rows]

    def _effective_max_rows(self) -> int:
        """Clamp visible task rows so the live region fits in the terminal."""
        terminal_height: int = self.console.size.height
        alert_rows = len(self._alert.lines) + 2 if self._alert is not None else 0
        available_rows = terminal_height - _LIVE_PANEL_FIXED_ROW_ALLOWANCE - alert_rows
        max_rows = min(self.max_rows, max(available_rows, 1))
        if self.group_by is None:
            return max_rows

        rows = self._select_tasks(max_rows)
        if not rows:
            return max_rows
        # Retain the same selection priority if the group panels require fewer
        # rows: all active work first, then newest history. Queued rows keep
        # their original order and appear only before any work has started.
        priority_rows: list[WorkflowTask] = []
        recent_rows: list[WorkflowTask] = []
        for task in rows:
            if task.status in {"RUNNING", "QUEUED"}:
                priority_rows.append(task)
            else:
                recent_rows.append(task)
        priority_rows.extend(reversed(recent_rows))

        identities: set[str] = set()
        group_allowance = 0
        for row_number, task in enumerate(priority_rows, start=1):
            identities.add(task.metadata.get(self.group_by, ""))
            # The first nested panel adds two border rows; each subsequent
            # panel also repeats the table header, rule and padding.
            group_allowance = _FIRST_GROUP_EXTRA_ROW_ALLOWANCE + (len(identities) - 1) * _ADDITIONAL_GROUP_ROW_ALLOWANCE
            if row_number + group_allowance > available_rows:
                return max(row_number - 1, 1)
        return min(max_rows, max(available_rows - group_allowance, 1))

    def _format_row_limit(self, *, effective_max_rows: int) -> str:
        if effective_max_rows == self.max_rows:
            return f"rows <= {self.max_rows}"
        return f"rows <= {effective_max_rows}/{self.max_rows}"

    def _task_duration(self, *, task: WorkflowTask) -> str:
        if task.started_time is None:
            return ""
        end_time: datetime.datetime = task.finished_time or datetime.datetime.now()
        return _format_duration((end_time - task.started_time).total_seconds())

    @staticmethod
    def _status_style(*, status: WorkflowStatus) -> str:
        if status == "OK":
            return "green"
        if status == "SKIP":
            return "yellow"
        if status == "FAIL":
            return "red"
        if status == "RUNNING":
            return "cyan"
        return "dim"


def _format_duration(total_seconds: float) -> str:
    seconds: int = max(int(total_seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _table_cell(*, value: str, column: WorkflowColumn) -> str | Text:
    """Preserve literal escaped values and styles while allowing ellipsis sizing."""
    if not (column.no_wrap and column.overflow == "ellipsis"):
        return value
    text = Text.from_markup(value, overflow="ellipsis")
    text.no_wrap = True
    return text


def _empty_row_values(*, columns: Sequence[WorkflowColumn], message: str) -> list[str]:
    if not columns:
        return []
    return [message if index == 0 else "" for index, _ in enumerate(columns)]
