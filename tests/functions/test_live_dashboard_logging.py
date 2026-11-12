"""Exercise logging configured before Rich takes ownership of the terminal."""

# The refresh-owner regression checks the actual Rich worker lifecycle.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import logging
import sys
from io import StringIO
from pathlib import Path
from threading import Event, Thread, current_thread

import pytest
from loguru import logger
from rich.console import Console, RenderableType
from rich.live import Live

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import configure_serial_logging, forward_standard_logging


def test_default_file_sink_keeps_info_and_filters_debug(tmp_path: Path) -> None:
    log_file = tmp_path / "default-info.log"
    try:
        configure_serial_logging("WARNING", str(log_file))
        logger.enable(__name__)
        logger.debug("FILTERED_DEFAULT_DEBUG")
        logger.info("RETAINED_DEFAULT_INFO")
    finally:
        logger.remove()
    saved = log_file.read_text(encoding="utf-8")
    assert "RETAINED_DEFAULT_INFO" in saved
    assert "FILTERED_DEFAULT_DEBUG" not in saved


def test_worker_log_render_finishes_before_coordinator_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """A worker may render a live frame even when auto-refresh is disabled."""
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("FORCE_COLOR", "1")
    dashboard = LiveWorkflowDashboard(title="Concurrent render", group_by="model")
    dashboard.console.width = 100
    dashboard.console.height = 23
    dashboard.set_tasks(["A", "B"], metadata=[{"model": "model.tcf"}, {"model": "model.tcf"}])
    rendering = Event()
    release = Event()
    refreshing = Event()
    refreshed = Event()
    errors: list[BaseException] = []
    write_buffer = dashboard.console._write_buffer

    def paused_write() -> None:
        if current_thread().name == "dashboard-test-worker":
            rendering.set()
            if not release.wait(5):
                message = "Test did not release the worker frame"
                raise TimeoutError(message)
        write_buffer()

    monkeypatch.setattr(dashboard.console, "_write_buffer", paused_write)

    def worker_log() -> None:
        try:
            print("BACKGROUND_WARNING", flush=True)
        except BaseException as exc:  # noqa: BLE001 - propagate worker failures to the test thread
            errors.append(exc)

    def refresh() -> None:
        try:
            refreshing.set()
            dashboard.mark_running(index=1, refresh=False)
            dashboard.refresh(force=True)
            refreshed.set()
        except BaseException as exc:  # noqa: BLE001 - propagate coordinator failures to the test thread
            errors.append(exc)

    with dashboard:
        assert dashboard._live is not None
        worker = Thread(target=worker_log, name="dashboard-test-worker", daemon=True)
        coordinator = Thread(target=refresh, daemon=True)
        worker.start()
        try:
            assert rendering.wait(3)
            coordinator.start()
            assert refreshing.wait(3)
            # Without the console transaction, this refresh writes a new,
            # shorter frame before the worker writes its stale cursor reset.
            assert not refreshed.wait(0.1)
        finally:
            release.set()
            worker.join(5)
            if coordinator.ident is not None:
                coordinator.join(5)
        assert not worker.is_alive()
        assert not coordinator.is_alive()
        assert not errors
        assert refreshed.is_set()
    assert stream.getvalue().count("BACKGROUND_WARNING") == 1
    assert "RUNNING" in stream.getvalue()
    assert sys.stdout is stream


def test_grouped_dashboard_uses_one_refresh_owner_and_keeps_log_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    dashboard = LiveWorkflowDashboard(title="Synthetic simulations", transient=False, group_by="model")
    dashboard.console = Console(force_terminal=True, legacy_windows=False, width=151, height=23)
    dashboard.set_tasks(["A", "B"], metadata=[{"model": "synthetic.tcf"}, {"model": "synthetic.tcf"}])
    try:
        configure_serial_logging(
            console_sink=lambda message: print(message, end="", flush=True), console_format="{message}"
        )
        logger.enable(__name__)
        with dashboard:
            assert dashboard._live is not None
            assert dashboard._live._refresh_thread is None
            dashboard.mark_running(index=1)
            for level, message in (("SUCCESS", "INPUT_CHECK_PASSED"), ("WARNING", "VISIBLE_WARNING")):
                start = stream.tell()
                logger.log(level, message)
                output = stream.getvalue()[start:]
                assert "\x1b[2K" in output.split(message, 1)[0]
                assert output.index(message) < output.index("Synthetic simulations")
                assert "synthetic.tcf" in output
            dashboard.mark_finished(index=1, status="OK")
            dashboard.mark_running(index=2)
            dashboard.refresh(force=True)
    finally:
        logger.remove()
    assert sys.stdout is stream
    assert stream.getvalue().count("INPUT_CHECK_PASSED") == 1
    assert stream.getvalue().count("VISIBLE_WARNING") == 1


def test_opt_in_transient_cleanup_blanks_last_frame_before_rich_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Final cleanup blanks the panel, while the final summary stays visible."""
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("FORCE_COLOR", "1")
    rendered: list[RenderableType] = []
    original_update = Live.update

    def observe_update(live: Live, renderable: RenderableType, *, refresh: bool = False) -> None:
        rendered.append(renderable)
        original_update(live, renderable, refresh=refresh)

    monkeypatch.setattr(Live, "update", observe_update)
    dashboard = LiveWorkflowDashboard(title="Cleanup", clear_before_exit=True)
    dashboard.console.width = 100
    dashboard.console.height = 23
    dashboard.set_tasks(["one"])

    with dashboard:
        assert dashboard._live is not None
        dashboard.mark_finished(index=1, status="OK")
        dashboard.finish(outcome="SUCCESS", detail="1 completed run(s)")

    assert rendered[-1] == ""
    assert dashboard._live is None
    assert stream.getvalue().count("Cleanup: SUCCESS") == 1
    assert LiveWorkflowDashboard(title="Default").clear_before_exit is False


def test_default_serial_logs_clear_and_redraw_live_display(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    console = Console(force_terminal=True, legacy_windows=False, width=100, height=30)
    log_file = tmp_path / "runner.log"
    try:
        # Copied runners configure logging before the dashboard exists.
        configure_serial_logging("INFO", str(log_file), file_log_level="DEBUG", console_format="{message}")
        logger.enable(__name__)
        logger.info("BEFORE_LIVE")
        with Live("CURRENT DASHBOARD", console=console, auto_refresh=False):
            for width, height in ((100, 30), (60, 12), (120, 40)):
                console.width = width
                console.height = height
                start = stream.tell()
                logger.warning("RUN_EVENT {}\nDETAIL [literal]", width)
                output = stream.getvalue()[start:]
                # A raw stdout sink leaves the message below the live region;
                # Rich must clear it first, then redraw after the entire message.
                assert "\x1b[2K" in output.split("RUN_EVENT", 1)[0]
                assert output.index("DETAIL [literal]") < output.index("CURRENT DASHBOARD")
                assert output.count("RUN_EVENT") == 1
            logger.debug("FILE_ONLY_DEBUG")
        logger.success("AFTER_LIVE")
    finally:
        logger.remove()
    assert sys.stdout is stream
    assert stream.getvalue().count("BEFORE_LIVE") == 1
    assert stream.getvalue().count("AFTER_LIVE") == 1
    assert "FILE_ONLY_DEBUG" not in stream.getvalue()
    saved = log_file.read_text(encoding="utf-8")
    assert saved.count("RUN_EVENT") == 3
    assert "FILE_ONLY_DEBUG" in saved
    assert "\x1b[" not in saved


def test_default_serial_logs_with_redirected_output(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    console = Console(force_terminal=False, width=100)
    try:
        configure_serial_logging("INFO", console_format="{message}")
        logger.enable(__name__)
        with Live("FINAL DASHBOARD", console=console, auto_refresh=False):
            logger.info("HEADLESS_EVENT")
        logger.success("HEADLESS_DONE")
    finally:
        logger.remove()
    output = stream.getvalue()
    assert output.count("HEADLESS_EVENT") == 1
    assert output.count("HEADLESS_DONE") == 1
    assert output.count("FINAL DASHBOARD") == 1
    assert "\x1b[" not in output


def test_explicit_serial_console_sink_remains_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    messages: list[str] = []
    try:
        configure_serial_logging("SUCCESS", console_sink=messages.append, console_format="{message}")
        logger.enable(__name__)
        logger.info("HIDDEN_INFO")
        logger.success("CUSTOM_SINK")
    finally:
        logger.remove()
    assert messages == ["CUSTOM_SINK\n"]
    assert stream.getvalue() == ""


@pytest.mark.parametrize("existing_handler", [False, True])
def test_dependency_logs_follow_live_stdout_and_restore_configuration(
    monkeypatch: pytest.MonkeyPatch, existing_handler: bool
) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    dependency = logging.getLogger("synthetic_dashboard_dependency")
    original = logging.StreamHandler(stream)
    handlers = [original] if existing_handler else []
    monkeypatch.setattr(dependency, "handlers", handlers)
    monkeypatch.setattr(dependency, "level", logging.WARNING)
    monkeypatch.setattr(dependency, "propagate", True)
    root_handlers = logging.root.handlers[:]
    console = Console(force_terminal=True, legacy_windows=False, width=100, height=30)
    try:
        configure_serial_logging(console_format="{message}")
        with (  # noqa: PT012 - assert routing before exercising exceptional context exit
            pytest.raises(RuntimeError, match="exercise restoration"),
            forward_standard_logging(dependency.name),
        ):
            # Match a dependency that only installs its own default handler if
            # none exists. This works before and after its first import.
            assert dependency.hasHandlers()
            assert not dependency.propagate
            with Live("DEPENDENCY DASHBOARD", console=console, auto_refresh=False):
                start = stream.tell()
                worker = Thread(target=lambda: dependency.warning("DEPENDENCY_WARNING %s", "[literal]"))
                worker.start()
                worker.join(3)
                assert not worker.is_alive()
                output = stream.getvalue()[start:]
                assert "\x1b[2K" in output.split("DEPENDENCY_WARNING", 1)[0]
                assert output.index("DEPENDENCY_WARNING") < output.index("DEPENDENCY DASHBOARD")
            message = "exercise restoration"
            raise RuntimeError(message)
    finally:
        logger.remove()
    assert dependency.handlers == handlers
    assert dependency.level == logging.WARNING
    assert dependency.propagate
    assert logging.root.handlers == root_handlers
    assert stream.getvalue().count("DEPENDENCY_WARNING [literal]") == 1
