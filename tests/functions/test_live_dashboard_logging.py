"""Exercise logging configured before Rich takes ownership of the terminal."""

# The refresh-owner regression checks the actual Rich worker lifecycle.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path

import pytest
from loguru import logger
from rich.console import Console
from rich.live import Live

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import configure_serial_logging


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


def test_default_serial_logs_clear_and_redraw_live_display(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stream = StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setenv("TERM", "xterm-256color")
    console = Console(force_terminal=True, legacy_windows=False, width=100, height=30)
    log_file = tmp_path / "runner.log"
    try:
        # Copied runners configure logging before the dashboard exists.
        configure_serial_logging("INFO", str(log_file), console_format="{message}")
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
