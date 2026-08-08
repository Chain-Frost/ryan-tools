"""Behavioural tests for serial and multiprocessing Loguru configuration."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from loguru import logger

from ryan_library.functions.loguru_helpers import LoggerManager, configure_notebook_logging, setup_logger

REPO_ROOT = Path(__file__).resolve().parents[2]
HARNESS = REPO_ROOT / "tests" / "support" / "loguru_multiprocessing_harness.py"


def run_harness(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the spawn-based harness with bounded, fully captured output."""

    env: dict[str, str] = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    return subprocess.run(
        [sys.executable, str(HARNESS), *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )


def test_multiprocessing_info_console_and_debug_file(tmp_path: Path) -> None:
    log_file = tmp_path / "detailed.log"
    result = run_harness(
        "--console-level",
        "INFO",
        "--log-file",
        str(log_file),
        "--file-level",
        "DEBUG",
    )

    assert "MAIN_INFO" in result.stdout
    assert "WORKER_INFO" in result.stdout
    assert "MAIN_DEBUG" not in result.stdout
    assert "WORKER_DEBUG" not in result.stdout
    assert "loguru_multiprocessing_harness:emit_worker_logs" in result.stdout
    assert "loguru_helpers:listener_process" not in result.stdout
    assert result.stdout.count("WORKER_INFO") == 1

    file_output = log_file.read_text(encoding="utf-8")
    assert "MAIN_DEBUG" in file_output
    assert "WORKER_DEBUG" in file_output
    assert file_output.count("WORKER_INFO") == 1
    assert "loguru_helpers:listener_process" not in file_output


def test_success_level_is_low_context_mode() -> None:
    result = run_harness("--console-level", "SUCCESS")

    assert "MAIN_DEBUG" not in result.stdout
    assert "MAIN_INFO" not in result.stdout
    assert "WORKER_DEBUG" not in result.stdout
    assert "WORKER_INFO" not in result.stdout
    assert "MAIN_SUCCESS" in result.stdout
    assert "WORKER_SUCCESS" in result.stdout
    assert "WORKER_WARNING" in result.stdout


def test_worker_exception_survives_queue_transport() -> None:
    result = run_harness("--console-level", "INFO", "--raise-worker-exception")

    assert "WORKER_EXCEPTION" in result.stdout
    assert "ValueError: synthetic worker failure" in result.stdout
    assert "loguru_multiprocessing_harness:emit_worker_logs" in result.stdout
    assert result.stdout.count("WORKER_EXCEPTION") == 1


def test_context_shutdown_is_idempotent_and_sequential(tmp_path: Path) -> None:
    log_file = tmp_path / "sequential.log"
    first_context = setup_logger(console_log_level="WARNING", log_file=str(log_file))
    with first_context:
        logger.warning("FIRST_CONTEXT")
    first_context.shutdown()

    second_context = setup_logger(console_log_level="WARNING", log_file=str(log_file))
    with second_context:
        logger.warning("SECOND_CONTEXT")
    second_context.shutdown()

    output = log_file.read_text(encoding="utf-8")
    assert output.count("FIRST_CONTEXT") == 1
    assert output.count("SECOND_CONTEXT") == 1


def test_nested_context_is_rejected() -> None:
    with setup_logger(console_log_level="WARNING"):
        with pytest.raises(RuntimeError, match="already active"):
            with setup_logger(console_log_level="WARNING"):
                pass
        with pytest.raises(RuntimeError, match="Cannot replace notebook sinks"):
            configure_notebook_logging(console_log_level="WARNING")


def test_invalid_level_fails_before_starting_listener() -> None:
    with pytest.raises(ValueError, match="Unknown Loguru level"):
        setup_logger(console_log_level="VERBOSE")


def test_logger_manager_singleton_can_reinitialize(tmp_path: Path) -> None:
    LoggerManager._instance = None
    manager = LoggerManager(log_level="WARNING", log_file="manager.log", log_dir=tmp_path)
    assert manager is LoggerManager()
    manager.shutdown()
    manager.shutdown()

    reinitialized = LoggerManager(log_level="WARNING", log_file="manager-2.log", log_dir=tmp_path)
    assert reinitialized is manager
    reinitialized.shutdown()


def test_notebook_reconfiguration_is_concise_and_not_duplicated(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_file = tmp_path / "notebook-debug.log"
    try:
        configure_notebook_logging(
            console_log_level="INFO",
            log_file=str(log_file),
            file_log_level="DEBUG",
        )
        logger.info("NOTEBOOK_FIRST")
        configure_notebook_logging(
            console_log_level="SUCCESS",
            log_file=str(log_file),
            file_log_level="DEBUG",
        )
        logger.debug("NOTEBOOK_DEBUG")
        logger.info("NOTEBOOK_HIDDEN_INFO")
        logger.success("NOTEBOOK_SUCCESS")
    finally:
        logger.remove()

    captured = capsys.readouterr()
    assert captured.err.count("NOTEBOOK_FIRST") == 1
    assert "NOTEBOOK_HIDDEN_INFO" not in captured.err
    assert captured.err.count("NOTEBOOK_SUCCESS") == 1
    file_output = log_file.read_text(encoding="utf-8")
    assert "NOTEBOOK_DEBUG" in file_output
    assert "NOTEBOOK_HIDDEN_INFO" in file_output
    assert file_output.count("NOTEBOOK_SUCCESS") == 1
