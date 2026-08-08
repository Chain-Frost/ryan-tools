"""Focused unit coverage for the Loguru configuration helpers."""

from __future__ import annotations

import pickle
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ryan_library.functions import loguru_helpers


def test_normalize_log_level() -> None:
    assert loguru_helpers.normalize_log_level(" info ") == "INFO"
    with pytest.raises(ValueError, match="Unknown Loguru level"):
        loguru_helpers.normalize_log_level("verbose")
    with pytest.raises(ValueError, match="must not be empty"):
        loguru_helpers.normalize_log_level("  ")


def test_minimum_log_level() -> None:
    assert loguru_helpers.minimum_log_level("WARNING", "DEBUG", "INFO") == "DEBUG"
    with pytest.raises(ValueError, match="At least one"):
        loguru_helpers.minimum_log_level()


def test_configure_serial_logging_with_independent_levels() -> None:
    with (
        patch.object(loguru_helpers, "reset_logging") as mock_reset,
        patch.object(loguru_helpers, "_add_console_sink") as mock_console,
        patch.object(loguru_helpers, "add_file_sink") as mock_file,
    ):
        loguru_helpers.configure_serial_logging(
            console_log_level="INFO",
            log_file="test.log",
            file_log_level="DEBUG",
        )

    mock_reset.assert_called_once_with()
    mock_console.assert_called_once_with(level="INFO", forwarded=False)
    mock_file.assert_called_once_with(log_file="test.log", file_log_level="DEBUG")


def test_configure_notebook_logging_is_reconfigurable() -> None:
    with (
        patch.object(loguru_helpers, "reset_logging") as mock_reset,
        patch.object(loguru_helpers, "_add_console_sink") as mock_console,
    ):
        loguru_helpers.configure_notebook_logging(console_log_level="SUCCESS")
        loguru_helpers.configure_notebook_logging(console_log_level="DEBUG")

    assert mock_reset.call_count == 2
    assert mock_console.call_count == 2
    assert mock_console.call_args.kwargs["sink"] is loguru_helpers.sys.stderr
    assert mock_console.call_args.kwargs["format_string"] == loguru_helpers.NOTEBOOK_FORMAT


def test_worker_initializer_uses_queue_capture_level_by_default() -> None:
    mock_queue = MagicMock()
    with patch.object(loguru_helpers, "worker_configurer") as mock_configurer:
        loguru_helpers.worker_initializer(mock_queue)
    mock_configurer.assert_called_once_with(queue=mock_queue, level=None)


def test_worker_configurer_serializes_minimal_real_record_shape() -> None:
    mock_queue = MagicMock()
    mock_queue.capture_log_level = "DEBUG"
    with patch.object(loguru_helpers, "logger") as mock_logger:
        mock_logger.level.side_effect = lambda name: SimpleNamespace(name=name, no={"DEBUG": 10}[name])
        loguru_helpers.worker_configurer(mock_queue)

    sink = mock_logger.add.call_args.kwargs["sink"]
    message = SimpleNamespace(
        record={
            "level": SimpleNamespace(name="INFO"),
            "message": "worker message",
            "module": "worker_module",
            "function": "worker_function",
            "line": 42,
            "exception": None,
        }
    )
    sink.write(message)

    payload = pickle.loads(mock_queue.put.call_args.args[0])
    assert payload == {
        "level": "INFO",
        "message": "worker message",
        "module": "worker_module",
        "function": "worker_function",
        "line": 42,
        "exception": None,
    }


def test_listener_reconstructs_origin_once() -> None:
    mock_queue = MagicMock()
    payload: loguru_helpers.SerializedLogRecord = {
        "level": "INFO",
        "message": "Test Message",
        "module": "mod",
        "function": "func",
        "line": 10,
        "exception": None,
    }
    mock_queue.get.side_effect = [pickle.dumps(payload), None]

    with (
        patch.object(loguru_helpers, "reset_logging"),
        patch.object(loguru_helpers, "_add_console_sink"),
        patch.object(loguru_helpers, "logger") as mock_logger,
    ):
        loguru_helpers.listener_process(mock_queue)

    mock_logger.log.assert_called_once_with("INFO", "mod:func:10 - Test Message")


def test_setup_logger_passes_both_sink_levels() -> None:
    with patch.object(loguru_helpers, "LoguruMultiprocessingLogger") as mock_context:
        loguru_helpers.setup_logger(
            console_log_level="INFO",
            log_file="test.log",
            file_log_level="TRACE",
        )

    kwargs = mock_context.call_args.kwargs
    assert kwargs["console_log_level"] == "INFO"
    assert kwargs["file_log_level"] == "TRACE"
    assert str(kwargs["log_file"]).endswith("test.log")


def test_log_exception_uses_rendered_message() -> None:
    with patch.object(loguru_helpers, "logger") as mock_logger:
        loguru_helpers.log_exception(": details")
    mock_logger.exception.assert_called_once_with("An exception occurred: details")
