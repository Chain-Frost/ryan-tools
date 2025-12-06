"""Additional coverage tests for loguru_helpers."""

import pytest
from unittest.mock import MagicMock, patch, ANY
import pickle
from ryan_library.functions import loguru_helpers


class TestLoguruHelpersCoverage:
    @patch("ryan_library.functions.loguru_helpers.logger")
    def test_reset_logging(self, mock_logger):
        """Test reset_logging calls logger.remove()."""
        loguru_helpers.reset_logging()
        mock_logger.remove.assert_called_once()

    @patch("ryan_library.functions.loguru_helpers.logger")
    def test_configure_serial_logging_console_only(self, mock_logger):
        """Test configure_serial_logging with console only."""
        loguru_helpers.configure_serial_logging(console_log_level="DEBUG")

        # Should call remove once
        mock_logger.remove.assert_called_once()
        # Should call add once for console
        mock_logger.add.assert_called_once()
        args, kwargs = mock_logger.add.call_args
        assert kwargs["level"] == "DEBUG"
        assert kwargs["sink"] == loguru_helpers.sys.stdout

    @patch("ryan_library.functions.loguru_helpers.logger")
    @patch("ryan_library.functions.loguru_helpers.add_file_sink")
    def test_configure_serial_logging_with_file(self, mock_add_file, mock_logger):
        """Test configure_serial_logging with file."""
        loguru_helpers.configure_serial_logging(log_file="test.log")

        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()  # Console
        mock_add_file.assert_called_once_with(log_file="test.log")

    @patch("ryan_library.functions.loguru_helpers.logger")
    def test_add_file_sink(self, mock_logger):
        """Test add_file_sink."""
        with patch("os.getcwd", return_value="/tmp"):
            loguru_helpers.add_file_sink("test.log")

            mock_logger.add.assert_called_once()
            args, kwargs = mock_logger.add.call_args
            # Check if path is absolute
            assert "test.log" in str(kwargs["sink"])

    @patch("ryan_library.functions.loguru_helpers.logger")
    def test_log_exception(self, mock_logger):
        """Test log_exception."""
        loguru_helpers.log_exception("Error details")
        mock_logger.exception.assert_called_once()
        assert "Error details" in mock_logger.exception.call_args[0][0]

    @patch("ryan_library.functions.loguru_helpers.worker_configurer")
    def test_worker_initializer(self, mock_configurer):
        """Test worker_initializer."""
        mock_queue = MagicMock()
        loguru_helpers.worker_initializer(mock_queue)
        mock_configurer.assert_called_once_with(queue=mock_queue, level="DEBUG")

    @patch("ryan_library.functions.loguru_helpers.logger")
    def test_worker_configurer(self, mock_logger):
        """Test worker_configurer."""
        mock_queue = MagicMock()
        loguru_helpers.worker_configurer(mock_queue)

        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()
        # Check if sink is passed
        args, kwargs = mock_logger.add.call_args
        sink = kwargs.get("sink") or args[0]
        # Just verify it's an object with a write method, which QueueSink has
        assert hasattr(sink, "write")

    def test_setup_logger_factory(self):
        """Test setup_logger factory function."""
        with patch("ryan_library.functions.loguru_helpers.LoguruMultiprocessingLogger") as MockLogger:
            loguru_helpers.setup_logger(console_log_level="DEBUG", log_file="test.log")
            MockLogger.assert_called_once()
            _, kwargs = MockLogger.call_args
            assert kwargs["console_log_level"] == "DEBUG"
            assert "test.log" in str(kwargs["log_file"])

    @patch("ryan_library.functions.loguru_helpers.logger")
    @patch("ryan_library.functions.loguru_helpers.pickle")
    def test_listener_process_loop(self, mock_pickle, mock_logger):
        """Test listener_process loop processing."""
        mock_queue = MagicMock()

        # Simulate queue items: 1 record (any object), then None to exit
        mock_queue.get.side_effect = [b"somebytes", None]

        # Mock pickle.loads to return a dict-like object
        record_dict = {
            "level": MagicMock(name="INFO"),
            "message": "Test Message",
            "module": "mod",
            "function": "func",
            "line": 10,
            "exception": None,
            "file": "other.py",
        }
        record_dict["level"].name = "INFO"

        mock_pickle.loads.return_value = record_dict

        loguru_helpers.listener_process(mock_queue)

        # Verify logger.log was called
        mock_logger.log.assert_called()
        args, _ = mock_logger.log.call_args
        assert args[0] == "INFO"
        assert "mod:func:10 - Test Message" in args[1]

    @patch("ryan_library.functions.loguru_helpers.logger")
    @patch("ryan_library.functions.loguru_helpers.pickle")
    def test_listener_process_filter_self(self, mock_pickle, mock_logger):
        """Test listener_process filters logs from loguru_helpers.py."""
        mock_queue = MagicMock()
        mock_queue.get.side_effect = [b"bytes", None]

        record_dict = {"file": "loguru_helpers.py"}
        mock_pickle.loads.return_value = record_dict

        loguru_helpers.listener_process(mock_queue)

        # Should NOT log
        mock_logger.log.assert_not_called()
        mock_logger.opt.assert_not_called()
