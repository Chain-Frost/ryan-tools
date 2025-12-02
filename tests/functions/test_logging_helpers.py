"""Tests for ryan_library.functions.logging_helpers."""

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from ryan_library.functions import logging_helpers


class TestConditionalFormatter:
    def test_format_detailed(self):
        """Test formatting without simple_format attribute."""
        formatter = logging_helpers.ConditionalFormatter(
            detailed_fmt="DETAILED: %(message)s",
            simple_fmt="SIMPLE: %(message)s",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="Hello",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert result == "DETAILED: Hello"

    def test_format_simple(self):
        """Test formatting with simple_format attribute set to True."""
        formatter = logging_helpers.ConditionalFormatter(
            detailed_fmt="DETAILED: %(message)s",
            simple_fmt="SIMPLE: %(message)s",
        )
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="Hello",
            args=(),
            exc_info=None,
        )
        record.simple_format = True
        result = formatter.format(record)
        assert result == "SIMPLE: Hello"


class TestLoggerConfigurator:
    @pytest.fixture
    def mock_logger(self):
        with patch("logging.getLogger") as mock:
            yield mock

    def test_configure_console_only(self, mock_logger):
        """Test configuration with console logging only."""
        root_logger = MagicMock()
        mock_logger.return_value = root_logger
        
        configurator = logging_helpers.LoggerConfigurator(
            log_level=logging.DEBUG,
            enable_color=False # Disable color to test standard ConditionalFormatter path
        )
        configurator.configure()

        # Check level set
        root_logger.setLevel.assert_called_with(logging.DEBUG)
        
        # Check handlers cleared
        root_logger.handlers.clear.assert_called()
        
        # Check console handler added
        handlers = [call[0][0] for call in root_logger.addHandler.call_args_list]
        stream_handlers = [h for h in handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) > 0
        assert isinstance(stream_handlers[0].formatter, logging_helpers.ConditionalFormatter)

    def test_configure_file_logging(self, mock_logger, tmp_path):
        """Test configuration with file logging."""
        root_logger = MagicMock()
        mock_logger.return_value = root_logger
        
        # Mock Path.home to return tmp_path so logs go there
        with patch("pathlib.Path.home", return_value=tmp_path):
            configurator = logging_helpers.LoggerConfigurator(
                log_file="test.log",
                use_rotating_file=False
            )
            configurator.configure()

        # Verify file handler created
        # Since we mocked getLogger, we check the calls to addHandler
        # We expect 2 handlers: console and file (plus maybe NullHandler for __name__)
        handlers = [call[0][0] for call in root_logger.addHandler.call_args_list]
        file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) > 0
        
        # Verify log file creation attempted (directory structure)
        log_dir = tmp_path / "Documents" / "MyAppLogs"
        assert log_dir.exists()

    def test_configure_rotating_file_logging(self, mock_logger, tmp_path):
        """Test configuration with rotating file logging."""
        root_logger = MagicMock()
        mock_logger.return_value = root_logger
        
        with patch("pathlib.Path.home", return_value=tmp_path):
            configurator = logging_helpers.LoggerConfigurator(
                log_file="rotating.log",
                use_rotating_file=True,
                max_bytes=100,
                backup_count=2
            )
            configurator.configure()

        handlers = [call[0][0] for call in root_logger.addHandler.call_args_list]
        rotating_handlers = [h for h in handlers if isinstance(h, logging.handlers.RotatingFileHandler)]
        assert len(rotating_handlers) > 0
        assert rotating_handlers[0].maxBytes == 100
        assert rotating_handlers[0].backupCount == 2

    def test_configure_file_logging_exception(self, mock_logger, tmp_path):
        """Test exception handling during file logging setup."""
        root_logger = MagicMock()
        mock_logger.return_value = root_logger
        
        # Mock Path.home to raise an exception
        with patch("pathlib.Path.home", side_effect=PermissionError("Denied")):
            configurator = logging_helpers.LoggerConfigurator(log_file="test.log")
            configurator.configure()
            
        # Verify error logged
        root_logger.error.assert_called()
        assert "Failed to set up file logging" in root_logger.error.call_args[0][0]

    def test_setup_logging_backward_compatibility(self, mock_logger):
        """Test the backward compatible setup_logging function."""
        with patch("ryan_library.functions.logging_helpers.LoggerConfigurator") as MockConfigurator:
            instance = MockConfigurator.return_value
            logging_helpers.setup_logging(log_level=logging.WARNING, log_file="old.log")
            
            MockConfigurator.assert_called_with(
                log_level=logging.WARNING,
                log_file="old.log",
                max_bytes=10**6,
                backup_count=5,
                use_rotating_file=False,
                enable_color=True
            )
            instance.configure.assert_called_once()


class TestMultiprocessingLogging:
    def test_configure_multiprocessing_logging(self):
        """Test that QueueHandler is added to root logger."""
        mock_queue = MagicMock()
        
        with patch("logging.getLogger") as mock_get_logger:
            root_logger = MagicMock()
            root_logger.handlers = []
            mock_get_logger.return_value = root_logger
            
            logging_helpers.configure_multiprocessing_logging(mock_queue, logging.INFO)
            
            root_logger.setLevel.assert_called_with(logging.INFO)
            assert len(root_logger.addHandler.call_args_list) == 1
            handler = root_logger.addHandler.call_args[0][0]
            assert isinstance(handler, logging.handlers.QueueHandler)
            assert handler.queue == mock_queue

    def test_worker_initializer(self):
        """Test worker initializer calls configure."""
        with patch("ryan_library.functions.logging_helpers.configure_multiprocessing_logging") as mock_config:
            mock_queue = MagicMock()
            logging_helpers.worker_initializer(mock_queue, logging.DEBUG)
            mock_config.assert_called_with(mock_queue, logging.DEBUG)


class TestLogListenerProcess:
    def test_log_listener_process(self):
        """Test log listener process loop."""
        mock_queue = MagicMock()
        # Simulate one record then shutdown signal (None)
        record = logging.LogRecord("name", logging.INFO, "path", 1, "msg", (), None)
        mock_queue.get.side_effect = [record, None]
        
        with patch("logging.getLogger") as mock_get_logger:
            logger = MagicMock()
            mock_get_logger.return_value = logger
            
            # Mock LoggerConfigurator to avoid actual setup
            with patch("ryan_library.functions.logging_helpers.LoggerConfigurator") as MockConfig:
                logging_helpers.log_listener_process(mock_queue, log_file_name="listener.log")
                
                MockConfig.assert_called()
                MockConfig.return_value.configure.assert_called()
            
            # Verify record handled
            logger.handle.assert_called_with(record)
            # Verify shutdown message
            assert logger.info.call_count >= 2 # Started and Shutdown

    def test_log_listener_exception(self):
        """Test exception handling in log listener."""
        mock_queue = MagicMock()
        # Simulate exception then shutdown
        mock_queue.get.side_effect = [Exception("Queue Error"), None]
        
        with patch("logging.getLogger") as mock_get_logger:
            logger = MagicMock()
            mock_get_logger.return_value = logger
            
            with patch("ryan_library.functions.logging_helpers.LoggerConfigurator"):
                # Patch traceback to suppress printing to stderr during test
                with patch("traceback.print_exc"):
                    logging_helpers.log_listener_process(mock_queue)
            
            # Verify error logged
            logger.error.assert_called()
            assert "Error in log listener" in logger.error.call_args[0][0]
