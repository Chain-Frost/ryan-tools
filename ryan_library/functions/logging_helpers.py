# ryan_library/functions/logging_helpers.py

import logging
import logging.handlers
import sys
from typing import Optional
from logging.handlers import RotatingFileHandler
from pathlib import Path
from multiprocessing import Queue

try:
    import colorlog

    COLORLOG_AVAILABLE = True
except ImportError:
    COLORLOG_AVAILABLE = False


class ConditionalFormatter(logging.Formatter):
    """
    Custom formatter to switch between detailed and simple formats based on a log record attribute.
    """

    def __init__(self, detailed_fmt: str, simple_fmt: str, datefmt: Optional[str] = None):
        super().__init__(fmt=detailed_fmt, datefmt=datefmt)
        self.detailed_fmt = detailed_fmt
        self.simple_fmt = simple_fmt
        self.datefmt = datefmt

    def format(self, record: logging.LogRecord) -> str:
        if getattr(record, "simple_format", False):
            self._style._fmt = self.simple_fmt
        else:
            self._style._fmt = self.detailed_fmt
        return super().format(record)


class LoggerConfigurator:
    """
    Configures logging with options for detailed and simple formats, color support, and file handling.
    """

    def __init__(
        self,
        log_level: int = logging.INFO,
        log_file: Optional[str] = None,
        max_bytes: int = 10**6,  # 1 MB
        backup_count: int = 5,
        use_rotating_file: bool = False,
        enable_color: bool = True,
    ):
        self.log_level = log_level
        self.log_file = log_file
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.use_rotating_file = use_rotating_file
        self.enable_color = enable_color

        # Define formats based on colorlog availability
        if self.enable_color and COLORLOG_AVAILABLE:
            self.detailed_format = (
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - "
                "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s%(reset)s"
            )
            self.simple_format = "%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s"
        else:
            self.detailed_format = (
                "%(asctime)s - %(name)s - %(levelname)s - " "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
            )
            self.simple_format = "%(asctime)s - %(levelname)s - %(message)s"

        self.date_format = "%Y-%m-%d %H:%M:%S"

    def configure(self) -> None:
        logger = logging.getLogger()
        logger.setLevel(self.log_level)

        # Clear existing handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        # Console Handler with Conditional Formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)

        if self.enable_color and COLORLOG_AVAILABLE:
            # Use colorlog's ColoredFormatter
            color_formatter = colorlog.ColoredFormatter(
                fmt=self.detailed_format,
                datefmt=self.date_format,
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
                style="%",
            )
            console_handler.setFormatter(color_formatter)
        else:
            # Use ConditionalFormatter without log_color
            formatter = ConditionalFormatter(
                detailed_fmt=self.simple_format,  # Replace with a non-colored detailed format
                simple_fmt=self.simple_format,
                datefmt=self.date_format,
            )
            console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

        # File Handler with Standard Formatting
        if self.log_file:
            try:
                log_dir = Path.home() / "Documents" / "MyAppLogs"
                log_dir.mkdir(parents=True, exist_ok=True)
                log_path = log_dir / self.log_file

                if self.use_rotating_file:
                    file_handler = RotatingFileHandler(
                        log_path,
                        maxBytes=self.max_bytes,
                        backupCount=self.backup_count,
                        encoding="utf-8",
                    )
                else:
                    file_handler = logging.FileHandler(log_path, encoding="utf-8")

                file_handler.setLevel(self.log_level)
                file_formatter = logging.Formatter(
                    fmt=(
                        "%(asctime)s - %(name)s - %(levelname)s - "
                        "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
                    ),
                    datefmt=self.date_format,
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
            except Exception as e:
                logger.error(f"Failed to set up file logging: {e}")

        # Prevent "No Handlers" Warning
        logging.getLogger(__name__).addHandler(logging.NullHandler())

        logger.info("Logging is configured successfully.")


# Backward Compatibility: Retain the setup_logging() function
def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    use_rotating_file: bool = False,
    enable_color: bool = True,
) -> None:
    """
    Setup logging with colored console output and optional file logging.
    Retained for backward compatibility.

    Args:
        log_level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (Optional[str]): Name of the log file. If None, file logging is disabled.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        use_rotating_file (bool): Whether to use a rotating file handler.
        enable_color (bool): Whether to enable colored console logs.
    """
    logger_config = LoggerConfigurator(
        log_level=log_level,
        log_file=log_file,
        max_bytes=max_bytes,
        backup_count=backup_count,
        use_rotating_file=use_rotating_file,
        enable_color=enable_color,
    )
    logger_config.configure()


def worker_initializer(log_queue: Queue, log_level=logging.INFO):
    """
    Initializer for worker processes to configure logging and add simple log methods.
    """
    configure_multiprocessing_logging(log_queue, log_level)


def configure_multiprocessing_logging(log_queue: Queue, log_level=logging.INFO):
    """
    Configure logging for multiprocessing worker processes to ensure logs from child processes
    are correctly sent to the log queue.

    Args:
        log_queue (Queue): The multiprocessing queue to send log records to.
        log_level (int): Logging level.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers
    root_logger.handlers = []

    # Create a QueueHandler and add it to the root logger
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)


def log_listener_process(log_queue: Queue, log_level=logging.INFO, log_file_name=None):
    """
    A listener process that receives logs from child processes via a queue
    and logs them to a file and console. The handlers are configured within this
    process to avoid passing non-pickleable objects between processes.

    Args:
        log_queue (Queue): The multiprocessing queue to receive log records from.
        log_level (int): Logging level.
        log_file_name (str, optional): Name of the log file. If None, file logging is disabled.
    """
    # Configure logging for the listener using the class-based configurator
    logger_config = LoggerConfigurator(
        log_level=log_level,
        log_file=log_file_name,
        use_rotating_file=True,
        enable_color=True,
    )
    logger_config.configure()

    logger = logging.getLogger()
    logger.info("Log listener process started.")

    while True:
        try:
            record = log_queue.get()
            if record is None:  # We send None to signal the listener to exit
                logger.info("Log listener process received shutdown signal.")
                break
            logger.handle(record)  # No level or filter logic applied here
        except Exception as e:
            logger.error(f"Error in log listener: {e}")
            import traceback

            traceback.print_exc(file=sys.stderr)
