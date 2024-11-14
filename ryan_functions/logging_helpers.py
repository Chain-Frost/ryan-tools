# ryan_functions/logging_helpers.py

import logging
import logging.handlers
import multiprocessing
import sys
from typing import Optional
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    import colorlog

    COLORLOG_AVAILABLE = True
except ImportError:
    COLORLOG_AVAILABLE = False


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
    Tailored to be robust for Windows Store Python environments.

    Args:
        log_level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (Optional[str]): Name of the log file. If None, file logging is disabled.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        use_rotating_file (bool): Whether to use a rotating file handler.
        enable_color (bool): Whether to enable colored console logs.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Avoid adding multiple handlers if already configured
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console Handler with (optional) Colored Formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    if enable_color and COLORLOG_AVAILABLE:
        # Define color formatter
        color_formatter = colorlog.ColoredFormatter(
            fmt=(
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - "
                "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s%(reset)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
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
        # Fallback to standard formatter if colorlog is unavailable or color is disabled
        standard_formatter = logging.Formatter(
            fmt=(
                "%(asctime)s - %(name)s - %(levelname)s - "
                "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(standard_formatter)

    logger.addHandler(console_handler)

    # File Handler with Standard Formatting
    if log_file:
        try:
            # Define log directory within user's Documents
            log_dir = Path.home() / "Documents" / "MyAppLogs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / log_file

            if use_rotating_file:
                file_handler = RotatingFileHandler(
                    log_path,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
            else:
                file_handler = logging.FileHandler(log_path, encoding="utf-8")

            file_handler.setLevel(log_level)

            # Define standard formatter for file logs
            file_formatter = logging.Formatter(
                fmt=(
                    "%(asctime)s - %(name)s - %(levelname)s - "
                    "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
                ),
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to set up file logging: {e}")

    # === Null Handler to Prevent "No Handlers" Warning ===
    logging.getLogger(__name__).addHandler(logging.NullHandler())

    logger.info("Logging is configured successfully.")


def configure_multiprocessing_logging(log_queue, log_level=logging.INFO):
    """
    Configure logging for multiprocessing worker processes to ensure logs from child processes
    are correctly displayed in the main process via a log queue.
    """
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)

    # Use a QueueHandler to send all logs to the queue
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)


def log_listener_process(log_queue, log_level=logging.INFO, log_file_name=None):
    """
    A listener process that receives logs from child processes via a queue
    and logs them to a file and console. The handlers are configured within this
    process to avoid passing non-pickleable objects between processes.
    """
    # Configure the root logger in this child process
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s: %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()

    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        "%(asctime)s: %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Configure file handler if a log_file_name is specified
    if log_file_name:
        try:
            # Define log directory within user's Documents
            log_dir = Path.home() / "Documents" / "MyAppLogs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / log_file_name

            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(
                "%(asctime)s: %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logging.error(f"Failed to set up file logging in listener: {e}")

    logging.info("Log listener process started.")

    # Listen for log records on the queue
    while True:
        try:
            record = log_queue.get()
            if record is None:  # We send None to signal the listener to exit
                logging.info("Log listener process received shutdown signal.")
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception as e:
            logging.error(f"Error in log listener: {e}")
            import traceback

            traceback.print_exc(file=sys.stderr)

    logging.info("Log listener process is shutting down.")
