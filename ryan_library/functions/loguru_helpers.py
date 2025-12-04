# ryan_library/functions/loguru_helpers.py

import atexit
import multiprocessing
import os
import pickle
import sys
from multiprocessing import Process, Queue
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, TypeAlias, cast

from loguru import logger

# Centralized Configuration
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {module}:{function}:{line} - {message}"
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_COLORIZE = True
ROTATION = "10 MB"
RETENTION = "10 days"
COMPRESSION = "zip"

SerializedLogRecord: TypeAlias = dict[str, Any]
LogQueue: TypeAlias = Queue[bytes | None]


def worker_initializer(queue: LogQueue) -> None:
    """Initializer for worker processes in a multiprocessing Pool.
    Configures the logger to send log records to the centralized logging queue."""
    worker_configurer(queue=queue)


def reset_logging() -> None:
    """Reset loguru configuration by removing all sinks."""
    logger.remove()


def configure_serial_logging(console_log_level: str = "INFO", log_file: str | None = None) -> None:
    """Configure logging for a simple serial execution (no multiprocessing queue).

    Parameters:
        console_log_level (str): Minimum log level for console output.
            Valid options: "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL".
        log_file (str | None): Path to the log file. If None, file logging is disabled.
    """
    reset_logging()

    # Console sink
    logger.add(
        sink=sys.stdout,
        level=console_log_level,
        format=CONSOLE_FORMAT,
        colorize=CONSOLE_COLORIZE,
        backtrace=True,
        diagnose=True,
        enqueue=False,
    )

    # File sink, if requested
    if log_file:
        add_file_sink(log_file=log_file)


def listener_process(queue: LogQueue, log_file: str | None = None, console_log_level: str = "INFO") -> None:
    """Listener process that receives log records and writes them to the configured sinks.
    Parameters:
    queue (Queue): The multiprocessing queue to receive log records.
    log_file (str): Path to the log file. If None, file logging is disabled.
    console_log_level (str): Minimum log level for console output.
        Valid options: "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"."""
    logger.remove()  # Remove any default handlers

    # File sink, if requested
    if log_file:
        logger.add(
            sink=log_file,
            level="DEBUG",
            format=FILE_FORMAT,
            rotation=ROTATION,
            retention=RETENTION,
            compression=COMPRESSION,
            backtrace=True,
            diagnose=True,
            enqueue=False,  # Not using Loguru's internal queue
        )

    # Console sink
    # Use a format that doesn't include module/func/line because the message already has it
    # from the worker process, or we reconstruct it.
    # The worker sends: f"{module}:{function}:{line} - {msg}"
    # So we just need time and level here.
    logger.add(
        sink=sys.stdout,
        level=console_log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
        colorize=CONSOLE_COLORIZE,
        backtrace=True,
        diagnose=True,
        enqueue=False,
    )

    while True:
        try:
            queue_item: bytes | None = queue.get()
            if queue_item is None:  # sentinel
                break

            message: dict[str, Any] = cast(SerializedLogRecord, pickle.loads(queue_item))

            # skip logs originating from this helper module
            if message.get("file") == "loguru_helpers.py":
                continue

            level_obj = message.get("level")
            if level_obj is not None and hasattr(level_obj, "name"):
                level = str(level_obj.name)
            else:
                level = str(level_obj)
            msg = str(message.get("message", ""))
            module = str(message.get("module", ""))
            function = str(message.get("function", ""))
            line = int(message.get("line", 0))
            exception = message.get("exception")

            # Reconstruct the message with context
            # The console sink format is "{time} | {level} | {message}"
            # So {message} should be "module:func:line - original_msg"
            formatted_message: str = f"{module}:{function}:{line} - {msg}"

            if exception:
                logger.opt(exception=exception).log(level, formatted_message)
            else:
                logger.log(level, formatted_message)

        except Exception:
            logger.opt(exception=True).error("Error in logging listener")


def worker_configurer(queue: LogQueue) -> None:
    """Configures the logger for a worker process to send log records to the listener via the queue.
    Parameters:
        queue (Queue): The multiprocessing queue to send log records."""

    class QueueSink:
        """Custom sink that serializes log messages and sends them to a multiprocessing queue."""

        def __init__(self, queue: LogQueue) -> None:
            self.queue = queue

        def write(self, message: Any) -> None:
            try:
                # Extract the Loguru record as a dictionary
                record: dict[str, Any] = cast(dict[str, Any], message.record.copy())
                # Retain all necessary information, including exceptions and extras
                # Removed key removal to ensure complete logging
                serialized: bytes = pickle.dumps(record)
                self.queue.put(serialized)
            except Exception:
                # If something goes wrong, write to stderr
                sys.stderr.write("Failed to send log message to listener.\n")

        def flush(self) -> None:
            pass  # No need to implement for queue sink

    logger.remove()  # Remove default handlers
    # Add the custom queue sink
    logger.add(sink=QueueSink(queue=queue), level="DEBUG", format=LOG_FORMAT)


class LoguruMultiprocessingLogger:
    """Context manager to set up Loguru with multiprocessing support."""

    def __init__(self, log_file: str | None = None, console_log_level: str = "INFO") -> None:
        """Initialize the logger.
        Parameters:
            log_file (str or None): Path to the log file. If None, file logging is disabled.
            console_log_level (str): Minimum log level for console output.
                Valid options: "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"."""
        self.log_file: str | None = log_file
        self.console_log_level: str = console_log_level
        self.queue: LogQueue = cast(LogQueue, Queue())
        self.listener: Process | None = None

    def __enter__(self) -> LogQueue:
        # start the listener
        self.listener = Process(
            target=listener_process,
            args=(self.queue, self.log_file, self.console_log_level),
        )
        self.listener.start()

        # configure this (main) process’s logger to use the queue
        worker_configurer(self.queue)

        # ensure cleanup at exit
        atexit.register(self.shutdown)

        return self.queue

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown()

    def shutdown(self) -> None:
        # 1) stop listener if running
        if self.listener and self.listener.is_alive():
            self.queue.put(None)  # sentinel
            self.listener.join(timeout=5)
            if self.listener.is_alive():
                self.listener.terminate()
            self.listener = None

        # 2) remove all loguru sinks (including our queue sink)
        logger.remove()

        # 3) tear down the queue’s feeder thread so Python can exit
        self.queue.close()
        self.queue.join_thread()


def setup_logger(console_log_level: str = "INFO", log_file: str | None = None) -> LoguruMultiprocessingLogger:
    """Convenience factory for the multiprocessing logger context manager.

    Parameters:
        console_log_level (str): Minimum log level for console output.
            Valid options: "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL".
        log_file (str | None): Path to the log file.

    Usage:
        with setup_logger("DEBUG", "myapp.log"):
            ..."""
    if log_file and not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)
    return LoguruMultiprocessingLogger(log_file=log_file, console_log_level=console_log_level)


def add_file_sink(log_file: str) -> None:
    """Add an extra file sink to the logger (outside of multiprocessing context).
    Parameters:
        log_file (str): Path to the log file."""
    if not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)
    logger.add(
        sink=log_file,
        level="DEBUG",
        format=FILE_FORMAT,
        rotation=ROTATION,
        retention=RETENTION,
        compression=COMPRESSION,
        backtrace=True,
        diagnose=True,
        enqueue=False,
    )


def log_exception(err: str | None) -> None:
    """Logs the current exception with a stack trace."""
    msg: str = "An exception occurred" + (err or "")
    logger.exception(msg)


class LoggerManager:
    """Singleton wrapper around ``LoguruMultiprocessingLogger`` for
    backward compatibility."""

    _instance: ClassVar["LoggerManager | None"] = None
    _lock: ClassVar[Any] = multiprocessing.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> "LoggerManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_level: str = "INFO",
        log_file: str | None = "app.log",
        log_dir: Path | None = None,
        max_bytes: int = 10**6,
        backup_count: int = 5,
        enable_color: bool = True,
        additional_sinks: list[Any] | None = None,
    ) -> None:
        if getattr(self, "_initialized", False):
            return

        log_dir = Path(log_dir or os.getcwd())
        self._log_path = str((log_dir / log_file) if log_file else None)
        self._logger_context: LoguruMultiprocessingLogger | None = setup_logger(
            console_log_level=log_level, log_file=self._log_path
        )
        assert self._logger_context is not None
        self._log_queue: LogQueue = self._logger_context.__enter__()
        self._listener: Process | None = self._logger_context.listener
        self._initialized = True

    def shutdown(self) -> None:
        if self._logger_context is not None:
            self._logger_context.__exit__(None, None, None)
            self._logger_context = None


def worker_process(log_queue: LogQueue) -> None:
    """Compatibility wrapper that configures the logger for a worker process."""
    worker_configurer(log_queue)
