# ryan_library/functions/loguru_helpers.py

from loguru import logger
from typing import Any, Dict, Optional
from pathlib import Path
from multiprocessing import Process, Queue, Lock
import multiprocessing
import time
import signal
import sys
from contextlib import contextmanager

# Define a sentinel value for shutting down the logging listener
SENTINEL = "STOP"


class LoggerManager:
    """
    Manages the Loguru logger with multiprocessing support.
    Ensures a single listener process is running.
    Handles graceful shutdown on system signals.
    """

    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        """
        Implements the Singleton pattern to ensure only one instance exists.
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_level: str = "INFO",
        log_file: str | None = None,
        log_dir: Path | None = None,
        max_bytes: int = 10**6,  # 1 MB
        backup_count: int = 5,
        enable_color: bool = True,
        additional_sinks: list[Dict[str, Any]] | None = None,
    ):
        """
        Initializes the LoggerManager with the specified configuration.

        Args:
            log_level (str): Logging level (e.g., "DEBUG", "INFO").
            log_file (str, optional): Name of the log file. If None, file logging is disabled.
            log_dir (Path, optional): Directory where log files will be stored. Defaults to ~/Documents/MyAppLogs.
            max_bytes (int): Maximum size in bytes before rotating the log file.
            backup_count (int): Number of backup log files to keep.
            enable_color (bool): Whether to enable colored console logs.
            additional_sinks (list[dict[str, Any]], optional): List of additional sink configurations.
        """
        if not hasattr(self, "_initialized"):
            self.log_level = log_level
            self.log_file = log_file
            self.log_dir = log_dir or Path.home() / "Documents" / "MyAppLogs"
            self.max_bytes = max_bytes
            self.backup_count = backup_count
            self.enable_color = enable_color
            self.additional_sinks = additional_sinks or []
            self._log_queue, self._listener = self._initialize_logging()
            self._setup_signal_handlers()
            self._initialized = True

    def _initialize_logging(self) -> tuple[Queue, Process]:
        """
        Initializes the logging queue and starts the listener process.

        Returns:
            tuple[Queue, Process]: The logging queue and the listener process.
        """
        log_queue = multiprocessing.Queue(-1)
        listener = multiprocessing.Process(
            target=self.log_listener_process,
            args=(
                log_queue,
                self.log_level,
                self.log_file,
                self.log_dir,
                self.max_bytes,
                self.backup_count,
                self.enable_color,
                self.additional_sinks,
            ),
            name="LogListener",
            daemon=True,
        )
        listener.start()
        return log_queue, listener

    @staticmethod
    def log_listener_process(
        log_queue: Queue,
        log_level: str,
        log_file: str | None,
        log_dir: Path,
        max_bytes: int,
        backup_count: int,
        enable_color: bool,
        additional_sinks: list[Dict[str, Any]],
    ) -> None:
        """
        Listener process that receives log records from worker processes and logs them.
        """
        setup_logger(
            log_level=log_level,
            log_file=log_file,
            log_dir=log_dir,
            max_bytes=max_bytes,
            backup_count=backup_count,
            enable_color=enable_color,
            additional_sinks=additional_sinks,
            enqueue=False,  # Prevent listener's logs from being enqueued
        )
        logger.info("Log listener started.")

        while True:
            try:
                record = log_queue.get()
                if record == SENTINEL:
                    logger.info("Log listener received shutdown signal.")
                    break
                # Reconstruct Loguru's LogRecord from the received record
                if isinstance(record, dict):
                    level_obj = record.get("level")
                    # Safely get the level name
                    level = getattr(level_obj, "name", "INFO") if level_obj else "INFO"
                    message = record.get("message", "")
                    logger.opt(depth=1).log(level, message)
                else:
                    logger.error(f"Received unexpected log record type: {type(record)}")
            except Exception as e:
                logger.exception(f"Exception in log listener: {e}")

        logger.info("Log listener stopped.")

    def shutdown(self):
        """
        Shuts down the logging listener process gracefully.
        Ensures all logs are processed before shutdown.
        """
        if self._log_queue and self._listener:
            # Wait until the queue is empty
            while not self._log_queue.empty():
                time.sleep(0.1)
            self._log_queue.put(SENTINEL)
            self._listener.join()

    def _setup_signal_handlers(self):
        """
        Sets up signal handlers to ensure graceful shutdown on termination signals.
        """
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Handles termination signals by shutting down the listener.
        """
        logger.info(f"Received signal {signum}. Shutting down logger.")
        self.shutdown()
        sys.exit(0)


def setup_logger(
    log_level: str = "INFO",
    log_file: str | None = None,
    log_dir: Path | None = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    enable_color: bool = True,
    additional_sinks: list[Dict[str, Any]] | None = None,
    enqueue: bool = True,  # New parameter
) -> None:
    """
    Configures Loguru logger with console, file, and additional sinks.
    """
    # Remove default handlers
    logger.remove()

    # Define log directory
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)

    # Console Handler
    console_format = (
        (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "{message}"
        )
        if enable_color
        else ("{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}")
    )

    logger.add(
        sys.stdout,
        level=log_level,
        format=console_format,
        colorize=enable_color,
        enqueue=enqueue,  # Use the parameter
    )

    # File Handler
    if log_file:
        if log_dir:
            log_path = log_dir / log_file
        else:
            log_path = log_file
        file_format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        logger.add(
            log_path,
            level=log_level,
            format=file_format,
            rotation=max_bytes,
            retention=backup_count,
            compression="zip",
            enqueue=enqueue,  # Use the parameter
        )

    # Additional Sinks
    additional_sinks = additional_sinks or []
    for sink_conf in additional_sinks:
        logger.add(**sink_conf)


def log_sink(record: Dict[str, Any], log_queue: Queue) -> None:
    """
    Helper function to send log records to the queue.
    """
    log_queue.put(record)


def initialize_worker_logger(log_queue: Queue, log_level: str = "INFO"):
    """
    Configures the worker logger to send logs to the listener's queue at the specified log level.
    """
    logger.remove()

    # Define a sink function that enqueues the log record as a dictionary
    def worker_log_sink(message: Any) -> None:
        log_queue.put(message.record)

    logger.add(
        worker_log_sink,
        level=log_level,
    )


def worker_process(log_queue: Queue, worker_id: int, log_level: str = "INFO"):
    """
    Example worker process that logs messages.
    """
    initialize_worker_logger(log_queue, log_level=log_level)

    context_logger = logger.bind(worker_id=worker_id)
    context_logger.info("Worker started.")
    for i in range(5):
        context_logger.debug(f"Processing iteration {i}.")  # This won't show if log_level=INFO
        time.sleep(1)
    context_logger.info("Worker finished.")


@contextmanager
def logging_context(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: Optional[Path] = None,
    max_bytes: int = 10**6,
    backup_count: int = 5,
    enable_color: bool = True,
    additional_sinks: Optional[list[Dict[str, Any]]] = None,
):
    """
    Context manager for setting up and tearing down logging.
    """
    logger_manager = LoggerManager(
        log_level=log_level,
        log_file=log_file,
        log_dir=log_dir,
        max_bytes=max_bytes,
        backup_count=backup_count,
        enable_color=enable_color,
        additional_sinks=additional_sinks,
    )
    try:
        yield logger_manager
    finally:
        logger_manager.shutdown()
