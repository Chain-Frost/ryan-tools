# ryan_library/functions/loguru_helpers.py

from loguru import logger
import multiprocessing
from multiprocessing import Process, Queue
from pathlib import Path
import sys
import signal
from typing import Any, List, Optional, Dict
from contextlib import contextmanager
import json
from pprint import pprint

# Define a sentinel value for shutting down the logging listener
SENTINEL = "STOP"


def pool_initializer(log_queue: Any) -> None:
    """
    Initializer for each worker process to set up logging.

    Args:
        log_queue (Queue): The shared logging queue.
    """
    initialize_worker(log_queue)


def listener_process(
    queue: Queue,
    log_level: str,
    log_file: Optional[str],
    log_dir: Path,
    max_bytes: int,
    backup_count: int,
    enable_color: bool,
    additional_sinks: Optional[list[dict[str, Any]]],
) -> None:
    """
    Listener process that consumes log records from the queue and logs them using Loguru.
    """
    logger.remove()  # Remove default handlers

    # Console handler
    logger.add(
        sys.stdout,
        level=log_level,
        colorize=enable_color,
        enqueue=False,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - {message}"
        ),
    )

    # File handler
    if log_file:
        logger.add(
            log_dir / log_file,
            level=log_level,
            rotation=max_bytes,
            retention=backup_count,
            compression="zip",
            enqueue=False,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        )

    # Additional sinks if any
    if additional_sinks:
        for sink in additional_sinks:
            logger.add(**sink)

    logger.info("Log listener started.")

    while True:
        try:
            record = queue.get()
            if record == SENTINEL:
                logger.info("Log listener received shutdown signal.")
                break
            # Deserialize the JSON log record
            try:
                record_dict = json.loads(record)
                # Manually extract level and message
                level = record_dict["record"]["level"]["name"]
                message = record_dict["record"]["message"].strip()

                # Debug: Print extracted level and message
                # print(f"Logging level: {level}, message: {message}")

                # Log the message using Loguru
                logger.log(level, message)
            except json.JSONDecodeError:
                logger.error("Received a non-JSON log record.")
            except KeyError as e:
                logger.error(f"Missing expected key in log record: {e}")
            except Exception as e:
                logger.exception(f"Error processing log record: {e}")
        except Exception:
            logger.exception("Error in log listener.")

    logger.info("Log listener stopped.")


def setup_listener(
    log_queue: Queue,
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: Optional[Path] = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    enable_color: bool = True,
    additional_sinks: Optional[list[dict[str, Any]]] = None,
) -> Process:
    """
    Sets up the Loguru listener process that consumes log records from the queue.
    """
    log_dir = log_dir or Path.home() / "Documents" / "MyAppLogs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Start the listener process using the top-level listener_process
    listener = Process(
        target=listener_process,
        args=(
            log_queue,
            log_level,
            log_file,
            log_dir,
            max_bytes,
            backup_count,
            enable_color,
            additional_sinks,
        ),
        name="LogListener",
        daemon=True,
    )
    listener.start()
    return listener


def worker_configurer(log_queue: Queue):
    """
    Configures the worker logger to send log records to the queue.
    """
    logger.remove()  # Remove default handlers

    # Define a sink that sends log records to the queue with serialization
    logger.add(log_queue.put, level="DEBUG", serialize=True)


class LoggerManager:
    """
    Singleton LoggerManager to manage Loguru logging across processes.
    """

    _instance = None
    _lock = multiprocessing.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_level: str = "INFO",
        log_file: Optional[str] = "app.log",
        log_dir: Optional[Path] = None,
        max_bytes: int = 10**6,
        backup_count: int = 5,
        enable_color: bool = True,
        additional_sinks: Optional[list[dict[str, Any]]] = None,
    ):
        if hasattr(self, "_initialized") and self._initialized:
            return

        # Only the main process should set up the listener
        if multiprocessing.current_process().name == "MainProcess":
            self.log_queue = multiprocessing.Queue(-1)
            self.listener = setup_listener(
                log_queue=self.log_queue,
                log_level=log_level,
                log_file=log_file,
                log_dir=log_dir,
                max_bytes=max_bytes,
                backup_count=backup_count,
                enable_color=enable_color,
                additional_sinks=additional_sinks,
            )
            self._setup_signal_handlers()
            self._initialized = True
        else:
            # In worker processes, do not start a listener
            self.log_queue = None
            self.listener = None
            self._initialized = True

    def _setup_signal_handlers(self):
        """
        Sets up signal handlers for graceful shutdown.
        """
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Handles termination signals.
        """
        logger.info(f"Received signal {signum}. Shutting down logger.")
        self.shutdown()
        sys.exit(0)

    def shutdown(self):
        """
        Shuts down the logging listener process.
        """
        if self.listener and self.listener.is_alive() and self.log_queue:
            self.log_queue.put(SENTINEL)
            self.listener.join(timeout=5)
            if self.listener.is_alive():
                self.listener.terminate()


@contextmanager
def logging_context(**logger_kwargs):
    """
    Context manager for setting up and tearing down logging.
    """
    manager = LoggerManager(**logger_kwargs)
    try:
        yield manager
    finally:
        manager.shutdown()


def initialize_worker(log_queue: Queue):
    """
    Initialize the worker process logger.
    Should be called at the start of each worker process.

    Args:
        log_queue (Queue): The shared logging queue.
    """
    worker_configurer(log_queue)
