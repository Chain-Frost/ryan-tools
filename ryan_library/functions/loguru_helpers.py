# ryan_library/functions/loguru_helpers.py

import sys
import os
import pickle
import atexit
from multiprocessing import Process, Queue
from loguru import logger

# Centralized Configuration
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {module}:{function}:{line} - {message}"
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_COLORIZE = True
ROTATION = "10 MB"
RETENTION = "10 days"
COMPRESSION = "zip"


def worker_initializer(queue: Queue) -> None:
    """Initializer for worker processes in a multiprocessing Pool.
    Configures the logger to send log records to the centralized logging queue."""
    worker_configurer(queue)


def listener_process(queue: Queue, log_file: str | None = None, console_log_level: str = "INFO") -> None:
    """Listener process that receives log records and writes them to the configured sinks.
    Parameters:
    queue (Queue): The multiprocessing queue to receive log records.
    log_file (str): Path to the log file. If None, file logging is disabled.
    console_log_level (str): Minimum log level for console output."""
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
    logger.add(
        sink=sys.stdout,
        level=console_log_level,
        format=CONSOLE_FORMAT,
        colorize=CONSOLE_COLORIZE,
        backtrace=True,
        diagnose=True,
        enqueue=False,
    )

    while True:
        try:
            record = queue.get()
            if record is None:  # sentinel
                break

            message = pickle.loads(record)

            # skip logs originating from this helper module
            if message.get("file") == "loguru_helpers.py":
                continue

            level = message["level"].name
            msg = message["message"]
            module = message["module"]
            function = message["function"]
            line = message["line"]
            exception = message.get("exception")

            if exception:
                logger.opt(exception=exception).log(level, f"{module}:{function}:{line} - {msg}")
            else:
                logger.log(level, f"{module}:{function}:{line} - {msg}")

        except Exception:
            logger.opt(exception=True).error("Error in logging listener")


def worker_configurer(queue: Queue) -> None:
    """Configures the logger for a worker process to send log records to the listener via the queue.
    Parameters:
        queue (Queue): The multiprocessing queue to send log records."""

    class QueueSink:
        """Custom sink that serializes log messages and sends them to a multiprocessing queue."""

        def __init__(self, queue: Queue) -> None:
            self.queue = queue

        def write(self, message) -> None:
            try:
                # Extract the Loguru record as a dictionary
                record = message.record.copy()
                # Retain all necessary information, including exceptions and extras
                # Removed key removal to ensure complete logging
                serialized = pickle.dumps(record)
                self.queue.put(serialized)
            except Exception:
                # If something goes wrong, write to stderr
                sys.stderr.write("Failed to send log message to listener.\n")

        def flush(self):
            pass  # No need to implement for queue sink

    logger.remove()  # Remove default handlers
    # Add the custom queue sink
    logger.add(QueueSink(queue), level="DEBUG", format=LOG_FORMAT)


class LoguruMultiprocessingLogger:
    """Context manager to set up Loguru with multiprocessing support."""

    def __init__(self, log_file: str | None = None, console_log_level: str = "INFO") -> None:
        """Initialize the logger.
        Parameters:
            log_file (str or None): Path to the log file. If None, file logging is disabled.
            console_log_level (str): Minimum log level for console output."""
        self.log_file = log_file
        self.console_log_level = console_log_level
        self.queue = Queue()
        self.listener: Process | None = None

    def __enter__(self) -> Queue:
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

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
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
