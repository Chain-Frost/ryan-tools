# ryan_library/functions/loguru_helpers.py

import sys
import os
from loguru import logger
from multiprocessing import Process, Queue
import atexit
import pickle

# Centralized Configuration
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {module}:{function}:{line} - {message}"
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_COLORIZE = True
ROTATION = "10 MB"
RETENTION = "10 days"
COMPRESSION = "zip"


def worker_initializer(queue: Queue) -> None:
    """
    Initializer function for worker processes in a multiprocessing Pool.
    Configures the logger to send log records to the centralized logging queue.

    Parameters:
        queue (Queue): The multiprocessing queue to send log records to.
    """
    worker_configurer(queue)


def listener_process(
    queue: Queue, log_file: str | None = None, console_log_level: str = "INFO"
) -> None:
    """
    Listener process that receives log records from worker processes and writes them to the configured sinks.

    Parameters:
        queue (Queue): The multiprocessing queue to receive log records.
        log_file (str): Path to the log file. If None, file logging is disabled.
        console_log_level (str): Minimum log level for console output.
    """
    logger.remove()  # Remove any default handlers

    if log_file:
        logger.add(
            log_file,
            level="DEBUG",  # Capture all levels; filtering is done by sinks
            format=FILE_FORMAT,
            rotation=ROTATION,
            retention=RETENTION,
            compression=COMPRESSION,
            backtrace=True,
            diagnose=True,
            enqueue=False,  # Not using Loguru's internal queue
        )

    # Configure console sink
    logger.add(
        sys.stdout,
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
            if record is None:
                # Sentinel received, shutting down
                break
            # Deserialize the log record
            message = pickle.loads(record)

            # Filter out logs from the 'loguru_helpers' module
            if message.get("file") == "loguru_helpers.py":
                continue

            # Extract necessary fields
            level = message.get("level").name
            msg = message.get("message")
            module = message.get("module")
            function = message.get("function")
            line = message.get("line")
            exception = message.get("exception")

            if exception:
                # If there's an exception, use logger.opt to include it
                logger.opt(exception=exception).log(
                    level, f"{module}:{function}:{line} - {msg}"
                )
            else:
                logger.log(level, f"{module}:{function}:{line} - {msg}")
        except Exception:
            logger.opt(exception=True).error("Error in logging listener")


def worker_configurer(queue: Queue) -> None:
    """
    Configures the logger for a worker process to send log records to the listener via the queue.

    Parameters:
        queue (Queue): The multiprocessing queue to send log records.
    """

    class QueueSink:
        """
        Custom sink that serializes log messages and sends them to a multiprocessing queue.
        """

        def __init__(self, queue) -> None:
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
    """
    Context manager to set up Loguru with multiprocessing support, optional colored console, and dynamic log levels.
    """

    def __init__(
        self,
        log_file: str | None = None,
        console_log_level: str = "INFO",
    ) -> None:
        """
        Initialize the logger.

        Parameters:
            log_file (str or None): Path to the log file. If None, file logging is disabled.
            console_log_level (str): Minimum log level for console output.
        """
        self.log_file = log_file
        self.console_log_level = console_log_level
        self.queue = Queue()
        self.listener = None

    def __enter__(self):
        # Start the listener process
        self.listener = Process(
            target=listener_process,
            args=(
                self.queue,
                self.log_file,
                self.console_log_level,
            ),
        )
        self.listener.start()

        # Configure the root logger to send messages to the queue
        worker_configurer(self.queue)

        # Ensure the listener is shut down properly
        atexit.register(self.shutdown)

        return self.queue  # Return the queue to be used by the main process if needed

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()

    def shutdown(self) -> None:
        if self.listener and self.listener.is_alive():
            self.queue.put(None)  # Sentinel to shut down the listener
            self.listener.join(timeout=5)
            if self.listener.is_alive():
                self.listener.terminate()
            self.listener = None


def setup_logger(
    log_file: str | None = None,
    console_log_level: str = "INFO",
) -> LoguruMultiprocessingLogger:
    """
    Convenience function to set up logging using a context manager.

    Parameters:
        log_file (str or None): Path to the log file. If None, file logging is disabled.
        console_log_level (str): Minimum log level for console output.

    Usage:
        with setup_logger("my_log.log") as log_queue:
            # Your multiprocessing code here
    """
    # If log_file is a relative path, make it relative to the current working directory
    if log_file and not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)

    return LoguruMultiprocessingLogger(
        log_file=log_file,
        console_log_level=console_log_level,
    )


def add_file_sink(
    log_file: str,
) -> None:
    """
    Adds an additional file sink to the logger. Useful for adding more sinks after initialization.

    Parameters:
        log_file (str): Path to the log file.
    """
    # Ensure the log_file path is absolute
    if not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)

    logger.add(
        log_file,
        level="DEBUG",  # Capture all levels; filtering is done by sinks
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
    if err:
        msg: str = "An exception occurred" + err
    else:
        msg = "An exception occurred"
    logger.exception(msg)
