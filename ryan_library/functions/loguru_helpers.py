# ryan_library/functions/loguru_helpers.py

from loguru import logger
from pathlib import Path
import sys
from multiprocessing import Process, Queue
import multiprocessing
import time

# Define a sentinel value for shutting down the logging listener
SENTINEL = "STOP"


def setup_logger(
    log_level: str = "INFO",
    log_file: str = None | None,
    log_dir: Path | None = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    enable_color: bool = True,
) -> None:
    """
    Configures Loguru logger with console and optional file handlers.

    Args:
        log_level (str): Logging level (e.g., "DEBUG", "INFO").
        log_file (str, optional): Name of the log file. If None, file logging is disabled.
        log_dir (Path, optional): Directory where log files will be stored. Defaults to ~/Documents/MyAppLogs.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        enable_color (bool): Whether to enable colored console logs.
    """
    # Remove default handlers
    logger.remove()

    # Define log directory
    if log_dir is None:
        log_dir = Path.home()
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
        else (
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function}:{line} - "
            "{message}"
        )
    )

    logger.add(
        sys.stdout,
        level=log_level,
        format=console_format,
        colorize=enable_color,
        enqueue=True,  # Necessary for multiprocessing
    )

    # File Handler
    if log_file:
        log_path = log_dir / log_file
        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function}:{line} - "
            "{message}"
        )
        logger.add(
            log_path,
            level=log_level,
            format=file_format,
            rotation=max_bytes,
            retention=backup_count,
            compression="zip",
            enqueue=True,  # Necessary for multiprocessing
        )


def worker_process(log_queue: Queue, worker_id: int):
    """
    Example worker process that logs messages.

    Args:
        log_queue (Queue): Queue for logging.
        worker_id (int): Identifier for the worker.
    """
    # Configure the logger to send logs to the queue
    logger.remove()
    logger.add(
        log_queue,
        enqueue=True,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )

    logger.info(f"Worker {worker_id} started.")
    for i in range(5):
        logger.debug(f"Worker {worker_id} is processing iteration {i}.")
        time.sleep(1)
    logger.info(f"Worker {worker_id} finished.")


def log_listener_process(
    log_queue: Queue,
    log_level: str,
    log_file: str,
    log_dir: Path,
    max_bytes: int,
    backup_count: int,
    enable_color: bool,
):
    """
    Listener process that receives log records from worker processes and logs them.

    Args:
        log_queue (Queue): Queue to receive log records from workers.
        log_level (str): Logging level.
        log_file (str): Log file name.
        log_dir (Path): Directory for log files.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        enable_color (bool): Whether to enable colored console logs.
    """
    setup_logger(
        log_level=log_level,
        log_file=log_file,
        log_dir=log_dir,
        max_bytes=max_bytes,
        backup_count=backup_count,
        enable_color=enable_color,
    )
    logger.info("Log listener started.")

    while True:
        try:
            record = log_queue.get()
            if record == SENTINEL:
                logger.info("Log listener received shutdown signal.")
                break
            logger.handle(record)
        except Exception as e:
            logger.exception(f"Exception in log listener: {e}")

    logger.info("Log listener stopped.")


def initialize_logging(
    log_level: str = "INFO",
    log_file: str = None,
    log_dir: Path = None,
    max_bytes: int = 10**6,
    backup_count: int = 5,
    enable_color: bool = True,
) -> tuple[Queue, Process]:
    """
    Initializes logging for multiprocessing by starting a listener process.

    Args:
        log_level (str): Logging level.
        log_file (str, optional): Name of the log file. If None, file logging is disabled.
        log_dir (Path, optional): Directory for log files. Defaults to ~/Documents/MyAppLogs.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        enable_color (bool): Whether to enable colored console logs.

    Returns:
        tuple[Queue, Process]: The logging queue and the listener process.
    """
    log_queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(
        target=log_listener_process,
        args=(
            log_queue,
            log_level,
            log_file,
            log_dir,
            max_bytes,
            backup_count,
            enable_color,
        ),
        name="LogListener",
        daemon=True,
    )
    listener.start()
    return log_queue, listener


def shutdown_logging(log_queue: Queue, listener: Process):
    """
    Shuts down the logging listener process gracefully.

    Args:
        log_queue (Queue): The logging queue.
        listener (Process): The listener process.
    """
    log_queue.put(SENTINEL)
    listener.join()


def setup_worker_logger(log_queue: Queue):
    """
    Configures the worker logger to send logs to the listener's queue.

    Args:
        log_queue (Queue): The logging queue.
    """
    logger.remove()
    logger.add(
        log_queue,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        enqueue=True,
    )


# Example usage (can be removed or commented out in the actual module)
if __name__ == "__main__":
    # Initialize logging
    log_queue, listener = initialize_logging(
        log_level="DEBUG",
        log_file="application.log",
        log_dir=Path.home() / "Documents" / "MyAppLogs",
        max_bytes=5 * 10**6,  # 5 MB
        backup_count=3,
        enable_color=True,
    )

    # Start worker processes
    workers = []
    for i in range(3):
        worker = multiprocessing.Process(
            target=worker_process,
            args=(log_queue, i),
            name=f"Worker-{i}",
        )
        worker.start()
        workers.append(worker)

    # Wait for all workers to finish
    for worker in workers:
        worker.join()

    # Shutdown logging
    shutdown_logging(log_queue, listener)
