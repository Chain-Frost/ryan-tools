# main_script.py

from ryan_library.functions.loguru_helpers import LoggerManager, initialize_worker
from multiprocessing import Process
import time


def worker_task(worker_id) -> None:
    initialize_worker()
    from loguru import logger

    logger.info(f"Worker {worker_id} started.")
    for i in range(5):
        logger.debug(f"Worker {worker_id} processing iteration {i}.")
        time.sleep(1)
    logger.info(f"Worker {worker_id} finished.")


if __name__ == "__main__":
    # Initialize the LoggerManager
    logger_manager = LoggerManager(
        log_level="DEBUG",
        log_file="application.log",
        log_dir=None,  # Defaults to ~/Documents/MyAppLogs
        max_bytes=10**6,
        backup_count=5,
        enable_color=True,
    )

    from loguru import logger

    logger.info("Main process started.")

    # Create worker processes
    workers = []
    for i in range(3):
        p = Process(target=worker_task, args=(i,), name=f"Worker-{i}")
        p.start()
        workers.append(p)

    # Wait for all workers to finish
    for p in workers:
        p.join()

    logger.info("Main process finished.")

    # Shutdown the logger
    logger_manager.shutdown()


# ryan_library/other_module.py

from loguru import logger


def some_function() -> None:
    logger.debug("Debug message from some_function.")
    logger.info("Info message from some_function.")
    logger.warning("Warning message from some_function.")
    logger.error("Error message from some_function.")
