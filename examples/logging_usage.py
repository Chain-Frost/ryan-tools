"""
Logging Usage Example
=====================

This example demonstrates how to correctly configure and use logging within the
ryan-tools repository. It covers:
1. Basic configuration (resetting and adding sinks).
2. Multiprocessing logging (using the listener/queue pattern).
3. Message formatting best practices.

Usage:
    python examples/logging_usage.py
"""

import sys
import multiprocessing as mp
from loguru import logger
from pathlib import Path

# Ensure the library is in the path for this example
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ryan_library.functions.loguru_helpers import (
    reset_logging,
    setup_logger,
    worker_initializer,
    configure_serial_logging,
)


def worker_task(x: int) -> int:
    """A simple worker task that logs messages."""
    logger.debug(f"Worker processing item: {x}")
    if x % 2 == 0:
        logger.info(f"Item {x} is even.")
    else:
        logger.warning(f"Item {x} is odd.")
    return x * x


def main():
    # 1. Basic Configuration (Serial)
    # Use the helper to configure logging for a standard serial script.
    # This handles resetting and setting up the console sink with the standard format.
    configure_serial_logging(console_log_level="INFO")

    logger.info("=== Starting Serial Example ===")
    logger.info("This is an info message.")
    logger.debug("This debug message will be hidden because level is INFO.")

    # 2. Multiprocessing Logging
    logger.info("=== Starting Multiprocessing Example ===")

    # Use the LoguruMultiprocessingLogger context manager (via setup_logger)
    # This starts a listener process and configures the main process to send logs to a queue.
    # The 'console_log_level' determines what the listener prints to stdout.
    with setup_logger(console_log_level="DEBUG") as queue:
        # Create a pool using 'spawn' (recommended for Windows/consistency)
        ctx = mp.get_context("spawn")

        with ctx.Pool(
            processes=2,
            initializer=worker_initializer,  # Critical: configures workers to use the queue
            initargs=(queue,),
        ) as pool:
            results = pool.map(worker_task, range(4))

        logger.success(f"Multiprocessing complete. Results: {results}")

    # 3. Message Formatting Best Practices
    logger.info("=== Formatting Best Practices ===")

    value = 42

    # PREFERRED: f-strings for user-facing logs (INFO, SUCCESS, WARNING, ERROR)
    # These are eagerly evaluated but readable and standard.
    logger.info(f"The value is {value}")

    # PREFERRED: Lazy formatting for DEBUG/TRACE logs
    # These are only evaluated if the level is enabled, saving performance in production.
    logger.debug("The value is {}", value)

    logger.info("Example complete.")

    # 4. Log Levels
    # Use the following guidelines when choosing a log level:
    #
    # TRACE (5):    Very low-level details, loop iterations, large data dumps.
    #               Use for deep debugging of complex algorithms.
    #
    # DEBUG (10):   Diagnostic information useful for developers.
    #               Variable states, flow control, internal events.
    #               Use lazy formatting: logger.debug("Value: {}", x)
    #
    # INFO (20):    General operational events. User-facing progress updates.
    #               "Starting process X", "Completed Y", "Loaded Z".
    #               Use f-strings: logger.info(f"Processing file {path}")
    #
    # SUCCESS (25): Positive confirmation of a significant workflow completion.
    #               "Multiprocessing run finished successfully".
    #
    # WARNING (30): Something unexpected happened, but execution can continue.
    #               "File not found, skipping", "Deprecated argument used".
    #
    # ERROR (40):   A specific operation failed, but the app might keep running.
    #               "Failed to process row 5", "Connection timeout".
    #
    # CRITICAL (50): Fatal error, the application cannot continue.
    #                "Out of memory", "Database corruption".


if __name__ == "__main__":
    main()
