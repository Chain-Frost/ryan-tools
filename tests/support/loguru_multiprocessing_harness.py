"""Small spawn-process harness used by Loguru behavioural tests."""

from __future__ import annotations

import argparse
import multiprocessing
from pathlib import Path

from loguru import logger

from ryan_library.functions.loguru_helpers import LogQueue, setup_logger, worker_initializer


def emit_worker_logs(log_queue: LogQueue, raise_exception: bool) -> None:
    """Configure one worker and emit stable marker messages."""

    worker_initializer(log_queue)
    logger.debug("WORKER_DEBUG")
    logger.info("WORKER_INFO")
    logger.success("WORKER_SUCCESS")
    logger.warning("WORKER_WARNING")
    if raise_exception:
        try:
            raise ValueError("synthetic worker failure")
        except ValueError:
            logger.exception("WORKER_EXCEPTION")


def main() -> None:
    """Run the deterministic logging scenario requested on the command line."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--console-level", default="INFO")
    parser.add_argument("--log-file", type=Path)
    parser.add_argument("--file-level", default="DEBUG")
    parser.add_argument("--raise-worker-exception", action="store_true")
    args = parser.parse_args()

    with setup_logger(
        console_log_level=args.console_level,
        log_file=str(args.log_file) if args.log_file else None,
        file_log_level=args.file_level,
    ) as log_queue:
        logger.debug("MAIN_DEBUG")
        logger.info("MAIN_INFO")
        logger.success("MAIN_SUCCESS")
        context = multiprocessing.get_context("spawn")
        worker = context.Process(target=emit_worker_logs, args=(log_queue, args.raise_worker_exception))
        worker.start()
        worker.join(timeout=15)
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)
            raise RuntimeError("Logging harness worker did not stop.")
        if worker.exitcode != 0:
            raise RuntimeError(f"Logging harness worker exited with code {worker.exitcode}.")


if __name__ == "__main__":
    main()
