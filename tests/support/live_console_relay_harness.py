"""Real spawned-worker and Rich Live smoke for parent-owned console logging."""

from __future__ import annotations

import multiprocessing
import sys
import time
from pathlib import Path

from loguru import logger

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import LogQueue, setup_logger, worker_initializer


def worker(queue: LogQueue) -> None:
    worker_initializer(queue)
    for index in range(40):
        logger.info("RELAY_PROGRESS {:03d}", index)
        if index in (9, 29):
            logger.warning("RELAY_WARNING {}", index)
        time.sleep(0.003)


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    logfile = Path(sys.argv[1])
    multiprocessing.set_start_method("spawn", force=True)
    dashboard = LiveWorkflowDashboard(title="TUFLOW Log Summary", max_rows=5, clear_before_exit=True)
    dashboard.set_tasks([f"Log {index:03d}" for index in range(40)])
    dashboard.set_phase("Processing")
    context = multiprocessing.get_context("spawn")
    with setup_logger(
        console_log_level="INFO",
        file_log_level="DEBUG",
        log_file=str(logfile),
        console_include_source=False,
        console_in_parent=True,
    ) as log_queue:
        with dashboard, log_queue.drain_console_on_exit():
            process = context.Process(target=worker, args=(log_queue,))
            process.start()
            for index in range(1, 41):
                dashboard.mark_running(index=index, refresh=False)
                dashboard.mark_finished(index=index, status="OK")
                time.sleep(0.002)
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                msg = "Worker did not finish"
                raise RuntimeError(msg)
            if process.exitcode != 0:
                msg = f"Worker exited with status {process.exitcode}"
                raise RuntimeError(msg)
            if not log_queue.flush_console(timeout_seconds=10.0):
                msg = "Console relay did not drain"
                raise RuntimeError(msg)
            dashboard.set_phase("Complete")
            dashboard.finish(outcome="SUCCESS", detail="40 completed run(s)")
    print("CONSOLE_RELAY_CLOSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
