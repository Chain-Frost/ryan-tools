"""Spawned-process integration smoke for the shared dashboard and Loguru sinks."""

from __future__ import annotations

import argparse
import multiprocessing
from pathlib import Path

from loguru import logger

from ryan_library.functions.dashboard_workflow import run_dashboard_workflow_report
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowStatus
from ryan_library.functions.loguru_helpers import setup_logger


def worker(value: int) -> int:
    logger.debug("DASH_WORKER_DEBUG {}", value)
    logger.info("DASH_WORKER_INFO {}", value)
    logger.warning("DASH_WORKER_WARNING {}", value)
    if value == 2:
        try:
            raise OSError("synthetic worker failure")
        except OSError:
            logger.exception("DASH_WORKER_ERROR {}", value)
        raise OSError("synthetic worker failure")
    return value * 3


def status(result: int) -> WorkflowStatus:
    return "OK"


def detail(result: int) -> str:
    return str(result)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--dashboard", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    multiprocessing.set_start_method("spawn", force=True)
    dashboard = LiveWorkflowDashboard(title="Spawn smoke", enabled=args.dashboard, max_rows=3)
    dashboard.set_tasks(["a", "b", "c"])
    with setup_logger(console_log_level="WARNING", log_file=str(args.log_file), file_log_level="DEBUG") as log_queue:
        with dashboard:
            dashboard.set_phase("Processing")
            report = run_dashboard_workflow_report(
                items=[1, 2, 3],
                process_item=worker,
                dashboard=dashboard,
                pool_size=2,
                status_for_result=status,
                detail_for_result=detail,
                log_queue=log_queue,
            )
            dashboard.set_phase("Exporting")
            dashboard.finish(outcome="FAILED", detail="one synthetic worker failed")
        assert [o.status for o in report.outcomes] == ["OK", "FAIL", "OK"]
        assert report.results == [3, 9]
        assert isinstance(report.outcomes[1].error, OSError)
        logger.success("SPAWN_RESULT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
