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
            msg = "synthetic worker failure"
            raise ValueError(msg)
        except ValueError:
            logger.exception("DASH_WORKER_ERROR {}", value)
        msg = "synthetic worker failure"
        raise ValueError(msg)
    return value * 3


def fast_worker(value: int) -> int:
    """Tiny work item: exercise queues and pool shutdown under high throughput."""
    return value


def transport_worker(_value: int) -> int:
    msg = "synthetic pool OSError"
    raise OSError(msg)


def status(_result: int) -> WorkflowStatus:
    return "OK"


def detail(result: int) -> str:
    return str(result)


def _interrupt_after_one(_outcome: object) -> None:
    raise KeyboardInterrupt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--dashboard", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--interrupt-on-outcome", action="store_true")
    parser.add_argument("--fast-count", type=int, default=0)
    parser.add_argument("--pool-oserror", action="store_true")
    args = parser.parse_args()
    multiprocessing.set_start_method("spawn", force=True)
    dashboard = LiveWorkflowDashboard(title="Spawn smoke", enabled=args.dashboard, max_rows=3)
    dashboard.set_tasks([f"item {index}" for index in range(args.fast_count)] if args.fast_count else ["a", "b", "c"])
    with setup_logger(console_log_level="WARNING", log_file=str(args.log_file), file_log_level="DEBUG") as log_queue:
        if args.fast_count:
            with dashboard:
                report = run_dashboard_workflow_report(
                    items=list(range(args.fast_count)),
                    process_item=fast_worker,
                    dashboard=dashboard,
                    pool_size=2,
                    status_for_result=status,
                    detail_for_result=detail,
                    log_queue=log_queue,
                    max_start_events=4,
                    poll_interval=0.01,
                )
                assert len(report.outcomes) == args.fast_count
                assert all(outcome.status == "OK" for outcome in report.outcomes)
                dashboard.finish(outcome="SUCCESS", detail=f"{args.fast_count} items")
            logger.success("FAST_BATCH_COMPLETE {}", args.fast_count)
            return 0
        if args.pool_oserror:
            try:
                run_dashboard_workflow_report(
                    items=[1],
                    process_item=transport_worker,
                    dashboard=dashboard,
                    pool_size=2,
                    status_for_result=status,
                    detail_for_result=detail,
                    log_queue=log_queue,
                )
            except OSError:
                logger.warning("POOL_OSERROR_PROPAGATED")
                return 0
            return 1
        try:
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
                    on_outcome=_interrupt_after_one if args.interrupt_on_outcome else None,
                )
                dashboard.set_phase("Exporting")
                dashboard.finish(outcome="FAILED", detail="one synthetic worker failed")
        except KeyboardInterrupt:
            dashboard.finish(outcome="INTERRUPTED", detail="synthetic cancellation")
            logger.warning("CANCELLED_PARENTSIDE")
            return 130
        assert [o.status for o in report.outcomes] == ["OK", "FAIL", "OK"]
        assert report.results == [3, 9]
        assert isinstance(report.outcomes[1].error, ValueError)
        logger.success("SPAWN_RESULT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
