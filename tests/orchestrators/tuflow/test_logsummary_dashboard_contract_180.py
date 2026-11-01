"""Focused dashboard lifecycle, logging and export tests for TUFLOW log summaries."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.orchestrators.tuflow import tuflow_logsummary as ls
from ryan_library.orchestrators.tuflow.tuflow_logsummary import LogFileProcessingResult

ROOT = Path(__file__).absolute().parents[3]
HARNESS = ROOT / "tests" / "support" / "dashboard_spawn_harness.py"


@pytest.mark.parametrize("enabled", [False, True])
def test_real_spawn_diagnostics_and_debug_file(tmp_path: Path, enabled: bool) -> None:
    log = tmp_path / "details.log"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    args = [sys.executable, str(HARNESS), "--log-file", str(log)]
    if not enabled:
        args.append("--no-dashboard")
    run = subprocess.run(args, capture_output=True, text=True, cwd=ROOT, env=env, timeout=60, check=True)
    assert "DASH_WORKER_WARNING 1" in run.stdout
    assert "DASH_WORKER_WARNING 2" in run.stdout
    assert "DASH_WORKER_ERROR 2" in run.stdout
    assert "DASH_WORKER_DEBUG" not in run.stdout
    if enabled:
        assert run.stdout.count("Spawn smoke: FAILED") == 1
        assert "\x1b[" not in run.stdout
    else:
        assert "Spawn smoke: FAILED" not in run.stdout
    detail = log.read_text(encoding="utf-8")
    assert all(f"DASH_WORKER_DEBUG {n}" in detail for n in (1, 2, 3))
    assert all(f"DASH_WORKER_INFO {n}" in detail for n in (1, 2, 3))
    assert "DASH_WORKER_ERROR 2" in detail
    assert "ValueError: synthetic worker failure" in detail


@pytest.mark.parametrize("enabled", [False, True])
def test_processing_selection_order_and_results_do_not_depend_on_display(
    tmp_path: Path,
    enabled: bool,
) -> None:
    files = [tmp_path / "b.tlf", tmp_path / "a.tlf"]
    for path in files:
        path.write_text("dummy", encoding="utf-8")

    def parse(path: Path) -> pd.DataFrame:
        return pd.DataFrame({"Runcode": [path.stem], "StartDate": [1]})

    with (
        patch.object(ls, "calculate_pool_size", return_value=1),
        patch.object(ls, "_process_log_file_dataframe", side_effect=parse),
        patch.object(ls, "_log_processing_results") as logged,
    ):
        outcomes = ls.process_log_files(
            files=files,
            root_dir=tmp_path,
            use_live_dashboard=enabled,
            live_refresh_per_second=2.0,
            live_max_rows=5,
            log_queue=None,
            console_log_level="INFO",
        )
    assert [result.logfile.name for result in outcomes] == ["b.tlf", "a.tlf"]
    assert [result.status for result in outcomes] == ["OK", "OK"]
    assert [result.data_frame.iloc[0]["Runcode"] for result in outcomes] == ["b", "a"]
    assert logged.call_args.kwargs == {"processing_results": outcomes}


def test_success_and_partial_failure_keep_export_outcome_truthful(tmp_path: Path) -> None:
    good = tmp_path / "done.tlf"
    bad = tmp_path / "failed.tlf"
    frame = pd.DataFrame({"StartDate": [1], "Runcode": ["done"]})
    success = LogFileProcessingResult(good, frame, "OK", "1 row")
    failure = LogFileProcessingResult(bad, pd.DataFrame(), "FAIL", "broken")
    with (
        patch.object(ls, "discover_log_files", return_value=[good, bad]),
        patch.object(ls, "process_log_files", return_value=[success, failure]),
        patch.object(ls, "build_log_summary_dataframe", return_value=frame),
        patch.object(ls, "ExcelExporter") as exporter,
    ):
        assert ls.main_processing(use_live_dashboard=False) is False
        exporter.return_value.save_to_excel.assert_called_once()
        exporter.return_value.save_to_excel.side_effect = OSError("disk full")
        assert ls.main_processing(use_live_dashboard=False) is False


def test_export_failure_does_not_emit_success(tmp_path: Path) -> None:
    good = tmp_path / "done.tlf"
    frame = pd.DataFrame({"StartDate": [1], "Runcode": ["done"]})
    success = LogFileProcessingResult(good, frame, "OK", "1 row")
    with (
        patch.object(ls, "discover_log_files", return_value=[good]),
        patch.object(ls, "process_log_files", return_value=[success]),
        patch.object(ls, "build_log_summary_dataframe", return_value=frame),
        patch.object(ls, "ExcelExporter") as exporter,
    ):
        exporter.return_value.save_to_excel.side_effect = OSError("disk full")
        assert ls.main_processing(use_live_dashboard=True) is False


def test_infrastructure_error_propagates_and_no_premature_success(tmp_path: Path) -> None:
    log_file = tmp_path / "one.tlf"
    with (
        patch.object(ls, "discover_log_files", return_value=[log_file]),
        patch.object(ls, "process_log_files", side_effect=RuntimeError("transport failed")),
    ):
        with pytest.raises(RuntimeError, match="transport failed"):
            ls.main_processing(use_live_dashboard=True)


def test_no_completed_logs_is_skip_not_report_success() -> None:
    with patch.object(ls, "discover_log_files", return_value=[]):
        assert ls.main_processing(use_live_dashboard=True) is True


def test_binary_size_metadata_is_not_decimal_mb() -> None:
    assert ls._format_bytes(1024) == "1.0 KiB"
    assert ls._format_bytes(1024 * 1024) == "1.0 MiB"


def test_spawn_callback_interruption_cleans_up_pool_and_logging(tmp_path: Path) -> None:
    log = tmp_path / "interrupted.log"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    run = subprocess.run(
        [
            sys.executable,
            str(HARNESS),
            "--log-file",
            str(log),
            "--interrupt-on-outcome",
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
        timeout=60,
        check=False,
    )
    assert run.returncode == 130
    assert "CANCELLED_PARENTSIDE" in run.stdout
    assert run.stdout.count("Spawn smoke: INTERRUPTED") == 1
    assert "\x1b[" not in run.stdout
    assert log.is_file()


def test_many_fast_spawned_tasks_do_not_deadlock_at_pool_join(tmp_path: Path) -> None:
    """A bounded start queue must not block worker-exit feeder threads."""
    log = tmp_path / "fast.log"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    run = subprocess.run(
        [sys.executable, str(HARNESS), "--log-file", str(log), "--fast-count", "10000"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
        timeout=90,
        check=True,
    )
    assert "FAST_BATCH_COMPLETE 10000" in run.stdout
    assert "Spawn smoke: SUCCESS" in run.stdout
    assert "\x1b[" not in run.stdout


def test_real_spawn_pool_oserror_propagates_for_consumer_fallback(tmp_path: Path) -> None:
    log = tmp_path / "transport.log"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    run = subprocess.run(
        [sys.executable, str(HARNESS), "--log-file", str(log), "--pool-oserror"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
        timeout=60,
        check=True,
    )
    assert "POOL_OSERROR_PROPAGATED" in run.stdout
