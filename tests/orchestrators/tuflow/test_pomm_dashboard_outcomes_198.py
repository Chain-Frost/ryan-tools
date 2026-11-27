"""POMM mean/median dashboard and exporter outcome regressions (#198)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.tuflow import pomm_utils
from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress
from ryan_library.orchestrators.tuflow import pomm_max_items
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


@pytest.mark.parametrize("kind", ["mean", "median"])
@pytest.mark.parametrize("enabled", [False, True])
def test_pomm_peak_reports_finish_only_after_export(kind: str, enabled: bool, tmp_path: Path) -> None:
    report = pomm_max_items.export_mean_peak_report if kind == "mean" else pomm_max_items.export_median_peak_report
    exporter_name = "save_peak_report_mean" if kind == "mean" else "save_peak_report_median"
    with (
        patch.object(pomm_max_items, "aggregated_from_paths", return_value=pd.DataFrame({"AbsMax": [1.5]})),
        patch.object(pomm_max_items, exporter_name) as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finished,
    ):
        assert report(script_directory=tmp_path, use_live_dashboard=enabled)
        exporter.assert_called_once()
        assert finished.call_args is not None
        assert finished.call_args.kwargs["outcome"] == "SUCCESS"
        assert exporter.call_args.kwargs["aggregated_df"]["AbsMax"].tolist() == [1.5]


@pytest.mark.parametrize("kind", ["mean", "median"])
def test_pomm_peak_empty_is_a_skip(kind: str, tmp_path: Path) -> None:
    report = pomm_max_items.export_mean_peak_report if kind == "mean" else pomm_max_items.export_median_peak_report
    exporter_name = "save_peak_report_mean" if kind == "mean" else "save_peak_report_median"
    with (
        patch.object(pomm_max_items, "aggregated_from_paths", return_value=pd.DataFrame()),
        patch.object(pomm_max_items, exporter_name) as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finished,
    ):
        assert report(script_directory=tmp_path, use_live_dashboard=False)
        exporter.assert_not_called()
        assert finished.call_args is not None
        assert finished.call_args.kwargs["outcome"] == "SKIPPED"


@pytest.mark.parametrize("kind", ["mean", "median"])
def test_pomm_peak_writer_failure_marks_failed(kind: str, tmp_path: Path) -> None:
    report = pomm_max_items.export_mean_peak_report if kind == "mean" else pomm_max_items.export_median_peak_report
    exporter_name = "save_peak_report_mean" if kind == "mean" else "save_peak_report_median"
    with (
        patch.object(pomm_max_items, "aggregated_from_paths", return_value=pd.DataFrame({"AbsMax": [1.5]})),
        patch.object(pomm_max_items, exporter_name, side_effect=PermissionError("locked report")),
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finished,
        pytest.raises(PermissionError, match="locked report"),
    ):
        report(script_directory=tmp_path, use_live_dashboard=False)
    assert finished.call_args is not None
    assert finished.call_args.kwargs["outcome"] == "FAILED"


def test_pomm_invalid_dashboard_settings_precede_aggregation(tmp_path: Path) -> None:
    with (
        patch.object(pomm_max_items, "aggregated_from_paths") as aggregate,
        pytest.raises(ValueError, match="live_max_rows"),
    ):
        pomm_max_items.export_mean_peak_report(
            script_directory=tmp_path,
            live_max_rows=0,
            use_live_dashboard=False,
        )
    aggregate.assert_not_called()


@pytest.mark.parametrize("kind", ["mean", "median"])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("incomplete", [False, True])
def test_pomm_partial_input_never_exports(kind: str, enabled: bool, incomplete: bool, tmp_path: Path) -> None:
    """A valid contributor cannot conceal a failed or missing file outcome."""
    report = pomm_max_items.export_mean_peak_report if kind == "mean" else pomm_max_items.export_median_peak_report
    exporter_name = "save_peak_report_mean" if kind == "mean" else "save_peak_report_median"

    def aggregate(**kwargs: object) -> pd.DataFrame:
        progress = kwargs["batch_progress"]
        assert isinstance(progress, FileBatchProgress)
        progress.reset(scheduled=2)
        progress.observe("OK")
        if not incomplete:
            progress.observe("FAIL")
        return pd.DataFrame({"AbsMax": [1.5]})

    with (
        patch.object(pomm_max_items, "aggregated_from_paths", side_effect=aggregate),
        patch.object(pomm_max_items, exporter_name) as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        pytest.raises(RuntimeError, match=r"input\(s\) failed"),
    ):
        report(script_directory=tmp_path, use_live_dashboard=enabled)

    exporter.assert_not_called()
    assert finish.call_args is not None
    assert finish.call_args.kwargs["outcome"] == "FAILED"


def test_pomm_file_accounting_reaches_shared_executor(tmp_path: Path) -> None:
    """Preserve the file-level contract through all three POMM helpers."""
    progress = FileBatchProgress()
    with (
        patch.object(pomm_utils, "collect_files", return_value=[tmp_path / "run_POMM.csv"]),
        patch.object(pomm_utils, "process_files_in_parallel", return_value=ProcessorCollection()) as process,
    ):
        result = pomm_utils.aggregated_from_paths(paths=[tmp_path], log_queue=MagicMock(), batch_progress=progress)

    assert result.empty
    assert process.call_args is not None
    assert process.call_args.kwargs["batch_progress"] is progress


def test_pomm_empty_scan_clears_reused_file_accounting(tmp_path: Path) -> None:
    """A valid empty scan must not inherit a previous failure count."""
    progress = FileBatchProgress()
    progress.reset(scheduled=1)
    progress.observe("FAIL")
    with patch.object(pomm_utils, "collect_files", return_value=[]):
        result = pomm_utils.aggregated_from_paths(paths=[tmp_path], log_queue=MagicMock(), batch_progress=progress)

    assert result.empty
    assert progress.scheduled == progress.failed == progress.completed == 0
