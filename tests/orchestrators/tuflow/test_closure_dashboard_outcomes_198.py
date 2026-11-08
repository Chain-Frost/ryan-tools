"""Whole-workflow closure dashboard and publication outcomes (#198)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress
from ryan_library.orchestrators.tuflow import closure_durations as closure


def _summary() -> pd.DataFrame:
    return pd.DataFrame({"Path": ["inputs"], "Location": ["C01"], "ThresholdFlow": [1.0], "AEP": ["1%"]})


@pytest.mark.parametrize("enabled", [False, True])
def test_closure_finishes_only_after_publication(tmp_path: Path, enabled: bool) -> None:
    collection = MagicMock()
    collection.processors = [MagicMock()]
    with (
        patch.object(closure, "bulk_read_and_merge_tuflow_csv", return_value=collection),
        patch.object(closure, "_collect_po_data", return_value=pd.DataFrame({"Value": [2.0]})),
        patch.object(
            closure, "_calculate_threshold_durations", return_value=pd.DataFrame({"Duration_Exceeding": [1.0]})
        ),
        patch.object(closure, "summarise_results", return_value=_summary()),
        patch.object(closure, "_export_closure_duration_artifacts") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
    ):
        assert closure.run_closure_durations(paths=[tmp_path], thresholds=[1.0], use_live_dashboard=enabled)
        exporter.assert_called_once()
        assert finish.call_args is not None
        assert finish.call_args.kwargs["outcome"] == "SUCCESS"


@pytest.mark.parametrize("empty_stage", ["processors", "po_rows", "durations"])
def test_closure_valid_empty_is_skipped(tmp_path: Path, empty_stage: str) -> None:
    collection = MagicMock()
    collection.processors = [] if empty_stage == "processors" else [MagicMock()]
    po = pd.DataFrame() if empty_stage == "po_rows" else pd.DataFrame({"Value": [2.0]})
    duration = pd.DataFrame() if empty_stage == "durations" else pd.DataFrame({"Duration_Exceeding": [1.0]})
    with (
        patch.object(closure, "bulk_read_and_merge_tuflow_csv", return_value=collection),
        patch.object(closure, "_collect_po_data", return_value=po),
        patch.object(closure, "_calculate_threshold_durations", return_value=duration),
        patch.object(closure, "_export_closure_duration_artifacts") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
    ):
        assert closure.run_closure_durations(paths=[tmp_path], thresholds=[1.0], use_live_dashboard=False)
        exporter.assert_not_called()
        assert finish.call_args is not None
        assert finish.call_args.kwargs["outcome"] == "SKIPPED"


@pytest.mark.parametrize("failure_at", ["processing", "export"])
def test_closure_failure_never_finishes_as_success(tmp_path: Path, failure_at: str) -> None:
    collection = MagicMock()
    collection.processors = [MagicMock()]
    with (
        patch.object(closure, "bulk_read_and_merge_tuflow_csv", return_value=collection) as read,
        patch.object(closure, "_collect_po_data", return_value=pd.DataFrame({"Value": [2.0]})),
        patch.object(
            closure, "_calculate_threshold_durations", return_value=pd.DataFrame({"Duration_Exceeding": [1.0]})
        ),
        patch.object(closure, "summarise_results", return_value=_summary()),
        patch.object(closure, "_export_closure_duration_artifacts") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
    ):
        if failure_at == "processing":
            read.side_effect = OSError("synthetic read denial")
        else:
            exporter.side_effect = PermissionError("synthetic export denial")
        with pytest.raises(OSError, match="synthetic"):
            closure.run_closure_durations(paths=[tmp_path], thresholds=[1.0], use_live_dashboard=False)
        assert finish.call_args is not None
        assert finish.call_args.kwargs["outcome"] == "FAILED"


def test_closure_invalid_display_settings_fail_before_processing(tmp_path: Path) -> None:
    with (
        patch.object(closure, "bulk_read_and_merge_tuflow_csv") as read,
        pytest.raises(ValueError, match="live_max_rows"),
    ):
        closure.run_closure_durations(paths=[tmp_path], thresholds=[1.0], live_max_rows=0)
    read.assert_not_called()


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("incomplete", [False, True])
def test_closure_partial_processor_batch_never_publishes(tmp_path: Path, enabled: bool, incomplete: bool) -> None:
    """Missing and failed PO inputs fail the entire export, not just a row."""
    collection = MagicMock()
    collection.processors = [MagicMock()]

    def read(**kwargs: object) -> MagicMock:
        progress = kwargs["batch_progress"]
        assert isinstance(progress, FileBatchProgress)
        progress.reset(scheduled=2)
        progress.observe("OK")
        if not incomplete:
            progress.observe("FAIL")
        return collection

    with (
        patch.object(closure, "bulk_read_and_merge_tuflow_csv", side_effect=read),
        patch.object(closure, "_collect_po_data") as aggregate,
        patch.object(closure, "_export_closure_duration_artifacts") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        pytest.raises(RuntimeError, match=r"input\(s\) failed"),
    ):
        closure.run_closure_durations(paths=[tmp_path], thresholds=[1.0], use_live_dashboard=enabled)

    aggregate.assert_not_called()
    exporter.assert_not_called()
    assert finish.call_args is not None
    assert finish.call_args.kwargs["outcome"] == "FAILED"
