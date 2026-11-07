"""POMM mean/median dashboard and exporter outcome regressions (#198)."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.orchestrators.tuflow import pomm_max_items


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
    with patch.object(pomm_max_items, "aggregated_from_paths") as aggregate, pytest.raises(
        ValueError, match="live_max_rows"
    ):
        pomm_max_items.export_mean_peak_report(
            script_directory=tmp_path,
            live_max_rows=0,
            use_live_dashboard=False,
        )
    aggregate.assert_not_called()
