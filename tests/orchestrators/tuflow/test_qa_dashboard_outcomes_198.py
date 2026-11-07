"""Dashboard lifecycle and export-failure regressions for PO QA workflows (#198)."""

from pathlib import Path
from unittest.mock import patch

import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.orchestrators.tuflow import peak_check_po_csvs as peak
from ryan_library.orchestrators.tuflow import tuflow_timeseries_stability as stability


@pytest.mark.parametrize("enabled", [False, True])
def test_po_peak_dashboard_export_is_not_early_success(tmp_path: Path, enabled: bool) -> None:
    path = tmp_path / "model_PO.csv"
    with (
        patch.object(peak, "collect_po_csv_files", return_value=[path]),
        patch.object(peak.cf, "ProcessPoolExecutor") as pool,
        patch.object(peak, "ExcelExporter") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
    ):
        pool.return_value.__enter__.return_value.map.return_value = [
            [{"run_code": "run1", "status": "OK", "peak_time": 1.0}]
        ]
        assert peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=enabled)
        exporter.return_value.save_to_excel.assert_called_once()
        assert finish.call_args is not None
        assert finish.call_args.kwargs["outcome"] == "SUCCESS"


@pytest.mark.parametrize("enabled", [False, True])
def test_stability_dashboard_export_is_not_early_success(tmp_path: Path, enabled: bool) -> None:
    path = tmp_path / "model_1d_Q.csv"
    with (
        patch.object(stability, "collect_timeseries_files", return_value=[(path, "Q")]),
        patch.object(stability.cf, "ProcessPoolExecutor") as pool,
        patch.object(stability, "ExcelExporter") as exporter,
        patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
    ):
        pool.return_value.__enter__.return_value.map.return_value = [
            [{"run_code": "run1", "status": "OK", "points": 5}]
        ]
        assert stability.main_processing(paths_to_process=[tmp_path], result_types=["Q"], use_live_dashboard=enabled)
        exporter.return_value.save_to_excel.assert_called_once()
        assert finish.call_args is not None
        assert finish.call_args.kwargs["outcome"] == "SUCCESS"


@pytest.mark.parametrize("workflow", ["peak", "stability"])
def test_qa_failure_never_reports_success_or_writes_result(tmp_path: Path, workflow: str) -> None:
    path = tmp_path / "input.csv"
    if workflow == "peak":
        with (
            patch.object(peak, "collect_po_csv_files", return_value=[path]),
            patch.object(peak.cf, "ProcessPoolExecutor") as pool,
            patch.object(peak, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.side_effect = OSError("worker failure")
            with pytest.raises(OSError, match="worker failure"):
                peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.assert_not_called()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"
    else:
        with (
            patch.object(stability, "collect_timeseries_files", return_value=[(path, "PO")]),
            patch.object(stability.cf, "ProcessPoolExecutor") as pool,
            patch.object(stability, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.side_effect = OSError("worker failure")
            with pytest.raises(OSError, match="worker failure"):
                stability.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.assert_not_called()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"


@pytest.mark.parametrize("workflow", ["peak", "stability"])
def test_qa_export_failure_marks_dashboard_failed(tmp_path: Path, workflow: str) -> None:
    path = tmp_path / "input.csv"
    if workflow == "peak":
        with (
            patch.object(peak, "collect_po_csv_files", return_value=[path]),
            patch.object(peak.cf, "ProcessPoolExecutor") as pool,
            patch.object(peak, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = [[{"run_code": "run1"}]]
            exporter.return_value.save_to_excel.side_effect = OSError("locked output")
            with pytest.raises(OSError, match="locked output"):
                peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"
    else:
        with (
            patch.object(stability, "collect_timeseries_files", return_value=[(path, "PO")]),
            patch.object(stability.cf, "ProcessPoolExecutor") as pool,
            patch.object(stability, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = [[{"run_code": "run1"}]]
            exporter.return_value.save_to_excel.side_effect = OSError("locked output")
            with pytest.raises(OSError, match="locked output"):
                stability.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"


@pytest.mark.parametrize("workflow", ["peak", "stability"])
def test_qa_no_files_is_a_skip_not_success(tmp_path: Path, workflow: str) -> None:
    if workflow == "peak":
        with (
            patch.object(peak, "collect_po_csv_files", return_value=[]),
            patch.object(peak, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            assert peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.assert_not_called()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "SKIPPED"
    else:
        with (
            patch.object(stability, "collect_timeseries_files", return_value=[]),
            patch.object(stability, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            assert stability.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.assert_not_called()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "SKIPPED"


@pytest.mark.parametrize("workflow", ["peak", "stability"])
def test_qa_rejects_bad_live_settings_before_discovery(tmp_path: Path, workflow: str) -> None:
    if workflow == "peak":
        with patch.object(peak, "collect_po_csv_files") as find, pytest.raises(ValueError, match="live_max_rows"):
            peak.main_processing(paths_to_process=[tmp_path], live_max_rows=0)
        find.assert_not_called()
    else:
        with (
            patch.object(stability, "collect_timeseries_files") as find,
            pytest.raises(ValueError, match="live_max_rows"),
        ):
            stability.main_processing(paths_to_process=[tmp_path], live_max_rows=0)
        find.assert_not_called()


@pytest.mark.parametrize("workflow", ["peak", "stability"])
@pytest.mark.parametrize("statuses", [["OK", "TIME_PARSE_FAIL"], ["CSV_PARSE_FAIL"]])
def test_qa_malformed_input_never_claims_full_success(tmp_path: Path, workflow: str, statuses: list[str]) -> None:
    files = [tmp_path / f"run_{index}_PO.csv" for index in range(len(statuses))]
    diagnostic_rows = [
        [{"run_code": path.stem, "status": status}] for path, status in zip(files, statuses, strict=True)
    ]
    if workflow == "peak":
        with (
            patch.object(peak, "collect_po_csv_files", return_value=files),
            patch.object(peak.cf, "ProcessPoolExecutor") as pool,
            patch.object(peak, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = diagnostic_rows
            assert peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False) is False
            exporter.return_value.save_to_excel.assert_called_once()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"
    else:
        with (
            patch.object(stability, "collect_timeseries_files", return_value=[(path, "PO") for path in files]),
            patch.object(stability.cf, "ProcessPoolExecutor") as pool,
            patch.object(stability, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = diagnostic_rows
            assert stability.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False) is False
            exporter.return_value.save_to_excel.assert_called_once()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "FAILED"


@pytest.mark.parametrize("workflow", ["peak", "stability"])
def test_qa_no_data_diagnostic_export_is_a_skip(tmp_path: Path, workflow: str) -> None:
    path = tmp_path / "empty_PO.csv"
    if workflow == "peak":
        with (
            patch.object(peak, "collect_po_csv_files", return_value=[path]),
            patch.object(peak.cf, "ProcessPoolExecutor") as pool,
            patch.object(peak, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = [[{"status": "NO_DATA"}]]
            assert peak.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.return_value.save_to_excel.assert_called_once()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "SKIPPED"
    else:
        with (
            patch.object(stability, "collect_timeseries_files", return_value=[(path, "PO")]),
            patch.object(stability.cf, "ProcessPoolExecutor") as pool,
            patch.object(stability, "ExcelExporter") as exporter,
            patch.object(LiveWorkflowDashboard, "finish", autospec=True) as finish,
        ):
            pool.return_value.__enter__.return_value.map.return_value = [[{"status": "NO_DATA"}]]
            assert stability.main_processing(paths_to_process=[tmp_path], use_live_dashboard=False)
            exporter.return_value.save_to_excel.assert_called_once()
            assert finish.call_args is not None
            assert finish.call_args.kwargs["outcome"] == "SKIPPED"
