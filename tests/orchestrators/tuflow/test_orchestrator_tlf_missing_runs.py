"""Validate legacy table reporting and exclusive output creation."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.orchestrators.tuflow import tlf_missing_runs as workflow
from ryan_library.orchestrators.tuflow.tlf_missing_runs import orchestrate_missing_runs_check
from ryan_library.orchestrators.tuflow.tuflow_logsummary import LogFileProcessingResult


def test_csv_summary_and_collision(tmp_path: Path) -> None:
    source = tmp_path / "tracking.csv"
    pd.DataFrame(
        {
            "trim_run_code": ["M"] * 10,
            "Duration": [60] * 10,
            "AEP": [1] * 10,
            "TP": [f"TP{n:02d}" for n in range(1, 11)],
        }
    ).to_csv(source, index=False)
    original_input = source.read_bytes()
    output = orchestrate_missing_runs_check(source)
    assert output == tmp_path / "tracking__missing_runs_summary.csv"
    original_output = output.read_bytes()
    assert b"trim_run_code" in original_output
    with pytest.raises(FileExistsError):
        orchestrate_missing_runs_check(source)
    assert output.read_bytes() == original_output
    assert source.read_bytes() == original_input


def test_excel_selection(tmp_path: Path) -> None:
    source = tmp_path / "tracking.xlsx"
    with (
        patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.read_excel", return_value=pd.DataFrame()) as read,
        patch(
            "ryan_library.orchestrators.tuflow.tlf_missing_runs.summarize_for_cli",
            return_value=("Summary", pd.DataFrame({"message": ["Summary"]})),
        ),
    ):
        output = orchestrate_missing_runs_check(source, sheet_name="Runs")
    read.assert_called_once_with(io=source, sheet_name="Runs")
    assert "Summary" in output.read_text()


def test_verification_dashboard_tracks_actual_logs_and_skips(tmp_path: Path) -> None:
    completed = tmp_path / "M_01p_TP01.tlf"
    completed.write_text("Simulation FINISHED\n")
    (tmp_path / "M_01p_TP01.hpc.tlf").write_text("Simulation FINISHED\n")
    (tmp_path / "M_01p_TP02.tlf").write_text("Running\n")
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    with (
        patch.object(workflow, "LiveWorkflowDashboard", return_value=dashboard),
        patch.object(dashboard, "set_tasks", wraps=dashboard.set_tasks) as tasks,
        patch.object(dashboard, "mark_running", wraps=dashboard.mark_running) as running,
        patch.object(dashboard, "mark_finished", wraps=dashboard.mark_finished) as finished,
        patch.object(workflow, "process_log_file_for_dashboard") as parse,
        patch.object(workflow.ExcelExporter, "save_to_excel") as export,
    ):
        result = workflow.orchestrate_ensemble_scan(tmp_path, verify_success=True)
    assert result.verified_count == 1
    assert len(tasks.call_args.kwargs["files"]) == 2
    assert running.call_count == 2
    assert {call.kwargs["status"] for call in finished.call_args_list} == {"OK", "SKIP"}
    parse.assert_not_called()
    export.assert_not_called()


def test_optional_export_reuses_standard_parser_and_workbook(tmp_path: Path) -> None:
    source = tmp_path / "M_01p_TP01.tlf"
    source.write_text("Simulation FINISHED\n")
    (tmp_path / "M_01p_TP01.hpc.tlf").write_text("Simulation FINISHED\n")
    frame = pd.DataFrame({"Runcode": ["M"], "StartDate": ["2026-11-07"], "EndStatus": ["Simulation FINISHED"]})
    parsed = LogFileProcessingResult(source, frame, "OK", "Complete")
    with (
        patch.object(workflow, "process_log_file_for_dashboard", return_value=parsed) as parse,
        patch.object(workflow.ExcelExporter, "save_to_excel") as export,
    ):
        workflow.orchestrate_ensemble_scan(
            tmp_path,
            verify_success=True,
            export_log_summary=True,
            use_live_dashboard=False,
        )
    parse.assert_called_once_with(source)
    kwargs = export.call_args.kwargs
    assert kwargs["file_name_prefix"] == "ModellingLog"
    assert kwargs["sheet_name"] == "Log Summary"
    assert kwargs["include_data_dictionary"] is True
    assert kwargs["output_directory"] == tmp_path
    assert kwargs["data_frame"].iloc[0]["Runcode"] == "M"


def test_export_requires_verification(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires"):
        workflow.orchestrate_ensemble_scan(tmp_path, export_log_summary=True)


def test_export_failure_finishes_dashboard_as_failed(tmp_path: Path) -> None:
    source = tmp_path / "M_01p_TP01.tlf"
    source.write_text("Simulation FINISHED\n")
    frame = pd.DataFrame({"Runcode": ["M"], "StartDate": ["2026-11-07"]})
    dashboard = LiveWorkflowDashboard(title="Test", enabled=False)
    with (
        patch.object(workflow, "LiveWorkflowDashboard", return_value=dashboard),
        patch.object(dashboard, "finish", wraps=dashboard.finish) as finish,
        patch.object(
            workflow, "process_log_file_for_dashboard", return_value=LogFileProcessingResult(source, frame, "OK", "")
        ),
        patch.object(workflow.ExcelExporter, "save_to_excel", side_effect=OSError("export blocked")),
    ):
        with pytest.raises(OSError, match="export blocked"):
            workflow.orchestrate_ensemble_scan(tmp_path, verify_success=True, export_log_summary=True)
    finish.assert_called_once_with(outcome="FAILED", detail="Log scan or summary export failed")
