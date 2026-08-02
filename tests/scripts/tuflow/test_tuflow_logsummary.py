"""Tests for ryan_library.orchestrators.tuflow.tuflow_logsummary."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from ryan_library.orchestrators.tuflow import tuflow_logsummary
from ryan_library.orchestrators.tuflow.tuflow_logsummary import LogFileProcessingResult


@pytest.fixture
def mock_log_file(tmp_path):
    f = tmp_path / "run.tlf"
    f.write_text("Log content")
    return f


def test_process_log_file_success(mock_log_file):
    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.read_log_file") as mock_read:
        with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.search_for_completion") as mock_search:
            with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.process_top_lines") as mock_process:
                with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.finalise_data") as mock_finalise:

                    mock_read.return_value = ["Line 1", "Line 2"]
                    # search_for_completion returns (data_dict, sim_complete, current_section)
                    # We need sim_complete=2 to proceed
                    mock_search.return_value = ({}, 2, None)

                    # process_top_lines returns (data_dict, success, spec_events, spec_scen, spec_var)
                    # We need success=4 to proceed
                    mock_process.return_value = ({}, 4, False, False, False)

                    mock_finalise.return_value = pd.DataFrame({"Runcode": ["run"]})

                    df = tuflow_logsummary.process_log_file(mock_log_file)

                    assert not df.empty
                    assert df.iloc[0]["Runcode"] == "run"


def test_process_log_file_incomplete(mock_log_file):
    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.read_log_file") as mock_read:
        with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.search_for_completion") as mock_search:

            mock_read.return_value = ["Line 1"]
            # sim_complete != 2
            mock_search.return_value = ({}, 0, None)

            df = tuflow_logsummary.process_log_file(mock_log_file)

            assert df.empty


def test_main_processing_success():
    log_file = Path("run.tlf")
    data_frame = pd.DataFrame({"Runcode": ["run"], "StartDate": [1]})
    processing_result = LogFileProcessingResult(
        logfile=log_file,
        data_frame=data_frame,
        status="OK",
        detail="1 row",
    )
    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.setup_logger"):
        with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.discover_log_files") as mock_discover:
            with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.process_log_files") as mock_process:
                with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.ExcelExporter") as mock_exporter:
                    mock_save = mock_exporter.return_value.save_to_excel

                    mock_discover.return_value = [log_file]
                    mock_process.return_value = [processing_result]

                    tuflow_logsummary.main_processing(use_live_dashboard=False)

                    mock_discover.assert_called_once()
                    mock_process.assert_called_once()
                    mock_save.assert_called_once()


def test_main_processing_no_files():
    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.setup_logger"):
        with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.discover_log_files") as mock_discover:

            mock_discover.return_value = []

            tuflow_logsummary.main_processing()

            mock_discover.assert_called_once()
