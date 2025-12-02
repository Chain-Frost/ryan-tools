
"""Tests for ryan_library.scripts.tuflow.tuflow_logsummary."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.tuflow import tuflow_logsummary

@pytest.fixture
def mock_log_file(tmp_path):
    f = tmp_path / "run.tlf"
    f.write_text("Log content")
    return f

def test_process_log_file_success(mock_log_file):
    with patch("ryan_library.scripts.tuflow.tuflow_logsummary.read_log_file") as mock_read:
        with patch("ryan_library.scripts.tuflow.tuflow_logsummary.search_for_completion") as mock_search:
            with patch("ryan_library.scripts.tuflow.tuflow_logsummary.process_top_lines") as mock_process:
                with patch("ryan_library.scripts.tuflow.tuflow_logsummary.finalise_data") as mock_finalise:
                    
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
    with patch("ryan_library.scripts.tuflow.tuflow_logsummary.read_log_file") as mock_read:
        with patch("ryan_library.scripts.tuflow.tuflow_logsummary.search_for_completion") as mock_search:
            
            mock_read.return_value = ["Line 1"]
            # sim_complete != 2
            mock_search.return_value = ({}, 0, None)
            
            df = tuflow_logsummary.process_log_file(mock_log_file)
            
            assert df.empty

def test_main_processing_success():
    with patch("ryan_library.scripts.tuflow.tuflow_logsummary.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_logsummary.find_files_parallel") as mock_find:
            with patch("ryan_library.scripts.tuflow.tuflow_logsummary.Pool") as mock_pool:
                with patch("ryan_library.scripts.tuflow.tuflow_logsummary.save_to_excel") as mock_save:
                    
                    mock_find.return_value = [Path("run.tlf")]
                    
                    # Mock pool.map
                    pool_instance = mock_pool.return_value.__enter__.return_value
                    pool_instance.map.return_value = [pd.DataFrame({"Runcode": ["run"], "StartDate": [1]})]
                    
                    tuflow_logsummary.main_processing()
                    
                    mock_find.assert_called_once()
                    pool_instance.map.assert_called_once()
                    mock_save.assert_called_once()

def test_main_processing_no_files():
    with patch("ryan_library.scripts.tuflow.tuflow_logsummary.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_logsummary.find_files_parallel") as mock_find:
            
            mock_find.return_value = []
            
            tuflow_logsummary.main_processing()
            
            mock_find.assert_called_once()
