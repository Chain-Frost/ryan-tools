
"""Tests for ryan_library.scripts.tuflow.tuflow_culverts_mean."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.tuflow import tuflow_culverts_mean

@pytest.fixture
def mock_aggregated_df():
    return pd.DataFrame({
        "aep_text": ["1%", "1%", "1%"],
        "duration_text": ["1h", "1h", "2h"],
        "tp_text": ["tp1", "tp1", "tp1"],
        "trim_runcode": ["run1", "run1", "run1"], # Same runcode to allow grouping/comparison
        "Chan ID": ["C1", "C1", "C1"],
        "Q": [10.0, 12.0, 15.0],
        "V": [1.0, 1.2, 1.5],
        "DS_h": [5.0, 5.1, 5.2],
        "US_h": [6.0, 6.1, 6.2]
    })

def test_find_culvert_aep_dur_mean_success(mock_aggregated_df):
    result = tuflow_culverts_mean.find_culvert_aep_dur_mean(mock_aggregated_df)
    
    assert not result.empty
    assert "mean_Q" in result.columns
    assert "count" in result.columns
    
    # Check 1h group (2 items)
    row_1h = result[result["duration_text"] == "1h"].iloc[0]
    assert row_1h["mean_Q"] == 11.0 # (10+12)/2
    assert row_1h["count"] == 2
    
    # Check 2h group (1 item)
    row_2h = result[result["duration_text"] == "2h"].iloc[0]
    assert row_2h["mean_Q"] == 15.0
    assert row_2h["count"] == 1

def test_find_culvert_aep_dur_mean_empty():
    result = tuflow_culverts_mean.find_culvert_aep_dur_mean(pd.DataFrame())
    assert result.empty

def test_find_culvert_aep_mean_max_success(mock_aggregated_df):
    # First get the means
    dur_mean = tuflow_culverts_mean.find_culvert_aep_dur_mean(mock_aggregated_df)
    
    # Then find the max mean
    result = tuflow_culverts_mean.find_culvert_aep_mean_max(dur_mean)
    
    assert not result.empty
    assert len(result) == 1 # One max per AEP/Chan ID group
    assert result.iloc[0]["duration_text"] == "2h" # 15.0 > 11.0

def test_run_culvert_mean_report_success():
    with patch("ryan_library.scripts.tuflow.tuflow_culverts_mean.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv") as mock_bulk:
            with patch("ryan_library.scripts.tuflow.tuflow_culverts_mean.ExcelExporter") as mock_exporter:
                
                # Mock collection
                mock_collection = MagicMock()
                mock_collection.processors = [MagicMock()]
                mock_collection.combine_1d_maximums.return_value = pd.DataFrame({
                    "aep_text": ["1%"], "duration_text": ["1h"], "tp_text": ["tp1"],
                    "trim_runcode": ["r1"], "Chan ID": ["C1"], "Q": [10.0]
                })
                mock_bulk.return_value = mock_collection
                
                tuflow_culverts_mean.run_culvert_mean_report()
                
                mock_bulk.assert_called_once()
                mock_collection.combine_1d_maximums.assert_called_once()
                mock_exporter.return_value.export_dataframes.assert_called_once()

def test_run_culvert_mean_report_no_processors():
    with patch("ryan_library.scripts.tuflow.tuflow_culverts_mean.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv") as mock_bulk:
            mock_collection = MagicMock()
            mock_collection.processors = [] # Empty
            mock_bulk.return_value = mock_collection
            
            tuflow_culverts_mean.run_culvert_mean_report()
            
            mock_bulk.assert_called_once()
            # combine_1d_maximums should NOT be called
            mock_collection.combine_1d_maximums.assert_not_called()
