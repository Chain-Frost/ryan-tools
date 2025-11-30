
"""Tests for ryan_library.scripts.RORB.closure_durations."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.RORB import closure_durations

@pytest.fixture
def mock_batch_data():
    return pd.DataFrame({
        "AEP": ["1%"],
        "Duration": ["24h"],
        "TPat": [1],
        "csv": ["path/to/hydro.csv"],
        "Path": ["path/to/batch.out"]
    })

@pytest.fixture
def mock_hydro_results():
    return pd.DataFrame({
        "AEP": ["1%"],
        "Duration": ["24h"],
        "TP": [1],
        "Location": ["Loc1"],
        "ThresholdFlow": [10.0],
        "Duration_Exceeding": [5.0],
        "out_path": ["path/to/batch.out"]
    })

def test_collect_batch_data_success(mock_batch_data):
    with patch("ryan_library.scripts.RORB.closure_durations.find_batch_files") as mock_find:
        with patch("ryan_library.scripts.RORB.closure_durations.parse_batch_output") as mock_parse:
            mock_find.return_value = [Path("batch.out")]
            mock_parse.return_value = mock_batch_data
            
            df = closure_durations._collect_batch_data([Path(".")])
            
            assert not df.empty
            assert len(df) == 1
            mock_find.assert_called_once()
            mock_parse.assert_called_once()

def test_collect_batch_data_empty():
    with patch("ryan_library.scripts.RORB.closure_durations.find_batch_files") as mock_find:
        mock_find.return_value = []
        
        df = closure_durations._collect_batch_data([Path(".")])
        
        assert df.empty

def test_process_hydrographs_success(mock_batch_data, mock_hydro_results):
    with patch("ryan_library.scripts.RORB.closure_durations.analyze_hydrograph") as mock_analyze:
        mock_analyze.return_value = mock_hydro_results
        
        df = closure_durations._process_hydrographs(mock_batch_data, [10.0])
        
        assert not df.empty
        assert len(df) == 1
        mock_analyze.assert_called_once()

def test_summarise_results(mock_hydro_results):
    # Mock median_stats_func
    with patch("ryan_library.scripts.RORB.closure_durations.median_stats_func") as mock_stats:
        mock_stats.return_value = ({
            "median": 5.0,
            "median_duration": "24h",
            "median_TP": 1,
            "low": 4.0,
            "high": 6.0,
            "mean_including_zeroes": 5.0
        }, None)
        
        summary = closure_durations._summarise_results(mock_hydro_results)
        
        assert not summary.empty
        assert "Central_Value" in summary.columns
        assert summary.iloc[0]["Central_Value"] == 5.0

def test_run_closure_durations_success(mock_batch_data, mock_hydro_results):
    with patch("ryan_library.scripts.RORB.closure_durations.setup_logger"):
        with patch("ryan_library.scripts.RORB.closure_durations._collect_batch_data") as mock_collect:
            with patch("ryan_library.scripts.RORB.closure_durations._process_hydrographs") as mock_process:
                with patch("ryan_library.scripts.RORB.closure_durations._summarise_results") as mock_summarise:
                    with patch("pandas.DataFrame.to_parquet"):
                        with patch("pandas.DataFrame.to_csv"):
                            mock_collect.return_value = mock_batch_data
                            mock_process.return_value = mock_hydro_results
                            mock_summarise.return_value = pd.DataFrame({
                                "Path": ["p"], "Location": ["l"], "ThresholdFlow": [10], "AEP": ["1%"]
                            })
                            
                            closure_durations.run_closure_durations()
                            
                            mock_collect.assert_called_once()
                            mock_process.assert_called_once()
                            mock_summarise.assert_called_once()

def test_run_closure_durations_no_data():
    with patch("ryan_library.scripts.RORB.closure_durations.setup_logger"):
        with patch("ryan_library.scripts.RORB.closure_durations._collect_batch_data") as mock_collect:
            mock_collect.return_value = pd.DataFrame()
            
            closure_durations.run_closure_durations()
            
            mock_collect.assert_called_once()
