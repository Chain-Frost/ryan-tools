"""Tests for ryan_library.scripts.tuflow.closure_durations."""

import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
import pandas as pd
from ryan_library.scripts.tuflow import closure_durations

class TestProcessOnePo:
    @patch("ryan_library.scripts.tuflow.closure_durations.analyze_po_file")
    def test_process_one_po_success(self, mock_analyze):
        """Test successful processing of a single PO file."""
        mock_df = pd.DataFrame({"A": [1]})
        mock_analyze.return_value = mock_df
        
        args = (Path("test.csv"), [1.0], "Flow", {"Loc1"})
        res = closure_durations._process_one_po(args)
        
        assert res.equals(mock_df)
        mock_analyze.assert_called_once_with(
            csv_path=Path("test.csv"),
            thresholds=[1.0],
            data_type="Flow",
            allowed_locations={"Loc1"}
        )

    @patch("ryan_library.scripts.tuflow.closure_durations.analyze_po_file")
    def test_process_one_po_empty(self, mock_analyze):
        """Test processing returning empty dataframe."""
        mock_analyze.return_value = pd.DataFrame()
        
        args = (Path("test.csv"), [1.0], "Flow", None)
        res = closure_durations._process_one_po(args)
        
        assert res.empty

    @patch("ryan_library.scripts.tuflow.closure_durations.analyze_po_file")
    @patch("ryan_library.scripts.tuflow.closure_durations.logger")
    def test_process_one_po_exception(self, mock_logger, mock_analyze):
        """Test exception handling in worker."""
        mock_analyze.side_effect = Exception("Worker Error")
        
        args = (Path("test.csv"), [1.0], "Flow", None)
        res = closure_durations._process_one_po(args)
        
        assert res.empty
        assert mock_logger.exception.called

class TestProcessFiles:
    @patch("ryan_library.scripts.tuflow.closure_durations.analyze_po_file")
    def test_process_files_sequential(self, mock_analyze):
        """Test sequential processing."""
        mock_analyze.return_value = pd.DataFrame({"A": [1]})
        
        files = [Path("f1.csv"), Path("f2.csv")]
        res = closure_durations._process_files(
            files=files,
            thresholds=[1.0],
            data_type="Flow",
            allowed_locations=None,
            parallel=False
        )
        
        assert len(res) == 2
        assert mock_analyze.call_count == 2

    @patch("ryan_library.scripts.tuflow.closure_durations.analyze_po_file")
    def test_process_files_sequential_empty(self, mock_analyze):
        """Test sequential processing with empty results."""
        mock_analyze.return_value = pd.DataFrame()
        
        files = [Path("f1.csv")]
        res = closure_durations._process_files(
            files=files,
            thresholds=[1.0],
            data_type="Flow",
            allowed_locations=None,
            parallel=False
        )
        
        assert res.empty

    @patch("ryan_library.scripts.tuflow.closure_durations.ProcessPoolExecutor")
    def test_process_files_parallel(self, mock_executor):
        """Test parallel processing."""
        mock_pool = mock_executor.return_value.__enter__.return_value
        # Mock map return value
        mock_pool.map.return_value = [pd.DataFrame({"A": [1]}), pd.DataFrame({"A": [2]})]
        
        files = [Path("f1.csv"), Path("f2.csv")]
        res = closure_durations._process_files(
            files=files,
            thresholds=[1.0],
            data_type="Flow",
            allowed_locations=None,
            parallel=True,
            max_workers=2
        )
        
        assert len(res) == 2
        assert res["A"].tolist() == [1, 2]

    @patch("ryan_library.scripts.tuflow.closure_durations.ProcessPoolExecutor")
    def test_process_files_parallel_empty(self, mock_executor):
        """Test parallel processing with no results."""
        mock_pool = mock_executor.return_value.__enter__.return_value
        mock_pool.map.return_value = []
        
        files = [Path("f1.csv")]
        res = closure_durations._process_files(
            files=files,
            thresholds=[1.0],
            data_type="Flow",
            allowed_locations=None,
            parallel=True
        )
        
        assert res.empty

class TestRunClosureDurations:
    @patch("ryan_library.scripts.tuflow.closure_durations.find_po_files")
    @patch("ryan_library.scripts.tuflow.closure_durations._process_files")
    @patch("ryan_library.scripts.tuflow.closure_durations.summarise_results")
    @patch("ryan_library.scripts.tuflow.closure_durations.logger")
    def test_run_closure_durations_success(self, mock_logger, mock_summary, mock_process, mock_find, tmp_path):
        """Test successful run."""
        mock_find.return_value = [Path("test.csv")]
        mock_df = MagicMock(spec=pd.DataFrame)
        mock_df.empty = False
        mock_process.return_value = mock_df
        
        mock_sum_df = MagicMock(spec=pd.DataFrame)
        mock_sum_df.__getitem__.return_value.str.extract.return_value.__getitem__.return_value.astype.return_value = pd.Series([1.0])
        mock_summary.return_value = mock_sum_df
        
        with patch("ryan_library.scripts.tuflow.closure_durations.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20250101-1200"
            
            # We need to mock to_parquet and to_csv on the result df
            # and to_csv on summary df
            
            closure_durations.run_closure_durations(paths=[tmp_path])
            
            mock_df.to_parquet.assert_called()
            mock_df.to_csv.assert_called()
            mock_sum_df.to_csv.assert_called()
            assert mock_logger.info.called
            assert "Processing complete" in mock_logger.info.call_args[0][0]

    @patch("ryan_library.scripts.tuflow.closure_durations.find_po_files")
    @patch("ryan_library.scripts.tuflow.closure_durations.logger")
    def test_run_closure_durations_no_files(self, mock_logger, mock_find):
        """Test run with no files found."""
        mock_find.return_value = []
        
        closure_durations.run_closure_durations()
        
        assert mock_logger.warning.called
        assert "No PO CSV files found" in mock_logger.warning.call_args[0][0]

    @patch("ryan_library.scripts.tuflow.closure_durations.find_po_files")
    @patch("ryan_library.scripts.tuflow.closure_durations._process_files")
    @patch("ryan_library.scripts.tuflow.closure_durations.logger")
    def test_run_closure_durations_no_data(self, mock_logger, mock_process, mock_find):
        """Test run with files but no data processed."""
        mock_find.return_value = [Path("test.csv")]
        mock_process.return_value = pd.DataFrame()
        
        closure_durations.run_closure_durations()
        
        assert mock_logger.warning.called
        assert "No hydrograph data processed" in mock_logger.warning.call_args[0][0]
