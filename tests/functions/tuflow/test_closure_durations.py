"""Unit tests for ryan_library.functions.tuflow.closure_durations."""

import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
from pathlib import Path
from ryan_library.functions.tuflow.closure_durations import (
    find_po_files,
    parse_metadata,
    read_po_csv,
    analyze_po_file,
    summarise_results,
)


class TestClosureDurations:
    """Tests for closure_durations module."""

    @patch("ryan_library.functions.tuflow.closure_durations.find_files_parallel")
    @patch("ryan_library.functions.tuflow.closure_durations.is_non_zero_file")
    def test_find_po_files(self, mock_is_non_zero, mock_find):
        """Test finding PO files."""
        mock_find.return_value = [Path("test_PO.csv")]
        mock_is_non_zero.return_value = True
        
        paths = [Path("root")]
        with patch.object(Path, "is_dir", return_value=True):
            result = find_po_files(paths)
        
        assert len(result) == 1
        assert result[0] == Path("test_PO.csv")

    @patch("ryan_library.functions.tuflow.closure_durations.TuflowStringParser")
    def test_parse_metadata(self, mock_parser_cls):
        """Test metadata parsing."""
        mock_parser = MagicMock()
        mock_parser.aep.raw_value = "1%"
        mock_parser.duration.raw_value = "1hr"
        mock_parser.tp.raw_value = "tp1"
        mock_parser_cls.return_value = mock_parser
        
        result = parse_metadata(Path("test_001_1hr_tp1.csv"))
        
        assert result["AEP"] == "1%"
        assert result["Duration"] == "1hr"
        assert result["TP"] == "tp1"

    def test_read_po_csv_success(self):
        """Test successful PO CSV reading."""
        csv_content = "Location,Flow\nTime,Loc1\n0.0,10.0\n1.0,11.0"
        
        with patch.object(Path, "open", mock_open(read_data=csv_content)):
            with patch("pandas.read_csv") as mock_read_csv:
                mock_read_csv.return_value = pd.DataFrame({
                    "Time": [0.0, 1.0],
                    "Loc1": [10.0, 11.0]
                })
                df = read_po_csv(Path("test.csv"))
                
        assert not df.empty
        assert "Time" in df.columns
        assert "Loc1" in df.columns

    @patch("ryan_library.functions.tuflow.closure_durations.read_po_csv")
    @patch("ryan_library.functions.tuflow.closure_durations.parse_metadata")
    def test_analyze_po_file(self, mock_meta, mock_read):
        """Test PO file analysis."""
        mock_read.return_value = pd.DataFrame({
            "Time": [0.0, 60.0],  # 1 minute timestep
            "Loc1": [0.5, 1.5]
        })
        mock_meta.return_value = {"AEP": "1%", "Duration": "1hr", "TP": "tp1"}
        
        result = analyze_po_file(
            Path("test.csv"),
            thresholds=[1.0],
            data_type="Flow"
        )
        
        assert not result.empty
        assert len(result) == 1
        row = result.iloc[0]
        assert row["Location"] == "Loc1"
        assert row["Duration_Exceeding"] == 60.0  # 1 count * 60.0 timestep

    def test_summarise_results(self):
        """Test results summarisation."""
        df = pd.DataFrame({
            "out_path": ["p1", "p1"],
            "Location": ["L1", "L1"],
            "ThresholdFlow": [1.0, 1.0],
            "AEP": ["1%", "1%"],
            "Duration": ["1hr", "2hr"],
            "TP": ["tp1", "tp1"],
            "Duration_Exceeding": [10.0, 20.0]
        })
        
        # Mock median_stats to return dummy dict
        with patch("ryan_library.functions.pandas.median_calc.median_stats") as mock_stats:
            mock_stats.return_value = ({
                "median": 15.0,
                "median_duration": "1.5hr",
                "median_TP": "tp1",
                "low": 10.0,
                "high": 20.0,
                "mean_including_zeroes": 15.0
            }, None)
            
            result = summarise_results(df)
            
        assert not result.empty
        assert "Central_Value" in result.columns
        assert result.iloc[0]["Central_Value"] == 15.0
