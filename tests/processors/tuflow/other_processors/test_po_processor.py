"""Unit tests for ryan_library.processors.tuflow.other_processors.PoProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor


class TestPOProcessor:
    """Tests for POProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock POProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = POProcessor(Path("test_PO.csv"))
            processor.file_name = "test_PO.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "PO"
            return processor

    def test_parse_point_output_success(self, mock_processor):
        """Test successful parsing of PO data."""
        # Construct a raw dataframe mimicking PO format
        # PO files have a dummy first column that gets dropped
        # Row 0: Measurement (Type)
        # Row 1: Location
        # Row 2+: Data
        
        data = [
            ["Dummy", "Time", "H", "V"],          # Measurement
            ["Dummy", "Time", "Loc1", "Loc1"],    # Location
            ["Dummy", "0.0", "10.0", "1.0"],      # Data 1
            ["Dummy", "1.0", "10.1", "1.1"]       # Data 2
        ]
        raw_df = pd.DataFrame(data)
        
        tidy_df = mock_processor._parse_point_output(raw_df)
        
        assert not tidy_df.empty
        assert list(tidy_df.columns) == ["Time", "Location", "Type", "Value"]
        
        # Check Loc1 H
        h_rows = tidy_df[(tidy_df["Location"] == "Loc1") & (tidy_df["Type"] == "H")]
        assert len(h_rows) == 2
        assert h_rows.iloc[0]["Value"] == 10.0
        
        # Check Loc1 V
        v_rows = tidy_df[(tidy_df["Location"] == "Loc1") & (tidy_df["Type"] == "V")]
        assert len(v_rows) == 2
        assert v_rows.iloc[0]["Value"] == 1.0

    def test_parse_point_output_missing_time(self, mock_processor):
        """Test parsing fails if Time column is missing."""
        data = [
            ["Dummy", "H", "V"],
            ["Dummy", "Loc1", "Loc1"],
            ["Dummy", "10.0", "1.0"]
        ]
        raw_df = pd.DataFrame(data)
        
        tidy_df = mock_processor._parse_point_output(raw_df)
        assert tidy_df.empty

    def test_parse_point_output_empty(self, mock_processor):
        """Test parsing empty raw dataframe."""
        raw_df = pd.DataFrame()
        tidy_df = mock_processor._parse_point_output(raw_df)
        assert tidy_df.empty

    @patch("pandas.read_csv")
    @patch("ryan_library.processors.tuflow.other_processors.POProcessor.POProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.other_processors.POProcessor.POProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.other_processors.POProcessor.POProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read, mock_processor):
        """Test full process flow success."""
        mock_validate.return_value = True
        
        # Mock read_csv return
        data = [
            ["Dummy", "Time", "H"],
            ["Dummy", "Time", "Loc1"],
            ["Dummy", "0.0", "10.0"]
        ]
        mock_read.return_value = pd.DataFrame(data)
        
        mock_processor.process()
        
        assert mock_processor.processed is True
        mock_add.assert_called_once()
        mock_apply.assert_called_once()
        assert not mock_processor.df.empty
