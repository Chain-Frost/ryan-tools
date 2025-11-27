"""Unit tests for ryan_library.processors.tuflow.other_processors.EofProcessor."""

import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.EOFProcessor import EOFProcessor


class TestEOFProcessor:
    """Tests for EOFProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock EOFProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = EOFProcessor(Path("test.eof"))
            processor.file_name = "test.eof"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "EOF"
            return processor

    @patch("pandas.read_fwf")
    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read_fwf, mock_processor):
        """Test successful processing of a valid EOF file content."""
        mock_validate.return_value = True
        
        # Mock read_fwf return
        data = {
            "Chan ID": ["C1"],
            "Type": ["R"],
            "Num_barrels": [1],
            "US_Invert": [10.0],
            "DS_Invert": [9.0],
            "US_Obvert": [12.0],
            "DS_Obvert": [11.0],
            "Length": [100.0],
            "Slope": [0.01],
            "Mannings_n": [0.013],
            "Diam_Width": [2.0],
            "Height": [2.0],
            "Inlet_Height": [2.0],
            "Inlet_Width": [2.0],
            "Entry Loss": [0.5],
            "Exit Loss": [1.0],
            "Fixed Loss": [0.0],
            "Ent/Exit Losses": [1.5]
        }
        mock_read_fwf.return_value = pd.DataFrame(data)

        # Sample EOF content to satisfy the file reading part
        eof_content = """
Some Header Info
...
CULVERT AND PIPE DATA
Channel  Type  Num_barrels
C1       R     1
"""
        with patch("builtins.open", mock_open(read_data=eof_content)):
            mock_processor.process()

        assert mock_processor.processed is True
        assert not mock_processor.df.empty
        assert len(mock_processor.df) == 1
        
        # Check renaming
        assert "US Invert" in mock_processor.df.columns
        assert "pSlope" in mock_processor.df.columns
        assert "n or Cd" in mock_processor.df.columns
        
        # Check values
        row = mock_processor.df.iloc[0]
        assert row["Chan ID"] == "C1"
        assert row["US Invert"] == 10.0
        assert row["Height"] == 2.0

    def test_process_section_not_found(self, mock_processor):
        """Test failure when section header is missing."""
        eof_content = """
Some Header Info
No Data Here
"""
        with patch("builtins.open", mock_open(read_data=eof_content)):
            mock_processor.process()

        assert mock_processor.df.empty
        assert mock_processor.processed is False

    def test_process_no_data_lines(self, mock_processor):
        """Test failure when header exists but no data follows."""
        eof_content = """
CULVERT AND PIPE DATA
Channel  Type  Num_barrels ...

"""
        # The logic looks for blank line after header. If header is followed immediately by blank line/end, data_lines might be empty.
        with patch("builtins.open", mock_open(read_data=eof_content)):
            mock_processor.process()

        assert mock_processor.df.empty
        assert mock_processor.processed is False
