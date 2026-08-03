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
            processor.resolved_file_path = processor.file_path.resolve()
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "EOF"
            return processor

    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.other_processors.EOFProcessor.EOFProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_processor):
        """Test successful processing of a valid EOF file content."""
        mock_validate.return_value = True

        # Sample EOF content to satisfy the file reading part
        eof_content = """
Some Header Info
...
CULVERT AND PIPE DATA
Channel  Type  Num_barrels  US_Invert  DS_Invert  US_Obvert  DS_Obvert  Length  Slope  Mannings_n  Diam_Width  Height  Inlet_Height  Inlet_Width  Entry Loss  Exit Loss  Fixed Loss  Ent/Exit Losses
C1  R  1  10.0  9.0  12.0  11.0  100.0  0.01  0.013  2.0  2.0  2.0  2.0  0.5  1.0  0.0  1.5
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
