"""Unit tests for ryan_library.processors.tuflow.other_processors.ChanProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.ChanProcessor import ChanProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestChanProcessor:
    """Tests for ChanProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock ChanProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = ChanProcessor(Path("test_1d_Chan.csv"))
            processor.file_name = "test_1d_Chan.csv"
            processor.resolved_file_path = processor.file_path.resolve()
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "Chan"
            return processor

    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read, mock_processor):
        """Test successful processing with all required columns."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = True
        
        # Setup df with required columns
        mock_processor.df = pd.DataFrame({
            "Channel": ["C1"],
            "LBUS Obvert": [10.0],
            "US Invert": [5.0]
        })

        mock_processor.process()
        
        assert mock_processor.processed is True
        
        # Check renaming
        assert "Chan ID" in mock_processor.df.columns
        assert "US Obvert" in mock_processor.df.columns
        assert "Channel" not in mock_processor.df.columns
        assert "LBUS Obvert" not in mock_processor.df.columns
        
        # Check calculation
        assert "Height" in mock_processor.df.columns
        assert mock_processor.df.iloc[0]["Height"] == 5.0  # 10.0 - 5.0

    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.read_maximums_csv")
    def test_process_missing_height_columns(self, mock_read, mock_processor):
        """Test failure when columns for Height calculation are missing."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        
        mock_processor.df = pd.DataFrame({
            "Channel": ["C1"],
            "LBUS Obvert": [10.0]
            # Missing US Invert
        })

        mock_processor.process()
        
        assert mock_processor.df.empty
        assert mock_processor.processed is False

    @patch("ryan_library.processors.tuflow.other_processors.ChanProcessor.ChanProcessor.read_maximums_csv")
    def test_process_missing_channel_column(self, mock_read, mock_processor):
        """Test failure when Channel column is missing."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        
        mock_processor.df = pd.DataFrame({
            "LBUS Obvert": [10.0],
            "US Invert": [5.0]
            # Missing Channel
        })

        mock_processor.process()
        
        assert mock_processor.df.empty
        assert mock_processor.processed is False
