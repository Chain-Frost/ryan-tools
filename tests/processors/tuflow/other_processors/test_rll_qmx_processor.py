"""Unit tests for ryan_library.processors.tuflow.other_processors.RLLQmxProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.RLLQmxProcessor import RLLQmxProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestRLLQmxProcessor:
    """Tests for RLLQmxProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock RLLQmxProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = RLLQmxProcessor(Path("test_RLL_Qmx.csv"))
            processor.file_name = "test_RLL_Qmx.csv"
            processor.resolved_file_path = processor.file_path.resolve()
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "RLLQmx"
            return processor

    def test_reshape_rll_qmx_data_success(self, mock_processor):
        """Test successful reshaping of RLL Qmx data."""
        mock_processor.df = pd.DataFrame({
            "ID": ["L1"],
            "Qmax": [10.0],
            "Time Qmax": [1.0],
            "dQmax": [0.0],
            "Time dQmax": [1.0],
            "H": [5.0]
        })

        mock_processor._reshape_rll_qmx_data()

        assert "Chan ID" in mock_processor.df.columns
        assert "Q" in mock_processor.df.columns
        assert "Time" in mock_processor.df.columns
        assert "dQ" in mock_processor.df.columns
        assert "Time dQ" in mock_processor.df.columns
        
        row = mock_processor.df.iloc[0]
        assert row["Chan ID"] == "L1"
        assert row["Q"] == 10.0

    def test_reshape_rll_qmx_data_missing_columns(self, mock_processor):
        """Test reshaping fails with missing columns."""
        mock_processor.df = pd.DataFrame({
            "ID": ["L1"]
            # Missing others
        })

        mock_processor._reshape_rll_qmx_data()
        assert mock_processor.df.empty

    @patch("ryan_library.processors.tuflow.other_processors.RLLQmxProcessor.RLLQmxProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.other_processors.RLLQmxProcessor.RLLQmxProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.other_processors.RLLQmxProcessor.RLLQmxProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.other_processors.RLLQmxProcessor.RLLQmxProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read, mock_processor):
        """Test full process flow success."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = True
        
        mock_processor.df = pd.DataFrame({
            "ID": ["L1"],
            "Qmax": [10.0],
            "Time Qmax": [1.0],
            "dQmax": [0.0],
            "Time dQmax": [1.0],
            "H": [5.0]
        })

        mock_processor.process()
        
        assert mock_processor.processed is True
        mock_read.assert_called_once()
        mock_add.assert_called_once()
