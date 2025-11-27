"""Unit tests for ryan_library.processors.tuflow.maximums_1d.NmxProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestNmxProcessor:
    """Tests for NmxProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock NmxProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = NmxProcessor(Path("test_1d_Nmx.csv"))
            processor.file_name = "test_1d_Nmx.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "Nmx"
            return processor

    def test_extract_and_transform_nmx_data_success(self, mock_processor):
        """Test successful extraction and transformation of NMX data."""
        mock_processor.df = pd.DataFrame({
            "Node ID": ["C1.1", "C1.2", "C2.1", "C2.2"],
            "Time Hmax": [1.0, 1.0, 2.0, 2.0],
            "Hmax": [10.0, 9.0, 20.0, 19.0]
        })

        mock_processor._extract_and_transform_nmx_data()

        assert len(mock_processor.df) == 2  # 2 channels
        assert "US_h" in mock_processor.df.columns
        assert "DS_h" in mock_processor.df.columns
        
        # Check C1
        c1 = mock_processor.df[mock_processor.df["Chan ID"] == "C1"].iloc[0]
        assert c1["US_h"] == 10.0
        assert c1["DS_h"] == 9.0

    def test_extract_and_transform_nmx_data_invalid_suffix(self, mock_processor):
        """Test filtering of invalid suffixes."""
        mock_processor.df = pd.DataFrame({
            "Node ID": ["C1.1", "C1.2", "C1.3"], # .3 is invalid
            "Time Hmax": [1.0, 1.0, 1.0],
            "Hmax": [10.0, 9.0, 10.0]
        })

        mock_processor._extract_and_transform_nmx_data()
        
        # Should have C1 with US_h and DS_h. C1.3 is filtered out.
        
        assert len(mock_processor.df) == 1
        c1 = mock_processor.df.iloc[0]
        assert c1["US_h"] == 10.0
        assert c1["DS_h"] == 9.0

    def test_extract_and_transform_nmx_data_missing_columns(self, mock_processor):
        """Test failure when columns are missing."""
        mock_processor.df = pd.DataFrame({
            "Node ID": ["C1.1"],
            # Missing Time Hmax, Hmax
        })

        mock_processor._extract_and_transform_nmx_data()
        assert mock_processor.df.empty

    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read, mock_processor):
        """Test full process flow success."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = True
        
        mock_processor.df = pd.DataFrame({
            "Node ID": ["C1.1", "C1.2"],
            "Time Hmax": [1.0, 1.0],
            "Hmax": [10.0, 9.0]
        })

        mock_processor.process()
        
        assert mock_processor.processed is True
        mock_read.assert_called_once()
        mock_add.assert_called_once()
