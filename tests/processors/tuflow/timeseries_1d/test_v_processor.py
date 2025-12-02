"""Unit tests for ryan_library.processors.tuflow.timeseries_1d.VProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.VProcessor import VProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestVProcessor:
    """Tests for VProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock VProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = VProcessor(Path("test_1d_V.csv"))
            processor.file_name = "test_1d_V.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "V"
            return processor

    @patch("ryan_library.processors.tuflow.timeseries_1d.VProcessor.VProcessor._process_timeseries_pipeline")
    def test_process(self, mock_pipeline, mock_processor):
        """Test process method delegates correctly."""
        mock_processor.process()
        mock_pipeline.assert_called_once_with(data_type="V")

    @patch("ryan_library.processors.tuflow.timeseries_1d.VProcessor.VProcessor._normalise_value_dataframe")
    def test_process_timeseries_raw_dataframe(self, mock_normalise, mock_processor):
        """Test process_timeseries_raw_dataframe delegates correctly."""
        mock_normalise.return_value = ProcessorStatus.SUCCESS
        status = mock_processor.process_timeseries_raw_dataframe()
        mock_normalise.assert_called_once_with(value_column="V")
        assert status == ProcessorStatus.SUCCESS
