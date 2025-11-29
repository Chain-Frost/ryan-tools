"""Unit tests for ryan_library.processors.tuflow.timeseries_1d.QProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.QProcessor import QProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestQProcessor:
    """Tests for QProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock QProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = QProcessor(Path("test_1d_Q.csv"))
            processor.file_name = "test_1d_Q.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "Q"
            return processor

    @patch("ryan_library.processors.tuflow.timeseries_1d.QProcessor.QProcessor._process_timeseries_pipeline")
    def test_process(self, mock_pipeline, mock_processor):
        """Test process method delegates correctly."""
        mock_processor.process()
        mock_pipeline.assert_called_once_with(data_type="Q")

    @patch("ryan_library.processors.tuflow.timeseries_1d.QProcessor.QProcessor._normalise_value_dataframe")
    def test_process_timeseries_raw_dataframe(self, mock_normalise, mock_processor):
        """Test process_timeseries_raw_dataframe delegates correctly."""
        mock_normalise.return_value = ProcessorStatus.SUCCESS
        status = mock_processor.process_timeseries_raw_dataframe()
        mock_normalise.assert_called_once_with(value_column="Q")
        assert status == ProcessorStatus.SUCCESS
