"""Unit tests for ryan_library.processors.tuflow.timeseries_1d.CFProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.CFProcessor import CFProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestCFProcessor:
    """Tests for CFProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock CFProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = CFProcessor(Path("test_1d_CF.csv"))
            processor.file_name = "test_1d_CF.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "CF"
            return processor

    @patch("ryan_library.processors.tuflow.timeseries_1d.CFProcessor.CFProcessor._process_timeseries_pipeline")
    def test_process(self, mock_pipeline, mock_processor):
        """Test process method delegates correctly."""
        mock_processor.process()
        mock_pipeline.assert_called_once_with(data_type="CF")

    @patch("ryan_library.processors.tuflow.timeseries_1d.CFProcessor.CFProcessor._normalise_value_dataframe")
    def test_process_timeseries_raw_dataframe(self, mock_normalise, mock_processor):
        """Test process_timeseries_raw_dataframe delegates correctly."""
        mock_normalise.return_value = ProcessorStatus.SUCCESS
        status = mock_processor.process_timeseries_raw_dataframe()
        mock_normalise.assert_called_once_with(value_column="CF")
        assert status == ProcessorStatus.SUCCESS

    @patch("ryan_library.processors.tuflow.timeseries_processor.TimeSeriesProcessor._clean_column_names")
    def test_clean_column_names(self, mock_super_clean, mock_processor):
        """Test _clean_column_names delegates with correct data_type."""
        columns = pd.Index(["A", "B"])
        mock_processor._clean_column_names(columns, "CF")
        mock_super_clean.assert_called_once_with(columns=columns, data_type="F")

    @patch("ryan_library.processors.tuflow.timeseries_1d.CFProcessor.CFProcessor.apply_dtype_mapping")
    def test_apply_final_transformations(self, mock_apply, mock_processor):
        """Test _apply_final_transformations applies correct mapping."""
        mock_processor._apply_final_transformations("CF")
        mock_apply.assert_called_once_with(
            dtype_mapping={"Time": "float64", "CF": "string"},
            context="final_transformations_cf",
        )
