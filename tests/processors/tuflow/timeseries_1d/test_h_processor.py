"""Unit tests for ryan_library.processors.tuflow.timeseries_1d.HProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.HProcessor import HProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestHProcessor:
    """Tests for HProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock HProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = HProcessor(Path("test_1d_H.csv"))
            processor.file_name = "test_1d_H.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "H"
            return processor

    @patch("ryan_library.processors.tuflow.timeseries_1d.HProcessor.HProcessor._process_timeseries_pipeline")
    def test_process(self, mock_pipeline, mock_processor):
        """Test process method delegates correctly."""
        mock_processor.process()
        mock_pipeline.assert_called_once_with(data_type="H")

    @patch("ryan_library.processors.tuflow.timeseries_1d.HProcessor.HProcessor.check_headers_match")
    def test_process_timeseries_raw_dataframe_success(self, mock_check_headers, mock_processor):
        """Test successful normalization of H dataframe."""
        mock_check_headers.return_value = True
        
        # Setup df with required columns and an identifier
        mock_processor.df = pd.DataFrame({
            "Time": [0.0, 1.0],
            "US_H": [10.0, 10.1],
            "DS_H": [9.0, 9.1],
            "Chan ID": ["C1", "C1"]
        })

        status = mock_processor.process_timeseries_raw_dataframe()
        
        assert status == ProcessorStatus.SUCCESS
        assert list(mock_processor.df.columns) == ["Time", "Chan ID", "US_H", "DS_H"]
        mock_check_headers.assert_called_once()

    def test_process_timeseries_raw_dataframe_missing_columns(self, mock_processor):
        """Test failure when required columns are missing."""
        mock_processor.df = pd.DataFrame({
            "Time": [0.0],
            "US_H": [10.0]
            # Missing DS_H
        })

        status = mock_processor.process_timeseries_raw_dataframe()
        assert status == ProcessorStatus.FAILURE

    def test_process_timeseries_raw_dataframe_no_identifier(self, mock_processor):
        """Test failure when no identifier column is found."""
        mock_processor.df = pd.DataFrame({
            "Time": [0.0],
            "US_H": [10.0],
            "DS_H": [9.0]
        })

        status = mock_processor.process_timeseries_raw_dataframe()
        assert status == ProcessorStatus.FAILURE

    def test_process_timeseries_raw_dataframe_multiple_identifiers(self, mock_processor):
        """Test failure when multiple identifier columns are found."""
        mock_processor.df = pd.DataFrame({
            "Time": [0.0],
            "US_H": [10.0],
            "DS_H": [9.0],
            "Chan ID": ["C1"],
            "Extra": ["X"]
        })

        status = mock_processor.process_timeseries_raw_dataframe()
        assert status == ProcessorStatus.FAILURE

    @patch("ryan_library.processors.tuflow.timeseries_1d.HProcessor.HProcessor.check_headers_match")
    def test_process_timeseries_raw_dataframe_empty_after_drop(self, mock_check_headers, mock_processor):
        """Test failure when dataframe becomes empty after dropping NaNs."""
        mock_processor.df = pd.DataFrame({
            "Time": [0.0],
            "US_H": [None],
            "DS_H": [None],
            "Chan ID": ["C1"]
        })

        status = mock_processor.process_timeseries_raw_dataframe()
        assert status == ProcessorStatus.EMPTY_DATAFRAME
