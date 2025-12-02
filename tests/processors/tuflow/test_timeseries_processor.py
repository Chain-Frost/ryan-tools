
"""Unit tests for ryan_library.processors.tuflow.timeseries_processor."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
from io import StringIO

from ryan_library.processors.tuflow.timeseries_processor import TimeSeriesProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus, DataValidationError, ProcessorError

# Create a concrete subclass for testing since TimeSeriesProcessor is abstract
class ConcreteTimeSeriesProcessor(TimeSeriesProcessor):
    def process(self) -> None:
        # We override process in base usually, but TimeSeriesProcessor implements it.
        # Wait, TimeSeriesProcessor implements process().
        # So we can just call super().process() if we want, or use the implementation.
        # But we need to implement abstract methods.
        return super().process()

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        return ProcessorStatus.SUCCESS

@pytest.fixture
def mock_processor():
    file_path = Path("test_file.csv")
    with patch("ryan_library.processors.tuflow.base_processor.TuflowStringParser") as MockParser:
        # Setup mock parser
        mock_parser_instance = MockParser.return_value
        mock_parser_instance.data_type = "H"
        mock_parser_instance.raw_run_code = "TestRun"
        mock_parser_instance.run_code_parts = {}
        mock_parser_instance.trim_run_code = "TestRun"
        mock_parser_instance.tp = None
        mock_parser_instance.duration = None
        mock_parser_instance.aep = None
        
        # Setup mock config loading
        with patch("ryan_library.processors.tuflow.base_processor.Config.get_instance") as MockConfig:
            mock_config_instance = MockConfig.return_value
            mock_data_type_def = MagicMock()
            mock_data_type_def.output_columns = {}
            mock_data_type_def.processing_parts.dataformat = "Timeseries"
            mock_data_type_def.processing_parts.processor_module = "timeseries_1d"
            mock_data_type_def.processing_parts.columns_to_use = {}
            mock_data_type_def.processing_parts.expected_in_header = ["Time", "Chan ID", "Val"]
            mock_data_type_def.processing_parts.to_dict.return_value = {
                "dataformat": "Timeseries",
                "expected_in_header": ["Time", "Chan ID", "Val"]
            }
            
            mock_config_instance.data_types.get.return_value = mock_data_type_def
            
            processor = ConcreteTimeSeriesProcessor(file_path)
            return processor

def test_read_and_process_timeseries_csv_success(mock_processor):
    # Mock read_csv to return a DataFrame
    with patch("pandas.read_csv") as mock_read_csv:
        # Return a DF that needs cleaning (first col dropped)
        mock_read_csv.return_value = pd.DataFrame({
            "Descriptor": ["Desc", "Desc"],
            "Time": [0.0, 1.0],
            "Q Chan1": [10.0, 11.0]
        })
        
        status = mock_processor.read_and_process_timeseries_csv(data_type="Q")
        
        assert status == ProcessorStatus.SUCCESS
        assert not mock_processor.df.empty
        assert "Time" in mock_processor.df.columns
        # After reshape, it should have Location and Q (since file name doesn't have _1d_)
        assert "Q" in mock_processor.df.columns
        
        mock_read_csv.assert_called_once()

def test_read_and_process_timeseries_csv_empty(mock_processor):
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = pd.DataFrame()
        
        status = mock_processor.read_and_process_timeseries_csv(data_type="Q")
        
        assert status == ProcessorStatus.EMPTY_DATAFRAME
        # self.df might not be empty if it initializes to empty DF, but the status is key.

def test_read_and_process_timeseries_csv_failure(mock_processor):
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.side_effect = Exception("Read error")
        
        status = mock_processor.read_and_process_timeseries_csv(data_type="Q")
        
        assert status == ProcessorStatus.FAILURE

def test_reshape_timeseries_df_success(mock_processor):
    # Setup cleaned dataframe
    df_clean = pd.DataFrame({
        "Time": [0.0, 1.0],
        "Chan1": [10.0, 11.0],
        "Chan2": [20.0, 21.0]
    })
    
    # Mock file_name to be 1d so it uses "Chan ID"
    mock_processor.file_name = "Test_1d_Q.csv"
    
    df_melted = mock_processor._reshape_timeseries_df(df=df_clean, data_type="Q")
    
    assert not df_melted.empty
    assert "Chan ID" in df_melted.columns
    assert "Q" in df_melted.columns
    assert "Time" in df_melted.columns
    # Check shape: 2 times * 2 channels = 4 rows
    assert len(df_melted) == 4

def test_reshape_timeseries_df_missing_time(mock_processor):
    # Setup dataframe without Time column
    df_clean = pd.DataFrame({
        "Chan1": [10.0, 11.0]
    })
    
    # _reshape_timeseries_df expects Time column to exist (checked in _clean_headers usually)
    # But melt will fail or produce weird results if Time is missing.
    # Actually _reshape_timeseries_df assumes Time exists.
    # If we pass it without Time, melt might fail if id_vars=["Time"] is used.
    
    with pytest.raises(ProcessorError):
        mock_processor._reshape_timeseries_df(df=df_clean, data_type="Q")

def test_clean_headers(mock_processor):
    """Test header cleaning."""
    df = pd.DataFrame({
        "Descriptor": ["D"],
        "Time (h)": [0.0],
        "Q Chan1": [10.0]
    })
    
    cleaned = mock_processor._clean_headers(df, data_type="Q")
    
    assert "Descriptor" not in cleaned.columns
    assert "Time" in cleaned.columns
    assert "Chan1" in cleaned.columns # Q prefix stripped

def test_clean_headers_missing_time(mock_processor):
    """Test header cleaning fails if Time is missing."""
    df = pd.DataFrame({
        "Descriptor": ["D"],
        "Val": [10.0]
    })
    
    with pytest.raises(ProcessorError, match="Time' column is missing"):
        mock_processor._clean_headers(df, data_type="Q")

def test_clean_column_names(mock_processor):
    """Test column name cleaning."""
    cols = pd.Index(["Q Chan1", "Q ds1 [M11_5m_001]", "Other"])
    cleaned = mock_processor._clean_column_names(cols, data_type="Q")
    
    assert cleaned == ["Chan1", "ds1", "Other"]

def test_normalise_value_dataframe_success(mock_processor):
    """Test normalisation of value dataframe."""
    mock_processor.df = pd.DataFrame({
        "Time": [0.0, 1.0],
        "Chan ID": ["C1", "C1"],
        "Q": [10.0, 11.0]
    })
    mock_processor.file_name = "Test_1d_Q.csv"
    # Update expected headers to match what we expect after normalisation
    mock_processor.expected_in_header = ["Time", "Chan ID", "Q"]
    
    status = mock_processor._normalise_value_dataframe(value_column="Q")
    
    assert status == ProcessorStatus.SUCCESS
    assert list(mock_processor.df.columns) == ["Time", "Chan ID", "Q"]

def test_normalise_value_dataframe_missing_columns(mock_processor):
    """Test normalisation fails with missing columns."""
    mock_processor.df = pd.DataFrame({
        "Time": [0.0]
        # Missing Q
    })
    
    status = mock_processor._normalise_value_dataframe(value_column="Q")
    assert status == ProcessorStatus.FAILURE

def test_normalise_value_dataframe_multiple_identifiers(mock_processor):
    """Test normalisation fails with multiple identifiers."""
    mock_processor.df = pd.DataFrame({
        "Time": [0.0],
        "Chan ID": ["C1"],
        "OtherID": ["O1"],
        "Q": [10.0]
    })
    
    status = mock_processor._normalise_value_dataframe(value_column="Q")
    assert status == ProcessorStatus.FAILURE

def test_process_pipeline_success(mock_processor):
    """Test the full process pipeline."""
    with patch.object(mock_processor, "read_and_process_timeseries_csv", return_value=ProcessorStatus.SUCCESS) as mock_read:
        with patch.object(mock_processor, "process_timeseries_raw_dataframe", return_value=ProcessorStatus.SUCCESS) as mock_process_raw:
            with patch.object(mock_processor, "add_common_columns") as mock_add:
                with patch.object(mock_processor, "apply_output_transformations") as mock_apply:
                    with patch.object(mock_processor, "validate_data", return_value=True) as mock_validate:
                        
                        mock_processor.process()
                        
                        assert mock_processor.processed is True
                        mock_read.assert_called_once()
                        mock_process_raw.assert_called_once()
                        mock_add.assert_called_once()
                        mock_apply.assert_called_once()
                        mock_validate.assert_called_once()

def test_process_pipeline_read_failure(mock_processor):
    """Test pipeline aborts on read failure."""
    with patch.object(mock_processor, "read_and_process_timeseries_csv", return_value=ProcessorStatus.FAILURE):
        mock_processor.process()
        assert mock_processor.processed is False

def test_process_pipeline_raw_process_failure(mock_processor):
    """Test pipeline aborts on raw process failure."""
    with patch.object(mock_processor, "read_and_process_timeseries_csv", return_value=ProcessorStatus.SUCCESS):
        with patch.object(mock_processor, "process_timeseries_raw_dataframe", return_value=ProcessorStatus.FAILURE):
            mock_processor.process()
            assert mock_processor.processed is False

def test_process_pipeline_validation_failure(mock_processor):
    """Test pipeline aborts on validation failure."""
    with patch.object(mock_processor, "read_and_process_timeseries_csv", return_value=ProcessorStatus.SUCCESS):
        with patch.object(mock_processor, "process_timeseries_raw_dataframe", return_value=ProcessorStatus.SUCCESS):
            with patch.object(mock_processor, "validate_data", return_value=False):
                mock_processor.process()
                assert mock_processor.processed is False
