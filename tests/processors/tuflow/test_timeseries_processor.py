
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
from io import StringIO

from ryan_library.processors.tuflow.timeseries_processor import TimeSeriesProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus

# Create a concrete subclass for testing since TimeSeriesProcessor is abstract
class ConcreteTimeSeriesProcessor(TimeSeriesProcessor):
    def process(self) -> None:
        pass

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
    
    with pytest.raises(Exception):
        mock_processor._reshape_timeseries_df(df=df_clean, data_type="Q")
