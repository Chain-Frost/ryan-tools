
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
from io import StringIO

from ryan_library.processors.tuflow.max_data_processor import MaxDataProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus

# Create a concrete subclass for testing since MaxDataProcessor is abstract
class ConcreteMaxDataProcessor(MaxDataProcessor):
    def process(self) -> None:
        pass

@pytest.fixture
def mock_processor():
    file_path = Path("test_file.csv")
    with patch("ryan_library.processors.tuflow.base_processor.TuflowStringParser") as MockParser:
        # Setup mock parser
        mock_parser_instance = MockParser.return_value
        mock_parser_instance.data_type = "Cmx"
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
            mock_data_type_def.processing_parts.dataformat = "Maximums"
            mock_data_type_def.processing_parts.processor_module = "maximums_1d"
            mock_data_type_def.processing_parts.columns_to_use = {"Col1": "float", "Col2": "string"}
            mock_data_type_def.processing_parts.expected_in_header = []
            mock_data_type_def.processing_parts.to_dict.return_value = {
                "dataformat": "Maximums",
                "columns_to_use": {"Col1": "float", "Col2": "string"}
            }
            
            mock_config_instance.data_types.get.return_value = mock_data_type_def
            
            processor = ConcreteMaxDataProcessor(file_path)
            return processor

def test_read_maximums_csv_success(mock_processor):
    csv_content = "Col1,Col2\n1.0,Test"
    
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = pd.DataFrame({"Col1": [1.0], "Col2": ["Test"]})
        
        status = mock_processor.read_maximums_csv()
        
        assert status == ProcessorStatus.SUCCESS
        assert not mock_processor.df.empty
        assert "Col1" in mock_processor.df.columns
        mock_read_csv.assert_called_once()

def test_read_maximums_csv_empty(mock_processor):
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = pd.DataFrame()
        
        status = mock_processor.read_maximums_csv()
        
        assert status == ProcessorStatus.EMPTY_DATAFRAME
        assert mock_processor.df.empty

def test_read_maximums_csv_header_mismatch(mock_processor):
    # Mock df with wrong columns
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = pd.DataFrame({"WrongCol": [1.0]})
        
        status = mock_processor.read_maximums_csv()
        
        assert status == ProcessorStatus.HEADER_MISMATCH

def test_read_maximums_csv_failure(mock_processor):
    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.side_effect = Exception("Read error")
        
        status = mock_processor.read_maximums_csv()
        
        assert status == ProcessorStatus.FAILURE
