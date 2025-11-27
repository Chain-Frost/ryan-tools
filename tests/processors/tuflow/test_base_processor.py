"""Unit tests for ryan_library.processors.tuflow.base_processor."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pathlib import Path
import pandas as pd
from ryan_library.processors.tuflow.base_processor import (
    BaseProcessor,
    ProcessorStatus,
    ConfigurationError,
    ImportProcessorError,
)
from ryan_library.classes.suffixes_and_dtypes import (
    DataTypeDefinition,
    ProcessingParts,
    SuffixesConfig,
    Config,
)


# Concrete implementation for testing abstract BaseProcessor
class ConcreteProcessor(BaseProcessor):
    def process(self) -> None:
        pass


class TestBaseProcessor:
    """Tests for BaseProcessor class."""

    @pytest.fixture
    def mock_suffixes_config(self):
        with patch("ryan_library.processors.tuflow.base_processor.SuffixesConfig") as MockConfig:
            yield MockConfig.get_instance.return_value

    @pytest.fixture
    def mock_config(self):
        with patch("ryan_library.processors.tuflow.base_processor.Config") as MockConfig:
            yield MockConfig.get_instance.return_value

    @pytest.fixture
    def mock_parser(self):
        with patch("ryan_library.processors.tuflow.base_processor.TuflowStringParser") as MockParser:
            instance = MockParser.return_value
            instance.data_type = "TestType"
            instance.raw_run_code = "Run123"
            instance.run_code_parts = {}
            instance.trim_run_code = "Run"
            instance.tp = None
            instance.duration = None
            instance.aep = None
            yield instance

    def test_from_file_success(self, mock_suffixes_config, mock_parser):
        """Test factory method successfully creates a processor."""
        # Setup mocks
        mock_suffixes_config.get_definition_for_data_type.return_value = DataTypeDefinition(
            processor="ConcreteProcessor",
            suffixes=[".csv"],
            output_columns={},
            processing_parts=ProcessingParts(dataformat="Timeseries")
        )
        
        # Mock get_processor_class to return our ConcreteProcessor
        with patch.object(BaseProcessor, "get_processor_class", return_value=ConcreteProcessor):
            # Mock _load_configuration to avoid needing full config setup
            with patch.object(ConcreteProcessor, "_load_configuration"):
                processor = BaseProcessor.from_file(Path("test_file.csv"))
                assert isinstance(processor, ConcreteProcessor)

    def test_from_file_no_data_type(self, mock_parser):
        """Test factory fails if no data type found."""
        mock_parser.data_type = None
        with pytest.raises(ValueError, match="No data type found"):
            BaseProcessor.from_file(Path("unknown.csv"))

    def test_from_file_no_processor_class(self, mock_suffixes_config, mock_parser):
        """Test factory fails if no processor class defined."""
        mock_suffixes_config.get_definition_for_data_type.return_value = None
        with pytest.raises(KeyError, match="No processor class specified"):
            BaseProcessor.from_file(Path("test.csv"))

    def test_load_configuration_success(self, mock_config, mock_parser):
        """Test loading configuration successfully."""
        # Setup Config mock
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={"ColA": "float"},
                processing_parts=ProcessingParts(
                    dataformat="Timeseries",
                    expected_in_header=["Time", "Val"]
                )
            )
        }

        processor = ConcreteProcessor(Path("test.csv"))
        # _load_configuration is called in __post_init__
        assert processor.output_columns == {"ColA": "float"}
        assert processor.expected_in_header == ["Time", "Val"]

    def test_load_configuration_missing_type(self, mock_config, mock_parser):
        """Test loading configuration fails for unknown type."""
        mock_config.data_types = {}
        with pytest.raises(KeyError, match="Data type 'TestType' is not defined"):
            ConcreteProcessor(Path("test.csv"))

    def test_check_headers_match_success(self, mock_config, mock_parser):
        """Test header validation success."""
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={},
                processing_parts=ProcessingParts(
                    dataformat="Timeseries",
                    expected_in_header=["Time", "Val"]
                )
            )
        }
        processor = ConcreteProcessor(Path("test.csv"))
        assert processor.check_headers_match(["Time", "Val"]) is True

    def test_check_headers_match_failure(self, mock_config, mock_parser):
        """Test header validation failure."""
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={},
                processing_parts=ProcessingParts(
                    dataformat="Timeseries",
                    expected_in_header=["Time", "Val"]
                )
            )
        }
        processor = ConcreteProcessor(Path("test.csv"))
        assert processor.check_headers_match(["Time", "Wrong"]) is False

    @patch("pandas.read_csv")
    def test_read_maximums_csv_success(self, mock_read_csv, mock_config, mock_parser):
        """Test reading CSV successfully."""
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={},
                processing_parts=ProcessingParts(
                    dataformat="Maximums",
                    columns_to_use={"ColA": "float"}
                )
            )
        }
        mock_df = pd.DataFrame({"ColA": [1.0]})
        mock_read_csv.return_value = mock_df
        
        processor = ConcreteProcessor(Path("test.csv"))
        status = processor.read_maximums_csv()
        
        assert status == ProcessorStatus.SUCCESS
        assert not processor.df.empty
        mock_read_csv.assert_called_once()

    @patch("pandas.read_csv")
    def test_read_maximums_csv_empty(self, mock_read_csv, mock_config, mock_parser):
        """Test reading empty CSV."""
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={},
                processing_parts=ProcessingParts(
                    dataformat="Maximums",
                    columns_to_use={"ColA": "float"}
                )
            )
        }
        mock_read_csv.return_value = pd.DataFrame()
        
        processor = ConcreteProcessor(Path("test.csv"))
        status = processor.read_maximums_csv()
        
        assert status == ProcessorStatus.EMPTY_DATAFRAME

    def test_filter_locations(self, mock_config, mock_parser):
        """Test filtering locations."""
        mock_config.data_types = {
            "TestType": DataTypeDefinition(
                processor="ConcreteProcessor",
                suffixes=[".csv"],
                output_columns={},
                processing_parts=ProcessingParts(
                    dataformat="Timeseries",
                    expected_in_header=["Time", "Val"]
                )
            )
        }
        processor = ConcreteProcessor(Path("test.csv"))
        processor.df = pd.DataFrame({"Location": ["Loc1", "Loc2", "Loc3"], "Val": [1, 2, 3]})
        
        processor.filter_locations(["Loc1", "Loc3"])
        
        assert len(processor.df) == 2
        assert "Loc2" not in processor.df["Location"].values
