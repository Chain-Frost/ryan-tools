"""Unit tests for ryan_library.processors.tuflow.base_processor."""

import pytest
from unittest.mock import MagicMock, patch
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
)


# Mock concrete implementation for testing BaseProcessor
class MockProcessor(BaseProcessor):
    def process(self) -> None:
        # Minimal implementation
        self.df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        self.processed = True


@pytest.fixture
def mock_processor_file(tmp_path):
    # Create a file with a valid suffix that maps to a processor
    p = tmp_path / "Test_Run_001_1d_H.csv"
    p.touch()
    return p


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

    def test_base_processor_initialization(self, mock_processor_file):
        # We can instantiate MockProcessor directly
        processor = MockProcessor(file_path=mock_processor_file)
        assert processor.file_name == mock_processor_file.name
        assert processor.data_type == "H"  # Derived from suffix
        assert not processor.processed

    def test_from_file_success(self, mock_suffixes_config, mock_parser):
        """Test factory method successfully creates a processor."""
        # Setup mocks
        mock_suffixes_config.get_definition_for_data_type.return_value = DataTypeDefinition(
            processor="MockProcessor",
            suffixes=[".csv"],
            output_columns={},
            processing_parts=ProcessingParts(dataformat="Timeseries")
        )
        
        # Mock get_processor_class to return our MockProcessor
        with patch.object(BaseProcessor, "get_processor_class", return_value=MockProcessor):
            # Mock _load_configuration to avoid needing full config setup
            with patch.object(MockProcessor, "_load_configuration"):
                processor = BaseProcessor.from_file(Path("test_file.csv"))
                assert isinstance(processor, MockProcessor)

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

    def test_add_common_columns(self, mock_processor_file, change_cwd, tmp_path):
        with change_cwd(tmp_path):
            processor = MockProcessor(file_path=mock_processor_file)
            processor.process()  # Sets self.df

            processor.add_common_columns()

            assert "internalName" in processor.df.columns
            assert "file" in processor.df.columns
            assert "rel_path" in processor.df.columns
            assert "directory_path" in processor.df.columns

            # Check values
            assert processor.df["internalName"].iloc[0] == "Test_Run_001"
            assert processor.df["file"].iloc[0] == "Test_Run_001_1d_H.csv"

    def test_filter_locations(self, mock_processor_file):
        processor = MockProcessor(file_path=mock_processor_file)
        # Setup df with Location column
        processor.df = pd.DataFrame({"Location": ["LocA", "LocB", "LocC"], "Value": [1, 2, 3]})

        # Filter for LocA and LocC
        locations = {"LocA", "LocC"}
        processor.filter_locations(locations)

        assert len(processor.df) == 2
        assert set(processor.df["Location"]) == {"LocA", "LocC"}
        assert processor.applied_location_filter == frozenset(locations)

    def test_filter_locations_empty_filter(self, mock_processor_file):
        processor = MockProcessor(file_path=mock_processor_file)
        processor.df = pd.DataFrame({"Location": ["A"], "Value": [1]})

        processor.filter_locations(None)
        assert len(processor.df) == 1

        processor.filter_locations([])
        assert len(processor.df) == 1

    def test_validate_data_default(self, mock_processor_file):
        processor = MockProcessor(file_path=mock_processor_file)
        processor.df = pd.DataFrame({"A": [1]})
        assert processor.validate_data() is True

        processor.df = pd.DataFrame()
        assert processor.validate_data() is False

    def test_check_headers_match_columns_to_use(self, mock_processor_file):
        processor = MockProcessor(file_path=mock_processor_file)
        # Manually set columns_to_use
        processor.columns_to_use = {"Col1": "float", "Col2": "string"}

        # Exact match
        assert processor.check_headers_match(["Col1", "Col2"]) is True

        # Order mismatch (should warn but pass)
        assert processor.check_headers_match(["Col2", "Col1"]) is True

        # Missing column
        assert processor.check_headers_match(["Col1"]) is False

        # Extra column
        assert processor.check_headers_match(["Col1", "Col2", "Extra"]) is False

    def test_check_headers_match_expected_in_header(self, mock_processor_file):
        processor = MockProcessor(file_path=mock_processor_file)
        processor.columns_to_use = {}  # Ensure this is empty
        processor.expected_in_header = ["Header1", "Header2"]

        assert processor.check_headers_match(["Header1", "Header2"]) is True
        assert processor.check_headers_match(["Header1"]) is False

    @patch("pandas.read_csv")
    def test_read_maximums_csv_success(self, mock_read_csv, mock_processor_file):
        """Test reading CSV successfully using BaseProcessor implementation."""
        mock_df = pd.DataFrame({"ColA": [1.0]})
        mock_read_csv.return_value = mock_df
        
        processor = MockProcessor(file_path=mock_processor_file)
        # Setup config on instance manually since we are bypassing _load_configuration
        processor.columns_to_use = {"ColA": "float"}
        
        status = processor.read_maximums_csv()
        
        assert status == ProcessorStatus.SUCCESS
        assert not processor.df.empty
        mock_read_csv.assert_called_once()

    @patch("pandas.read_csv")
    def test_read_maximums_csv_empty(self, mock_read_csv, mock_processor_file):
        """Test reading empty CSV."""
        mock_read_csv.return_value = pd.DataFrame()
        
        processor = MockProcessor(file_path=mock_processor_file)
        processor.columns_to_use = {"ColA": "float"}
        
        status = processor.read_maximums_csv()
        
        assert status == ProcessorStatus.EMPTY_DATAFRAME
