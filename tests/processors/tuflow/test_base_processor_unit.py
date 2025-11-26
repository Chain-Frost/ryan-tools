import pytest
from pathlib import Path
import pandas as pd
from ryan_library.processors.tuflow.base_processor import BaseProcessor, ProcessorStatus
from ryan_library.classes.tuflow_string_classes import TuflowStringParser


# Mock concrete implementation for testing BaseProcessor
class MockProcessor(BaseProcessor):
    def process(self) -> None:
        # Minimal implementation
        self.df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        self.processed = True


@pytest.fixture
def mock_processor_file(tmp_path):
    # Create a file with a valid suffix that maps to a processor
    # We need to pick a suffix that exists in config, e.g., _1d_H.csv
    # But BaseProcessor.from_file will try to load the REAL processor class.
    # So for unit testing BaseProcessor methods, we should instantiate MockProcessor directly.
    # However, BaseProcessor.__post_init__ uses TuflowStringParser which needs a real-ish filename.
    p = tmp_path / "Test_Run_001_1d_H.csv"
    p.touch()
    return p


def test_base_processor_initialization(mock_processor_file):
    # We can instantiate MockProcessor directly
    processor = MockProcessor(file_path=mock_processor_file)
    assert processor.file_name == mock_processor_file.name
    assert processor.data_type == "H"  # Derived from suffix
    assert not processor.processed


def test_add_common_columns(mock_processor_file, change_cwd, tmp_path):
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


def test_filter_locations(mock_processor_file):
    processor = MockProcessor(file_path=mock_processor_file)
    # Setup df with Location column
    processor.df = pd.DataFrame({"Location": ["LocA", "LocB", "LocC"], "Value": [1, 2, 3]})

    # Filter for LocA and LocC
    locations = {"LocA", "LocC"}
    processor.filter_locations(locations)

    assert len(processor.df) == 2
    assert set(processor.df["Location"]) == {"LocA", "LocC"}
    assert processor.applied_location_filter == frozenset(locations)


def test_filter_locations_empty_filter(mock_processor_file):
    processor = MockProcessor(file_path=mock_processor_file)
    processor.df = pd.DataFrame({"Location": ["A"], "Value": [1]})

    # Empty filter should return everything (or nothing? logic says: if not locations: return frozenset())
    # Let's check implementation:
    # if not locations: return frozenset() -> returns early, does NOT filter.
    processor.filter_locations(None)
    assert len(processor.df) == 1

    processor.filter_locations([])
    assert len(processor.df) == 1


def test_validate_data_default(mock_processor_file):
    processor = MockProcessor(file_path=mock_processor_file)
    processor.df = pd.DataFrame({"A": [1]})
    assert processor.validate_data() is True

    processor.df = pd.DataFrame()
    assert processor.validate_data() is False


def test_check_headers_match_columns_to_use(mock_processor_file):
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


def test_check_headers_match_expected_in_header(mock_processor_file):
    processor = MockProcessor(file_path=mock_processor_file)
    processor.columns_to_use = {}  # Ensure this is empty
    processor.expected_in_header = ["Header1", "Header2"]

    assert processor.check_headers_match(["Header1", "Header2"]) is True
    assert processor.check_headers_match(["Header1"]) is False
