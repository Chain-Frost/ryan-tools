"""Unit tests for ryan_library.classes.suffixes_and_dtypes."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from ryan_library.classes.suffixes_and_dtypes import (
    ProcessingParts,
    DataTypeDefinition,
    Config,
    SuffixesConfig,
    ConfigLoader,
)


class TestProcessingParts:
    """Tests for ProcessingParts dataclass."""

    def test_from_dict_valid(self):
        """Test parsing from a valid dictionary."""
        data = {
            "dataformat": "Timeseries",
            "module": "some_module",
            "columns_to_use": {"A": "B"},
            "expected_in_header": ["Time", "Val"],
        }
        pp = ProcessingParts.from_dict(data, "TestType")
        assert pp.dataformat == "Timeseries"
        assert pp.processor_module == "some_module"
        assert pp.columns_to_use == {"A": "B"}
        assert pp.expected_in_header == ["Time", "Val"]

    def test_from_dict_nested_dataformat(self):
        """Test parsing with nested dataformat dictionary (deprecated style)."""
        data = {
            "dataformat": {
                "category": "Timeseries",
                "module": "old_module"
            }
        }
        pp = ProcessingParts.from_dict(data, "TestType")
        assert pp.dataformat == "Timeseries"
        assert pp.processor_module == "old_module"


class TestDataTypeDefinition:
    """Tests for DataTypeDefinition dataclass."""

    def test_from_dict_valid(self):
        """Test parsing from a valid dictionary."""
        data = {
            "processor": "MyProcessor",
            "suffixes": [".csv"],
            "output_columns": {"A": "B"},
            "processingParts": {
                "dataformat": "Timeseries"
            }
        }
        dtd = DataTypeDefinition.from_dict(data, "TestType")
        assert dtd.processor == "MyProcessor"
        assert dtd.suffixes == [".csv"]
        assert dtd.output_columns == {"A": "B"}
        assert dtd.processing_parts.dataformat == "Timeseries"


class TestConfig:
    """Tests for Config singleton."""

    def setup_method(self):
        """Reset singleton before each test."""
        Config._instance = None
        Config._lock = MagicMock() # Mock lock to avoid threading issues in tests if needed, though simple reset usually works

    def teardown_method(self):
        Config._instance = None

    @patch("ryan_library.classes.suffixes_and_dtypes.ConfigLoader")
    def test_load(self, mock_loader_cls):
        """Test loading configuration."""
        mock_loader = mock_loader_cls.return_value
        mock_loader.get_data_types.return_value = {
            "TestType": {
                "processor": "TestProc",
                "suffixes": [".tst"],
                "output_columns": {},
                "processingParts": {}
            }
        }

        config = Config.load(Path("dummy.json"))
        assert "TestType" in config.data_types
        assert config.data_types["TestType"].processor == "TestProc"

    def test_singleton_behavior(self):
        """Test that get_instance returns the same object."""
        # Mock load to avoid file I/O
        with patch.object(Config, "load", return_value=Config({})) as mock_load:
            c1 = Config.get_instance()
            c2 = Config.get_instance()
            assert c1 is c2
            mock_load.assert_called_once()


class TestSuffixesConfig:
    """Tests for SuffixesConfig singleton."""

    def setup_method(self):
        SuffixesConfig._instance = None

    def teardown_method(self):
        SuffixesConfig._instance = None

    def test_load_and_lookup(self):
        """Test loading from Config and looking up suffixes."""
        # Create a dummy Config
        dtd = DataTypeDefinition(
            processor="TestProc",
            suffixes=["_test.csv"],
            output_columns={},
            processing_parts=ProcessingParts()
        )
        config = Config(data_types={"TestType": dtd})

        suffixes_config = SuffixesConfig.load(config)
        
        # Test lookup
        assert suffixes_config.get_data_type_for_suffix("file_test.csv") == "TestType"
        assert suffixes_config.get_data_type_for_suffix("file_other.csv") is None

    def test_get_processor_class(self):
        """Test retrieving processor class name."""
        dtd = DataTypeDefinition(
            processor="TestProc",
            suffixes=[],
            output_columns={},
            processing_parts=ProcessingParts()
        )
        config = Config(data_types={"TestType": dtd})
        suffixes_config = SuffixesConfig(suffix_to_type={}, config=config)

        assert suffixes_config.get_processor_class_for_data_type("TestType") == "TestProc"
        assert suffixes_config.get_processor_class_for_data_type("Unknown") is None
