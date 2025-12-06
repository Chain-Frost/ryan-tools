"""Additional coverage tests for ryan_library.classes.suffixes_and_dtypes."""

import pytest
from unittest.mock import MagicMock, patch
import json
from ryan_library.classes.suffixes_and_dtypes import (
    ConfigLoader,
    ProcessingParts,
    DataTypeDefinition,
    SuffixesConfig,
    Config,
)

class TestConfigLoaderCoverage:
    def test_load_json_config_success(self, tmp_path):
        """Test successful JSON load."""
        f = tmp_path / "config.json"
        data = {"test": "data"}
        f.write_text(json.dumps(data), encoding="utf-8")
        
        loader = ConfigLoader(f)
        assert loader.config_data == data

    def test_load_json_config_not_found(self, tmp_path):
        """Test FileNotFoundError."""
        f = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError):
            ConfigLoader(f)

    def test_load_json_config_decode_error(self, tmp_path):
        """Test JSONDecodeError."""
        f = tmp_path / "bad.json"
        f.write_text("{invalid json", encoding="utf-8")
        with pytest.raises(ValueError):
            ConfigLoader(f)

    def test_get_data_types_invalid(self, tmp_path):
        """Test get_data_types with invalid structure."""
        f = tmp_path / "config.json"
        # Top level is list, expected dict
        f.write_text("[]", encoding="utf-8")
        with pytest.raises(ValueError):
            ConfigLoader(f)

class TestProcessingPartsCoverage:
    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_invalid_module(self, mock_logger):
        """Test invalid module value."""
        data = {"module": 123}  # Not a string
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_dataformat_invalid_category(self, mock_logger):
        """Test invalid dataformat category."""
        data = {"dataformat": {"category": 123}}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_dataformat_deprecated_module(self, mock_logger):
        """Test deprecated dataformat.module."""
        data = {"dataformat": {"module": "dep_mod"}}
        pp = ProcessingParts.from_dict(data, "TestType")
        assert pp.processor_module == "dep_mod"
        mock_logger.warning.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_dataformat_invalid_module_type(self, mock_logger):
        """Test invalid dataformat.module type."""
        data = {"dataformat": {"module": 123}}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_invalid_dataformat_type(self, mock_logger):
        """Test invalid dataformat type (not str or dict)."""
        data = {"dataformat": 123}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_invalid_columns_to_use(self, mock_logger):
        """Test invalid columns_to_use."""
        # Case 1: Not a dict
        data = {"columns_to_use": []}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()
        
        # Case 2: Values not strings
        mock_logger.reset_mock()
        data = {"columns_to_use": {"A": 1}}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_invalid_expected_in_header(self, mock_logger):
        """Test invalid expected_in_header."""
        data = {"expected_in_header": "not a list"}
        with pytest.raises(ValueError):
            ProcessingParts.from_dict(data, "TestType")
        mock_logger.error.assert_called()

class TestDataTypeDefinitionCoverage:
    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_from_dict_invalid_fields(self, mock_logger):
        """Test invalid fields in DataTypeDefinition."""
        data = {
            "processor": 123,  # Invalid
            "suffixes": "not list",  # Invalid
            "output_columns": "not dict",  # Invalid
            "processingParts": "not dict",  # Invalid
        }
        with pytest.raises(ValueError):
            DataTypeDefinition.from_dict(data, "TestType")
        assert mock_logger.error.call_count >= 1

class TestSuffixesConfigCoverage:
    def test_invert_suffix_to_type(self):
        """Test invert_suffix_to_type."""
        suffix_to_type = {".a": "TypeA", ".b": "TypeA", ".c": "TypeB"}
        config = MagicMock(spec=Config)
        sc = SuffixesConfig(suffix_to_type, config)
        
        inverted = sc.invert_suffix_to_type()
        assert set(inverted["TypeA"]) == {".a", ".b"}
        assert inverted["TypeB"] == [".c"]

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_get_definition_for_data_type_missing(self, mock_logger):
        """Test get_definition_for_data_type with missing type."""
        config = MagicMock(spec=Config)
        config.data_types = {}
        sc = SuffixesConfig({}, config)
        
        assert sc.get_definition_for_data_type("Missing") is None
        mock_logger.error.assert_called()

    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_get_processor_class_missing(self, mock_logger):
        """Test get_processor_class_for_data_type with missing type."""
        config = MagicMock(spec=Config)
        config.data_types = {}
        sc = SuffixesConfig({}, config)
        
        assert sc.get_processor_class_for_data_type("Missing") is None
        mock_logger.error.assert_called()

class TestConfigCoverage:
    @patch("ryan_library.classes.suffixes_and_dtypes.logger")
    def test_load_invalid_data_type_entry(self, mock_logger, tmp_path):
        """Test loading config with invalid data type entry (not a dict)."""
        f = tmp_path / "config.json"
        data = {"InvalidType": "not a dict"}
        f.write_text(json.dumps(data), encoding="utf-8")

        with pytest.raises(ValueError):
            Config.load(f)
        mock_logger.error.assert_called()
