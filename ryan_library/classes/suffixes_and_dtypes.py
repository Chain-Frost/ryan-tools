# ryan_library/classes/suffixes_and_dtypes.py

import json
from pathlib import Path
from loguru import logger
from dataclasses import dataclass, field
from typing import Any, ClassVar, cast
from threading import Lock


def _empty_str_dict() -> dict[str, str]:
    return {}


def _empty_str_list() -> list[str]:
    return []


class ConfigLoader:
    """A centralized configuration loader that loads a single JSON configuration file
    and provides access to its components.

    This loader is intentionally fail-fast: missing files or malformed JSON raise
    immediately so callers do not proceed with partial configuration."""

    def __init__(self, config_path: Path) -> None:
        self.config_path: Path = config_path
        self.config_data: dict[str, Any] = self.load_json_config()

    def load_json_config(self) -> dict[str, Any]:
        """Load the central JSON configuration file.

        Returns:
            dict[str, Any]: The loaded JSON data."""
        try:
            with self.config_path.open("r", encoding="utf-8") as file:
                config: Any = json.load(file)
                if not isinstance(config, dict):
                    message: str = f"Configuration root is not a mapping in {self.config_path}"
                    logger.error(message)
                    raise ValueError(message)
                typed_config: dict[str, Any] = cast(dict[str, Any], config)
                logger.debug("Loaded configuration from {}: {}", self.config_path, typed_config)
                return typed_config
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            message = f"Error decoding JSON from {self.config_path}: {e}"
            logger.error(message)
            raise ValueError(message)

    def get_data_types(self) -> dict[str, Any]:
        """Extract the data types section from the configuration.

        Returns:
            dict[str, Any]: The data types configuration."""
        data_types: dict[str, Any] = self.config_data
        logger.debug("Data types loaded: {}", list(data_types.keys()))
        return data_types


@dataclass
class ProcessingParts:
    """Encapsulates all processing-related configurations for a data type."""

    dataformat: str = ""
    processor_module: str | None = None
    columns_to_use: dict[str, str] = field(default_factory=_empty_str_dict)
    expected_in_header: list[str] = field(default_factory=_empty_str_list)

    @classmethod
    def from_dict(cls, data: dict[str, Any], data_type_name: str) -> "ProcessingParts":
        dataformat_raw: Any = data.get("dataformat", "")
        dataformat: str = ""

        module_raw: Any = data.get("module")
        processor_module: str | None = None
        if isinstance(module_raw, str) and module_raw.strip():
            processor_module = module_raw.strip()
        elif module_raw is not None and module_raw != "":
            message = f"Invalid 'module' value for processingParts in '{data_type_name}'. Expected a non-empty string."
            logger.error(message)
            raise ValueError(message)

        if isinstance(dataformat_raw, dict):
            df_dict: dict[str, Any] = cast(dict[str, Any], dataformat_raw)
            category_value: Any = df_dict.get("category")
            if isinstance(category_value, str):
                dataformat = category_value
            elif category_value is not None:
                message = f"Invalid 'category' value for dataformat in '{data_type_name}'. Expected a string."
                logger.error(message)
                raise ValueError(message)

            module_value: Any = df_dict.get("module")
            if module_value is not None and module_value != "":
                if isinstance(module_value, str) and module_value.strip():
                    if processor_module is None:
                        processor_module = module_value.strip()
                    logger.warning(
                        "'dataformat.module' in '{}' is deprecated; move it to 'processingParts.module'.",
                        data_type_name,
                    )
                else:
                    message: str = (
                        f"Invalid 'module' value for dataformat in '{data_type_name}'. Expected a non-empty string."
                    )
                    logger.error(message)
                    raise ValueError(message)
        elif isinstance(dataformat_raw, str):
            dataformat = dataformat_raw
        elif dataformat_raw is not None and dataformat_raw != "":
            message = f"Invalid format for dataformat in '{data_type_name}'. Expected a string or mapping."
            logger.error(message)
            raise ValueError(message)

        if "skip_columns" in data:
            message = f"'skip_columns' is no longer supported for '{data_type_name}'. Remove it from configuration."
            logger.error(message)
            raise ValueError(message)

        columns_to_use_raw: Any = data.get("columns_to_use", {})
        columns_to_use: dict[str, str] = {}
        if not isinstance(columns_to_use_raw, dict):
            message = f"Invalid format for columns_to_use in '{data_type_name}'. Expected a dictionary."
            logger.error(message)
            raise ValueError(message)
        columns_to_use_raw_dict: dict[object, object] = cast(dict[object, object], columns_to_use_raw)
        for k, v in columns_to_use_raw_dict.items():
            if isinstance(k, str) and isinstance(v, str):
                columns_to_use[k] = v
            else:
                message = f"Non-string key or value in columns_to_use in '{data_type_name}'."
                logger.error(message)
                raise ValueError(message)

        expected_in_header_raw: Any = data.get("expected_in_header", [])
        expected_in_header: list[str] = []
        if isinstance(expected_in_header_raw, list):
            expected_in_header_raw_list: list[object] = cast(list[object], expected_in_header_raw)
            for item in expected_in_header_raw_list:
                if isinstance(item, str):
                    expected_in_header.append(item)
                else:
                    message = f"Non-string item in expected_in_header in '{data_type_name}'."
                    logger.error(message)
                    raise ValueError(message)
        else:
            message = f"Invalid format for expected_in_header in '{data_type_name}'. Expected a list of strings."
            logger.error(message)
            raise ValueError(message)

        logger.debug(
            "ProcessingParts loaded for '{}': dataformat={}, processor_module={}, columns_to_use={}, "
            "expected_in_header={}",
            data_type_name,
            dataformat,
            processor_module,
            columns_to_use,
            expected_in_header,
        )
        return cls(
            dataformat=dataformat,
            processor_module=processor_module,
            columns_to_use=columns_to_use,
            expected_in_header=expected_in_header,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the ProcessingParts instance back to a dictionary.

        Returns:
            dict[str, Any]: The processing parts as a dictionary."""
        data: dict[str, Any] = {
            "dataformat": self.dataformat,
            "columns_to_use": self.columns_to_use,
            "expected_in_header": self.expected_in_header,
        }
        if self.processor_module:
            data["module"] = self.processor_module
        return data


@dataclass
class DataTypeDefinition:
    processor: str
    suffixes: list[str]
    output_columns: dict[str, str]
    processing_parts: ProcessingParts

    @classmethod
    def from_dict(cls, data: dict[str, Any], data_type_name: str) -> "DataTypeDefinition":
        processor: Any = data.get("processor")
        p_str: str = ""
        if isinstance(processor, str):
            p_str = processor
        else:
            message: str = f"Invalid or missing 'processor' in '{data_type_name}'. Expected a string."
            logger.error(message)
            raise ValueError(message)

        suffixes_raw: Any = data.get("suffixes", [])
        suffixes: list[str] = []
        if isinstance(suffixes_raw, list):
            suffixes_raw_list: list[object] = cast(list[object], suffixes_raw)
            for item in suffixes_raw_list:
                if isinstance(item, str):
                    suffixes.append(item)
                else:
                    message = f"Non-string item in suffixes in '{data_type_name}'."
                    logger.error(message)
                    raise ValueError(message)
        else:
            message = f"Invalid format for suffixes in '{data_type_name}'. Expected a list."
            logger.error(message)
            raise ValueError(message)

        output_columns_raw: Any = data.get("output_columns", {})
        output_columns: dict[str, str] = {}
        if not isinstance(output_columns_raw, dict):
            message = f"Invalid format for output_columns in '{data_type_name}'. Expected a dictionary."
            logger.error(message)
            raise ValueError(message)
        output_columns_raw_dict: dict[object, object] = cast(dict[object, object], output_columns_raw)
        for k, v in output_columns_raw_dict.items():
            if isinstance(k, str) and isinstance(v, str):
                output_columns[k] = v
            else:
                message = f"Non-string key or value in output_columns in '{data_type_name}'."
                logger.error(message)
                raise ValueError(message)

        processing_parts_data: Any = data.get("processingParts", {})
        parts: ProcessingParts
        if isinstance(processing_parts_data, dict):
            processing_parts_dict: dict[str, Any] = cast(dict[str, Any], processing_parts_data)
            parts = ProcessingParts.from_dict(data=processing_parts_dict, data_type_name=data_type_name)
        else:
            message = f"Invalid format for processingParts in '{data_type_name}'. Expected a dictionary."
            logger.error(message)
            raise ValueError(message)

        logger.debug(
            "DataTypeDefinition loaded for '{}': processor={}, suffixes={}, output_columns={}, processing_parts={}",
            data_type_name,
            p_str,
            suffixes,
            output_columns,
            parts.to_dict(),
        )
        return cls(
            processor=p_str,
            suffixes=suffixes,
            output_columns=output_columns,
            processing_parts=parts,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the DataTypeDefinition instance back to a dictionary.

        Returns:
            dict[str, Any]: The data type definition as a dictionary."""
        return {
            "processor": self.processor,
            "suffixes": self.suffixes,
            "output_columns": self.output_columns,
            "processingParts": self.processing_parts.to_dict(),
        }


class Config:
    """A unified configuration class that holds all data types configurations."""

    _instance: ClassVar["Config | None"] = None
    _lock: ClassVar[Lock] = Lock()

    DEFAULT_CONFIG_FILENAME = "tuflow_results_validation_and_datatypes.json"

    def __init__(self, data_types: dict[str, DataTypeDefinition]) -> None:
        if Config._instance is not None:
            raise RuntimeError("This class is a singleton!")
        self.data_types: dict[str, DataTypeDefinition] = data_types

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Config":
        """Load the Config either from a provided config_path or from the default path.

        This method fails fast: unreadable configs or invalid data type entries raise
        ValueError/FileNotFoundError rather than being skipped."""
        if config_path is None:
            config_dir: Path = Path(__file__).parent
            config_path = config_dir / cls.DEFAULT_CONFIG_FILENAME
            logger.debug("No config_path provided. Using default path: {}", config_path)
        else:
            logger.debug("Loading Config from provided path: {}", config_path)

        loader = ConfigLoader(config_path=config_path)
        raw_data_types: dict[str, Any] = loader.get_data_types()
        data_types: dict[str, DataTypeDefinition] = {}
        for key, value in raw_data_types.items():
            if not isinstance(value, dict):
                message: str = f"Invalid format for data type '{key}'. Expected a dictionary."
                logger.error(message)
                raise ValueError(message)
            typed_value: dict[str, Any] = cast(dict[str, Any], value)
            data_type_def: DataTypeDefinition = DataTypeDefinition.from_dict(data=typed_value, data_type_name=key)
            data_types[key] = data_type_def
        logger.debug("Config loaded with data types: {}", list(data_types.keys()))
        return cls(data_types=data_types)

    @classmethod
    def get_instance(cls, config_path: Path | None = None) -> "Config":
        """Retrieve the singleton instance of Config."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls.load(config_path=config_path)
        return cls._instance


class SuffixesConfig:
    """A lookup dictionary that maps file suffixes to their respective data types."""

    _instance: ClassVar["SuffixesConfig | None"] = None
    _lock: ClassVar[Lock] = Lock()

    DEFAULT_CONFIG_FILENAME = "tuflow_results_validation_and_datatypes.json"

    def __init__(self, suffix_to_type: dict[str, str], config: Config) -> None:
        logger.debug("Initializing SuffixesConfig instance.")
        self.suffix_to_type: dict[str, str] = suffix_to_type
        self.config: Config = config  # Store the Config instance

    @classmethod
    def load(cls, config: Config | None = None) -> "SuffixesConfig":
        """Load the SuffixesConfig either from a provided Config object or from the default Config."""
        if config is None:
            config = Config.get_instance()
        suffix_to_type: dict[str, str] = {}
        for data_type, data_def in config.data_types.items():
            for suffix in data_def.suffixes:
                suffix_to_type[suffix] = data_type
                logger.debug("Mapping suffix '{}' to data type '{}'", suffix, data_type)
        logger.debug("SuffixesConfig loaded with {} suffixes.", len(suffix_to_type))
        return cls(suffix_to_type=suffix_to_type, config=config)

    @classmethod
    def get_instance(cls, config: Config | None = None) -> "SuffixesConfig":
        """Retrieve the singleton instance of SuffixesConfig."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls.load(config=config)
        return cls._instance

    def get_data_type_for_suffix(self, file_name: str) -> str | None:
        """Retrieve the data type based on the file's suffix."""
        for suffix, data_type in self.suffix_to_type.items():
            if file_name.endswith(suffix):
                logger.debug(
                    "File '{}' matches suffix '{}' with data type '{}'",
                    file_name,
                    suffix,
                    data_type,
                )
                return data_type
        logger.debug("File '{}' suffix did not match a data_type, returning None", file_name)
        return None

    def get_processor_class_for_data_type(self, data_type: str) -> str | None:
        """Retrieve the processor class name associated with a specific data type."""
        data_type_def: DataTypeDefinition | None = self.config.data_types.get(data_type)
        if not data_type_def:
            logger.error(f"Data type '{data_type}' not found in configuration.")
            return None
        return data_type_def.processor

    def get_definition_for_data_type(self, data_type: str) -> DataTypeDefinition | None:
        """Return the configuration block for ``data_type`` if it exists."""

        definition: DataTypeDefinition | None = self.config.data_types.get(data_type)
        if definition is None:
            logger.error(f"Data type '{data_type}' not found in configuration.")
        return definition

    def invert_suffix_to_type(self) -> dict[str, list[str]]:
        """Invert the suffix_to_type dictionary to map data types to suffixes."""
        inverted: dict[str, list[str]] = {}
        for suffix, data_type in self.suffix_to_type.items():
            inverted.setdefault(data_type, []).append(suffix)
        logger.debug("Inverted dictionary created with {} data types.", len(inverted))
        return inverted


if __name__ == "__main__":
    data_types_config: Config = Config.get_instance()
    suffixes_config: SuffixesConfig = SuffixesConfig.get_instance()
    import pprint

    pp = pprint.PrettyPrinter(indent=4)

    print("=== DataTypesConfig ===")
    # Display all data types and their configurations
    data_types_display: dict[str, Any] = {
        key: {
            "processor": dt.processor,
            "suffixes": dt.suffixes,
            "output_columns": dt.output_columns,
            "processingParts": dt.processing_parts.to_dict(),  # Access via method
        }
        for key, dt in data_types_config.data_types.items()
    }
    pp.pprint(data_types_display)

    print("\n=== SuffixesConfig ===")
    pp.pprint(suffixes_config.__dict__)
    print()
    pp.pprint(suffixes_config.invert_suffix_to_type())

    print("\n=== Example Access ===")
    # Access suffixes for 'Cmx'
    cmx_suffixes: list[str] = data_types_config.data_types["Cmx"].suffixes
    print("Cmx Suffixes:", cmx_suffixes)

    # Access dataformat for 'CCA'
    if "CCA" in data_types_config.data_types:
        cca_dataformat: str = data_types_config.data_types["CCA"].processing_parts.dataformat
        print("CCA Dataformat:", cca_dataformat)
    else:
        print("CCA Dataformat: Not defined")

    # Access expected_in_header for 'Q'
    if "Q" in data_types_config.data_types:
        q_expected_in_header: list[str] = data_types_config.data_types["Q"].processing_parts.expected_in_header
        print("Q Expected In Header:", q_expected_in_header)
    else:
        print("Q Expected In Header: Not defined")

    # Access columns_to_use for 'Nmx'
    if "Nmx" in data_types_config.data_types:
        nmx_columns_to_use: dict[str, str] = data_types_config.data_types["Nmx"].processing_parts.columns_to_use
        print("Nmx Columns to Use:", nmx_columns_to_use)
    else:
        print("Nmx Columns to Use: Not defined")

    # Example of using suffixes_config
    test_suffix = "_1d_Cmx.csv"
    data_type: str | None = suffixes_config.get_data_type_for_suffix(file_name=test_suffix)
    print(f"Data type for suffix '{test_suffix}':", data_type)
