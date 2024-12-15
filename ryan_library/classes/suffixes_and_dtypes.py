# ryan_library/classes/suffixes_and_dtypes.py

import json
from pathlib import Path
from loguru import logger
from dataclasses import dataclass, field
from typing import Any


class ConfigLoader:
    """
    A centralized configuration loader that loads a single JSON configuration file
    and provides access to its components.
    """

    def __init__(self, config_path: Path) -> None:
        self.config_path: Path = config_path
        self.config_data: dict[str, Any] = self.load_json_config()

    def load_json_config(self) -> dict[str, Any]:
        """
        Load the central JSON configuration file.

        Returns:
            dict[str, Any]: The loaded JSON data.
        """
        try:
            with self.config_path.open("r", encoding="utf-8") as file:
                config = json.load(file)
                logger.debug(f"Loaded configuration from {self.config_path}: {config}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {self.config_path}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {self.config_path}: {e}")
            return {}

    def get_data_types(self) -> dict[str, Any]:
        """
        Extract the data types section from the configuration.

        Returns:
            dict[str, Any]: The data types configuration.
        """
        data_types = self.config_data
        if not isinstance(data_types, dict):
            logger.error(
                "Invalid format for data types. Expected a dictionary at the top level."
            )
            return {}
        logger.debug(f"Data types loaded: {list(data_types.keys())}")
        return data_types


@dataclass
class ProcessingParts:
    """
    Encapsulates all processing-related configurations for a data type.
    """

    dataformat: str = ""
    skip_columns: list[int] = field(default_factory=list)
    columns_to_use: dict[str, str] = field(default_factory=dict)
    expected_in_header: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any], data_type_name: str) -> "ProcessingParts":
        dataformat = data.get("dataformat", "")
        if not isinstance(dataformat, str):
            logger.error(
                f"Invalid format for dataformat in '{data_type_name}'. Expected a string."
            )
            dataformat = ""

        skip_columns = data.get("skip_columns", [])
        if not isinstance(skip_columns, list) or not all(
            isinstance(i, int) for i in skip_columns
        ):
            logger.error(
                f"Invalid format for skip_columns in '{data_type_name}'. Expected a list of integers."
            )
            skip_columns = []

        columns_to_use = data.get("columns_to_use", {})
        if not isinstance(columns_to_use, dict):
            logger.error(
                f"Invalid format for columns_to_use in '{data_type_name}'. Expected a dictionary."
            )
            columns_to_use = {}
        else:
            # Ensure all values are strings
            if not all(isinstance(v, str) for v in columns_to_use.values()):
                logger.error(
                    f"All values in columns_to_use in '{data_type_name}' must be strings."
                )
                columns_to_use = {}

        expected_in_header = data.get("expected_in_header", [])
        if not isinstance(expected_in_header, list) or not all(
            isinstance(item, str) for item in expected_in_header
        ):
            logger.error(
                f"Invalid format for expected_in_header in '{data_type_name}'. Expected a list of strings."
            )
            expected_in_header = []

        logger.debug(
            f"ProcessingParts loaded for '{data_type_name}': "
            f"dataformat={dataformat}, skip_columns={skip_columns}, "
            f"columns_to_use={columns_to_use}, expected_in_header={expected_in_header}"
        )
        return cls(
            dataformat=dataformat,
            skip_columns=skip_columns,
            columns_to_use=columns_to_use,
            expected_in_header=expected_in_header,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the ProcessingParts instance back to a dictionary.

        Returns:
            dict[str, Any]: The processing parts as a dictionary.
        """
        return {
            "dataformat": self.dataformat,
            "skip_columns": self.skip_columns,
            "columns_to_use": self.columns_to_use,
            "expected_in_header": self.expected_in_header,
        }


@dataclass
class DataTypeDefinition:
    processor: str
    suffixes: list[str]
    output_columns: dict[str, str]
    processing_parts: ProcessingParts

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], data_type_name: str
    ) -> "DataTypeDefinition":
        processor: str | None = data.get("processor")
        if not isinstance(processor, str):
            logger.error(
                f"Invalid or missing 'processor' in '{data_type_name}'. Expected a string."
            )
            processor = ""

        suffixes = data.get("suffixes", [])
        if not isinstance(suffixes, list):
            logger.error(
                f"Invalid format for suffixes in '{data_type_name}'. Expected a list."
            )
            suffixes = []

        output_columns = data.get("output_columns", {})
        if not isinstance(output_columns, dict):
            logger.error(
                f"Invalid format for output_columns in '{data_type_name}'. Expected a dictionary."
            )
            output_columns = {}

        processing_parts_data = data.get("processingParts", {})
        if not isinstance(processing_parts_data, dict):
            logger.error(
                f"Invalid format for processingParts in '{data_type_name}'. Expected a dictionary."
            )
            processing_parts_data = {}

        processing_parts = ProcessingParts.from_dict(
            processing_parts_data, data_type_name
        )

        logger.debug(
            f"DataTypeDefinition loaded for '{data_type_name}': processor={processor}, suffixes={suffixes}, "
            f"output_columns={output_columns}, processing_parts={processing_parts.to_dict()}"
        )
        return cls(
            processor=processor,
            suffixes=suffixes,
            output_columns=output_columns,
            processing_parts=processing_parts,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the DataTypeDefinition instance back to a dictionary.

        Returns:
            dict[str, Any]: The data type definition as a dictionary.
        """
        return {
            "processor": self.processor,
            "suffixes": self.suffixes,
            "output_columns": self.output_columns,
            "processingParts": self.processing_parts.to_dict(),
        }


class Config:
    """
    A unified configuration class that holds all data types configurations.
    """

    def __init__(self, data_types: dict[str, DataTypeDefinition]) -> None:
        self.data_types = data_types

    @classmethod
    def load(cls, config_path: Path) -> "Config":
        loader = ConfigLoader(config_path)
        raw_data_types = loader.get_data_types()
        data_types = {}
        for key, value in raw_data_types.items():
            if not isinstance(value, dict):
                logger.error(
                    f"Invalid format for data type '{key}'. Expected a dictionary."
                )
                continue
            data_type_def = DataTypeDefinition.from_dict(value, key)
            data_types[key] = data_type_def
        logger.debug(f"Config loaded with data types: {list(data_types.keys())}")
        return cls(data_types=data_types)


class SuffixesConfig:
    """
    A lookup dictionary that maps file suffixes to their respective data types.
    """

    def __init__(self, suffix_to_type: dict[str, str]) -> None:
        self.suffix_to_type: dict[str, str] = suffix_to_type

    @classmethod
    def load(cls, config: Config) -> "SuffixesConfig":
        suffix_to_type = {}
        for data_type, data_def in config.data_types.items():
            for suffix in data_def.suffixes:
                # Map each suffix to its data type
                suffix_to_type[suffix] = data_type
                logger.debug(f"Mapping suffix '{suffix}' to data type '{data_type}'")
        logger.debug(f"SuffixesConfig loaded with {len(suffix_to_type)} suffixes.")
        return cls(suffix_to_type=suffix_to_type)

    def get_data_type_for_suffix(self, file_name: str) -> str | None:
        """
        Retrieve the data type based on the file's suffix.

        Args:
            file_name (str): Name of the file.

        Returns:
            str | None: The corresponding data type if found, else None.
        """
        for suffix, data_type in self.suffix_to_type.items():
            if file_name.endswith(suffix):
                logger.debug(
                    f"File '{file_name}' matches suffix '{suffix}' with data type '{data_type}'"
                )
                return data_type
        logger.debug(
            f"File '{file_name}' suffix did not match a data_type, returning None"
        )
        return None

    def get_processor_class_for_data_type(self, data_type: str) -> str | None:
        """
        Retrieve the processor class name associated with a specific data type.

        Args:
            data_type (str): The data type.

        Returns:
            str | None: The processor class name or None if not found.
        """
        data_type_def = data_types_config.data_types.get(data_type)
        if not data_type_def:
            logger.error(f"Data type '{data_type}' not found in configuration.")
            return None
        return data_type_def.processor

    def invert_suffix_to_type(self) -> dict[str, list[str]]:
        """
        Invert the suffix_to_type dictionary to map data types to suffixes.

        Returns:
            dict[str, list[str]]: A dictionary mapping data types to lists of suffixes.
        """
        inverted = {}
        for suffix, data_type in self.suffix_to_type.items():
            inverted.setdefault(data_type, []).append(suffix)
        logger.debug(f"Inverted dictionary created with {len(inverted)} data types.")
        return inverted


# Module-level configuration instances
config_dir: Path = Path(__file__).parent
config_path: Path = config_dir / "tuflow_results_validation_and_datatypes.json"
# Ensure this path is correct
data_types_config: Config = Config.load(config_path=config_path)
suffixes_config: SuffixesConfig = SuffixesConfig.load(config=data_types_config)


if __name__ == "__main__":
    import pprint

    pp = pprint.PrettyPrinter(indent=4)

    print("=== DataTypesConfig ===")
    # Display all data types and their configurations
    data_types_display = {
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

    print("\n=== Example Access ===")
    # Access suffixes for 'Cmx'
    cmx_suffixes = data_types_config.data_types["Cmx"].suffixes
    print("Cmx Suffixes:", cmx_suffixes)

    # Access dataformat for 'CCA'
    if "CCA" in data_types_config.data_types:
        cca_dataformat = data_types_config.data_types["CCA"].processing_parts.dataformat
        print("CCA Dataformat:", cca_dataformat)
    else:
        print("CCA Dataformat: Not defined")

    # Access expected_in_header for 'Q'
    if "Q" in data_types_config.data_types:
        q_expected_in_header = data_types_config.data_types[
            "Q"
        ].processing_parts.expected_in_header
        print("Q Expected In Header:", q_expected_in_header)
    else:
        print("Q Expected In Header: Not defined")

    # Access columns_to_use for 'Nmx'
    if "Nmx" in data_types_config.data_types:
        nmx_columns_to_use = data_types_config.data_types[
            "Nmx"
        ].processing_parts.columns_to_use
        print("Nmx Columns to Use:", nmx_columns_to_use)
    else:
        print("Nmx Columns to Use: Not defined")

    # Example of using suffixes_config
    test_suffix = "_1d_Cmx.csv"
    data_type = suffixes_config.get_data_type_for_suffix(test_suffix)
    print(f"Data type for suffix '{test_suffix}':", data_type)

    print(data_types_config)
