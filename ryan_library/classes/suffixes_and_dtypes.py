# ryan_library/classes/suffixes_and_dtypes.py

import json
from pathlib import Path
from loguru import logger
from typing import Any, Optional


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


class DataTypeDefinition:
    def __init__(
        self,
        suffixes: list[str],
        output_columns: dict[str, str],
        dataformat: str,
        skip_columns: list[int],
        columns_to_use: dict[str, str],
        expected_in_header: list[str],
    ) -> None:
        self.suffixes: list[str] = suffixes
        self.output_columns: dict[str, str] = output_columns
        self.dataformat: str = dataformat
        self.skip_columns: list[int] = skip_columns
        self.columns_to_use: dict[str, str] = columns_to_use
        self.expected_in_header: list[str] = expected_in_header

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], data_type_name: str
    ) -> "DataTypeDefinition":
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

        processing_parts = data.get("processingParts", {})
        if not isinstance(processing_parts, dict):
            logger.error(
                f"Invalid format for processingParts in '{data_type_name}'. Expected a dictionary."
            )
            processing_parts = {}

        dataformat = processing_parts.get("dataformat", "")
        if not isinstance(dataformat, str):
            logger.error(
                f"Invalid format for dataformat in '{data_type_name}'. Expected a string."
            )
            dataformat = ""

        skip_columns = processing_parts.get("skip_columns", [])
        if skip_columns is not None:
            if not isinstance(skip_columns, list) or not all(
                isinstance(i, int) for i in skip_columns
            ):
                logger.error(
                    f"Invalid format for skip_columns in '{data_type_name}'. Expected a list of integers."
                )
                skip_columns = []
        else:
            skip_columns = []

        columns_to_use: dict[str, str] = processing_parts.get("columns_to_use", {})
        if columns_to_use is not None:
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

        expected_in_header = processing_parts.get("expected_in_header", [])
        if expected_in_header is not None:
            if not isinstance(expected_in_header, list) or not all(
                isinstance(item, str) for item in expected_in_header
            ):
                logger.error(
                    f"Invalid format for expected_in_header in '{data_type_name}'. Expected a list of strings."
                )
                expected_in_header = []

        logger.debug(
            f"DataTypeDefinition loaded for '{data_type_name}': suffixes={suffixes}, "
            f"output_columns={output_columns}, dataformat={dataformat}, "
            f"skip_columns={skip_columns}, columns_to_use={columns_to_use}, "
            f"expected_in_header={expected_in_header}"
        )
        return cls(
            suffixes=suffixes,
            output_columns=output_columns,
            dataformat=dataformat,
            skip_columns=skip_columns,
            columns_to_use=columns_to_use,
            expected_in_header=expected_in_header,
        )


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

    def get_data_type(self, file_suffix: str) -> Optional[str]:
        """
        Retrieve the data type based on the file suffix.

        Args:
            file_suffix (str): The file suffix (e.g., "_1d_Cmx.csv").

        Returns:
            Optional[str]: The corresponding data type if found, else None.
        """
        return self.suffix_to_type.get(file_suffix)


# Module-level configuration instances
config_dir: Path = Path(__file__).parent
config_path: Path = (
    config_dir / "tuflow_results_validation_and_datatypes.json"
)  # Ensure this path is correct
data_types_config: Config = Config.load(config_path=config_path)
suffixes_config: SuffixesConfig = SuffixesConfig.load(config=data_types_config)


if __name__ == "__main__":
    import pprint

    pp = pprint.PrettyPrinter(indent=4)

    print("=== DataTypesConfig ===")
    # Display all data types and their configurations
    data_types_display = {
        key: {
            "suffixes": dt.suffixes,
            "output_columns": dt.output_columns,
            "processingParts": {
                "dataformat": dt.dataformat,
                "skip_columns": dt.skip_columns,
                "columns_to_use": dt.columns_to_use,
                "expected_in_header": dt.expected_in_header,
            },
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
    cca_dataformat = data_types_config.data_types["CCA"].dataformat
    print("CCA Dataformat:", cca_dataformat)

    # Access expected_in_header for 'Q'
    q_expected_in_header = data_types_config.data_types["Q"].expected_in_header
    print("Q Expected In Header:", q_expected_in_header)

    # Access columns_to_use for 'Nmx'
    nmx_columns_to_use = data_types_config.data_types["Nmx"].columns_to_use
    print("Nmx Columns to Use:", nmx_columns_to_use)

    # Example of using suffixes_config
    test_suffix = "_1d_Cmx.csv"
    data_type = suffixes_config.get_data_type(test_suffix)
    print(f"Data type for suffix '{test_suffix}':", data_type)
