# ryan_library/classes/suffixes_and_dtypes.py

import json
from pathlib import Path
from loguru import logger
from typing import Any


class SuffixesConfig:
    def __init__(
        self,
    ):
        self.suffixes = {}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SuffixesConfig":
        instance = cls()
        suffixes = data.get("suffixes", {})
        if not isinstance(suffixes, dict):
            logger.error("Invalid format for suffixes. Expected a dictionary.")
            suffixes = {}
        # Ensure all values are strings
        if not all(isinstance(value, str) for value in suffixes.values()):
            logger.error("All suffix values must be strings.")
            suffixes = {}
        logger.debug(f"Suffixes loaded: {suffixes}")
        instance.suffixes = suffixes
        return instance

    @classmethod
    def load(
        cls,
    ) -> "SuffixesConfig":
        config_dir = Path(__file__).parent
        suffixes_path = config_dir / "tuflow_results_suffixes.json"
        suffixes_data = cls.load_json_config(suffixes_path)
        return cls.from_dict(suffixes_data)

    @staticmethod
    def load_json_config(config_path: Path) -> dict[str, Any]:
        """
        Load a JSON configuration file.

        Args:
            config_path (Path): The path to the JSON file to load.

        Returns:
            dict[str, Any]: The loaded JSON data as a dictionary.
        """
        try:
            with config_path.open("r", encoding="utf-8") as file:
                config = json.load(file)
                logger.debug(
                    f"Loaded suffixes configuration from {config_path}: {config}"
                )
                return config
        except FileNotFoundError:
            logger.error(f"Suffixes configuration file not found at {config_path}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {config_path}: {e}")
            return {}


class DataTypeDefinition:
    def __init__(self, expected_headers: list[str], columns: dict[str, str]):
        self.expected_headers = expected_headers
        self.columns = columns

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataTypeDefinition":
        expected_headers = data.get("expected_headers", [])
        if not isinstance(expected_headers, list):
            logger.error("Invalid format for expected_headers. Expected a list.")
            expected_headers = []
        columns = data.get("columns", {})
        if not isinstance(columns, dict):
            logger.error("Invalid format for columns. Expected a dictionary.")
            columns = {}
        # Ensure all column types are strings
        invalid_columns = [
            col for col, dtype in columns.items() if not isinstance(dtype, str)
        ]
        if invalid_columns:
            logger.error(
                f"Invalid types for columns: {invalid_columns}. All column types must be strings."
            )
            # Optionally, remove invalid columns
            columns = {
                col: dtype for col, dtype in columns.items() if isinstance(dtype, str)
            }
        logger.debug(
            f"DataTypeDefinition loaded: expected_headers={expected_headers}, columns={columns}"
        )
        return cls(expected_headers=expected_headers, columns=columns)


class DataTypesConfig:
    def __init__(
        self,
    ) -> None:
        self.data_types = {}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataTypesConfig":
        instance = cls()
        if not isinstance(data, dict):
            logger.error("Invalid format for data types config. Expected a dictionary.")
            return instance
        data_types = {}
        for key, value in data.items():
            if not isinstance(value, dict):
                logger.error(
                    f"Invalid format for data type '{key}'. Expected a dictionary."
                )
                continue
            data_type_def = DataTypeDefinition.from_dict(value)
            data_types[key] = data_type_def
        logger.debug(f"DataTypesConfig loaded: {list(data_types.keys())}")
        instance.data_types = data_types
        return instance

    @classmethod
    def load(cls) -> "DataTypesConfig":
        config_dir = Path(__file__).parent
        data_types_path = config_dir / "tuflow_results_validation_and_datatypes.json"
        data_types_data = cls.load_json_config(data_types_path)
        return cls.from_dict(data_types_data)

    @staticmethod
    def load_json_config(config_path: Path) -> dict[str, Any]:
        """
        Load a JSON configuration file.

        Args:
            config_path (Path): The path to the JSON file to load.

        Returns:
            dict[str, Any]: The loaded JSON data as a dictionary.
        """
        try:
            with config_path.open("r", encoding="utf-8") as file:
                config = json.load(file)
                logger.debug(
                    f"Loaded data types configuration from {config_path}: {config}"
                )
                return config
        except FileNotFoundError:
            logger.error(f"Data types configuration file not found at {config_path}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {config_path}: {e}")
            return {}


# Module-level instances
suffixes_config = SuffixesConfig.load()
data_types_config = DataTypesConfig.load()

if "__main__" == __name__:
    import pprint

    pp = pprint.PrettyPrinter(indent=4)

    print("=== SuffixesConfig ===")
    pp.pprint(suffixes_config.__dict__)

    print("\n=== DataTypesConfig ===")
    # To display nested DataTypeDefinition objects, we'll convert them to dictionaries
    data_types_display = {
        key: {"expected_headers": dt.expected_headers, "columns": dt.columns}
        for key, dt in data_types_config.data_types.items()
    }
    pp.pprint(data_types_display)
