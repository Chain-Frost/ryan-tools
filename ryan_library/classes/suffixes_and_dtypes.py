# ryan_library/classes/suffixes_and_dtypes.py
import json
from pathlib import Path
import logging
from typing import Any

# Paths to the separate config files
SUFFIXES_PATH = Path(__file__).parent / "tuflow_results_suffixes.json"
VALIDATION_DATATYPES_PATH = (
    Path(__file__).parent / "tuflow_results_validation_and_datatypes.json"
)


def load_json_config(config_path: Path) -> dict[str, Any]:
    """
    Load a JSON configuration file.

    Args:
        config_path (Path): The path to the JSON file to load.

    Returns:
        dict[str, Any]: A dictionary of loaded JSON data.
    """
    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file)
            logging.debug(f"Loaded configuration from {config_path}: {config}")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {config_path}: {e}")
        return {}


def load_configs() -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Load suffixes and data type configurations from separate JSON files.

    Returns:
        tuple[dict[str, Any], dict[str, Any]]:
            A tuple containing:
            - A dict of suffixes (from tuflow_results_suffixes.json).
            - A dict of data types config (from tuflow_results_validation_and_datatypes.json).
    """
    suffixes_config = load_json_config(SUFFIXES_PATH)
    data_types_config = load_json_config(VALIDATION_DATATYPES_PATH)

    # Extract just the suffix mapping
    suffixes = suffixes_config.get("suffixes", {})
    # Extract the "data_types" mapping
    data_types = data_types_config.get("data_types", {})

    return suffixes, data_types


# Load configurations at module import
SUFFIXES, DATA_TYPES_CONFIG = load_configs()
