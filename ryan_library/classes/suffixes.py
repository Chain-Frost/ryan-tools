# ryan_library/classes/suffixes.py
import json
from pathlib import Path
import logging
from typing import Any

# Path to suffixes.json
SUFFIXES_PATH = Path(__file__).parent / "suffixes.json"


def load_suffixes() -> dict[str, Any]:
    """
    Load suffixes and datatype mappings from the suffixes.json file.

    Returns:
        dict[str, Any]: A dictionary containing suffixes and datatype mappings.
    """
    try:
        with SUFFIXES_PATH.open("r", encoding="utf-8") as file:
            suffixes_config = json.load(file)
            logging.debug(f"Loaded suffixes configuration: {suffixes_config}")
            return suffixes_config
    except FileNotFoundError:
        logging.error(f"Suffixes file not found at {SUFFIXES_PATH}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {SUFFIXES_PATH}: {e}")
        return {}


# Load configurations at module import
SUFFIX_CONFIG = load_suffixes()
SUFFIX_DATA_TYPES = SUFFIX_CONFIG.get("data_types", {})
SUFFIXES = SUFFIX_CONFIG.get("suffixes", {})
