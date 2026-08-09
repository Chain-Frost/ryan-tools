# ryan_library/functions/path_stuff.py

import json
from pathlib import Path
from typing import Any
from loguru import logger


def _load_network_mappings() -> dict[str, str]:
    config_path: Path = Path(__file__).resolve().parents[1] / "classes" / "path_mappings.json"
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                mapping: dict[str, str] | None = json.load(f)
                if isinstance(mapping, dict):
                    return mapping
        except Exception as e:
            logger.warning(f"Failed to load path_mappings.json: {e}")
    return {}


network_drive_mapping: dict[str, str] = _load_network_mappings()


def is_relative_to_current_directory(user_path: Path) -> bool:
    """
    Check if the user_path is relative to the current working directory.

    Args:
        user_path (Path): The path to check.

    Returns:
        bool: True if user_path is within the current directory, False otherwise.
    """
    # Get the current working directory
    current_directory: Path = Path.cwd()

    try:
        # Resolve the user path and check if it's within the current directory
        user_path.resolve().relative_to(current_directory.resolve())
        return True
    except ValueError:
        return False


def convert_network_path_to_drive_letter(user_path: Path, mapping: dict[str, str] | None = None) -> Path:
    """
    Convert a network path to a drive letter if applicable.

    Args:
        user_path (Path): The path to convert.
        mapping (dict[str, str] | None): Dictionary mapping UNC paths to drive letters.

    Returns:
        Path: The converted path.
    """
    active_mapping = mapping if mapping is not None else network_drive_mapping
    for network_path, drive_letter in active_mapping.items():
        if str(user_path).startswith(network_path):
            return Path(str(user_path).replace(network_path, drive_letter, 1))
    return user_path


def convert_to_relative_path(user_path: Path, network_mapping: dict[str, str] | None = None) -> Path:
    """
    Convert the user path to a relative path if possible, else return absolute path.

    Args:
        user_path (Path): The path to convert.
        network_mapping (dict[str, str] | None): Dictionary mapping UNC paths to drive letters.

    Returns:
        Path: The relative or absolute path.
    """
    # Convert network path to drive letter if applicable
    user_path = convert_network_path_to_drive_letter(user_path, mapping=network_mapping)

    # Get the current working directory
    current_directory: Path = Path.cwd()

    if is_relative_to_current_directory(user_path):
        # Return the relative path from the current directory
        try:
            rel_path = user_path.resolve().relative_to(current_directory.resolve())
            logger.debug("Converting to relative path: {}", rel_path)
            return rel_path
        except ValueError:
            logger.debug("Failed to convert to relative path: {}", user_path)
            return user_path
    else:
        # Return the absolute path if not within the current directory
        abs_path = user_path.resolve()
        logger.debug("Returning absolute path: {}", abs_path)
        return abs_path
