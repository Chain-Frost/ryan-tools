# ryan_library/functions/path_stuff.py

from pathlib import Path
import logging

# Define the mapping of network paths to drive letters
network_drive_mapping: dict[str, str] = {
    r"\\bgersgtnas05.bge-resources.com\waterways": "Q:"
}


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
        user_path.relative_to(current_directory)
        return True
    except ValueError:
        return False


def convert_network_path_to_drive_letter(user_path: Path) -> Path:
    """
    Convert a network path to a drive letter if applicable.

    Args:
        user_path (Path): The path to convert.

    Returns:
        Path: The converted path.
    """
    for network_path, drive_letter in network_drive_mapping.items():
        if str(user_path).startswith(network_path):
            return Path(str(user_path).replace(network_path, drive_letter, 1))
    return user_path


def convert_to_relative_path(user_path: Path) -> Path:
    """
    Convert the user path to a relative path if possible, else return absolute path.

    Args:
        user_path (Path): The path to convert.

    Returns:
        Path: The relative or absolute path.
    """
    # Convert network path to drive letter if applicable
    user_path = convert_network_path_to_drive_letter(user_path)

    # Get the current working directory
    current_directory: Path = Path.cwd()

    if is_relative_to_current_directory(user_path):
        # Return the relative path from the current directory
        try:
            rel_path = user_path.relative_to(current_directory)
            logging.debug(f"Converting to relative path: {rel_path}")
            return rel_path
        except ValueError:
            logging.debug(f"Failed to convert to relative path: {user_path}")
            return user_path
    else:
        # Return the absolute path if not within the current directory
        abs_path = user_path.resolve()
        logging.debug(f"Returning absolute path: {abs_path}")
        return abs_path
