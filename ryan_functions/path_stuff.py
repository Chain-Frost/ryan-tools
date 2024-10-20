import os
from pathlib import Path

# Define the mapping of network paths to drive letters
network_drive_mapping = {r"\\bgersgtnas05.bge-resources.com\waterways": "Q:"}


def is_relative_to_current_directory(user_path):
    # Convert user path to a Path object

    user_path = Path(user_path)
    print(user_path)
    # Get the current working directory
    current_directory = Path.cwd()
    print(current_directory)

    try:
        # Resolve the user path and check if it's within the current directory
        user_path.relative_to(current_directory)
        return True
    except ValueError:
        return False


def convert_network_path_to_drive_letter(user_path):
    for network_path, drive_letter in network_drive_mapping.items():
        if str(user_path).startswith(drive_letter):
            return Path(str(user_path).replace(drive_letter, network_path))
    return user_path


def convert_to_relative_path(user_path):
    # Convert user path to a Path object
    user_path = Path(user_path)

    # Convert network path to drive letter if applicable
    user_path = convert_network_path_to_drive_letter(user_path)

    # Get the current working directory
    current_directory = Path.cwd()

    if is_relative_to_current_directory(user_path):
        # Return the relative path from the current directory
        rel_path = user_path.relative_to(current_directory)
        print(f"Converting to relative path: {rel_path}")
        return rel_path
    else:
        # Return the absolute path if not within the current directory
        return user_path.resolve()
