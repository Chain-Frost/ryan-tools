"""Utility functions shared by wrapper scripts."""

from pathlib import Path
import os
from importlib.metadata import PackageNotFoundError, version


def change_working_directory(target_dir: Path) -> bool:
    """Change the working directory and handle failures."""
    try:
        os.chdir(target_dir)
        print(f"Current Working Directory: {Path.cwd()}")
    except OSError as exc:
        print(f"Failed to change working directory to {target_dir}: {exc}")
        os.system("PAUSE")
        return False
    return True


def print_library_version(package_name: str = "ryan_functions") -> None:
    """Display the installed version of *package_name* if available."""
    try:
        print(f"{package_name} version: {version(package_name)}")
    except PackageNotFoundError:
        print(f"{package_name} version: unknown")
