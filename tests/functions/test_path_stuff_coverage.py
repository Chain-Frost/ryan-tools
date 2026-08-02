"""Tests for path_stuff.py."""

import pytest
from pathlib import Path
from unittest.mock import patch

from ryan_library.functions.path_stuff import (
    is_relative_to_current_directory,
    convert_network_path_to_drive_letter,
    convert_to_relative_path,
    network_drive_mapping,
)


def test_is_relative_to_current_directory():
    cwd = Path.cwd()
    assert is_relative_to_current_directory(cwd / "subfolder" / "file.txt") is True
    # On Windows, a completely different drive isn't relative
    # If cwd is E:, C: is not relative
    assert is_relative_to_current_directory(Path("C:/different_drive")) is False


def test_convert_network_path_to_drive_letter():
    network_path = r"\\bgersgtnas05.bge-resources.com\waterways\Project\data.csv"
    expected = Path(r"Q:\Project\data.csv")
    assert convert_network_path_to_drive_letter(Path(network_path)) == expected

    # If it doesn't match, return original
    assert convert_network_path_to_drive_letter(Path("C:/data.csv")) == Path("C:/data.csv")


def test_convert_to_relative_path_success():
    cwd = Path.cwd()
    sub_path = cwd / "test" / "file.txt"
    res = convert_to_relative_path(sub_path)
    assert res == Path("test/file.txt")


def test_convert_to_relative_path_absolute_fallback():
    # Different drive or root so it cannot be relative
    diff_path = Path("C:/non_existent_folder/file.txt")
    res = convert_to_relative_path(diff_path)
    assert res == diff_path.resolve()


@patch("ryan_library.functions.path_stuff.is_relative_to_current_directory")
def test_convert_to_relative_path_value_error_fallback(mock_is_relative):
    # Force is_relative_to_current_directory to return True, but then relative_to raises ValueError
    mock_is_relative.return_value = True
    cwd = Path.cwd()
    diff_path = Path("C:/non_existent_folder/file.txt")
    # This will raise ValueError when relative_to is called internally
    res = convert_to_relative_path(diff_path)
    assert res == diff_path
