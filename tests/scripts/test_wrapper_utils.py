
"""Tests for ryan_library.scripts.wrapper_utils."""

import pytest
import os
from argparse import ArgumentParser, Namespace
from pathlib import Path
from unittest.mock import patch, MagicMock
from ryan_library.scripts import wrapper_utils

def test_change_working_directory_success(tmp_path):
    assert wrapper_utils.change_working_directory(tmp_path) is True
    assert Path.cwd() == tmp_path

def test_change_working_directory_failure():
    with patch("os.chdir", side_effect=OSError("Error")):
        with patch("os.system") as mock_system:
            assert wrapper_utils.change_working_directory(Path("invalid")) is False
            mock_system.assert_called_with("PAUSE")

def test_print_library_version_found(capsys):
    with patch("ryan_library.scripts.wrapper_utils.version", return_value="1.0.0"):
        wrapper_utils.print_library_version("pkg")
        captured = capsys.readouterr()
        assert "pkg version: 1.0.0" in captured.out

def test_print_library_version_not_found(capsys):
    with patch("ryan_library.scripts.wrapper_utils.version", side_effect=ImportError):
        # wrapper_utils catches PackageNotFoundError which inherits from ImportError in recent python?
        # Actually PackageNotFoundError is from importlib.metadata
        from importlib.metadata import PackageNotFoundError
        with patch("ryan_library.scripts.wrapper_utils.version", side_effect=PackageNotFoundError):
            wrapper_utils.print_library_version("pkg")
            captured = capsys.readouterr()
            assert "pkg version: unknown" in captured.out

def test_add_common_cli_arguments():
    parser = ArgumentParser()
    wrapper_utils.add_common_cli_arguments(parser)
    
    # Check if arguments were added
    args = parser.parse_args(["--console-log-level", "DEBUG", "--locations", "L1", "L2", "--working-directory", "."])
    assert args.console_log_level == "DEBUG"
    assert args.locations == ["L1", "L2"]
    assert args.working_directory == Path(".")

def test_parse_common_cli_arguments():
    args = Namespace(
        console_log_level="INFO",
        locations=[" L1 ", "L2"],
        working_directory=Path(".")
    )
    options = wrapper_utils.parse_common_cli_arguments(args)
    
    assert options.console_log_level == "INFO"
    assert options.locations_to_include == ("L1", "L2")
    assert options.working_directory == Path(".")

def test_coerce_locations_argument():
    assert wrapper_utils._coerce_locations_argument(None) is None
    assert wrapper_utils._coerce_locations_argument([]) is None
    assert wrapper_utils._coerce_locations_argument([""]) is None
    assert wrapper_utils._coerce_locations_argument([" a ", "b"]) == ("a", "b")
