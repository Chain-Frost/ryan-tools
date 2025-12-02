"""Tests for error handling in file_utils.py."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from ryan_library.functions import file_utils

class TestFindFilesParallelErrors:
    @patch("ryan_library.functions.file_utils.logger")
    @patch("ryan_library.functions.file_utils.Path.cwd")
    def test_root_dir_not_found(self, mock_cwd, mock_logger):
        """Test handling of non-existent root directory."""
        mock_cwd.return_value = Path("/cwd")
        
        non_existent = Path("non_existent_folder_12345")
        
        file_utils.find_files_parallel([non_existent], "*.txt")
        
        # Verify logger.error was called with expected message
        assert mock_logger.error.called
        args, _ = mock_logger.error.call_args
        assert "Root directory does not exist" in args[0]

    @patch("ryan_library.functions.file_utils.logger")
    @patch("ryan_library.functions.file_utils.Path.iterdir")
    def test_iterdir_permission_error(self, mock_iterdir, mock_logger, tmp_path):
        """Test PermissionError during directory iteration."""
        root = tmp_path / "root"
        root.mkdir()
        
        mock_root = MagicMock(spec=Path)
        mock_root.expanduser.return_value = mock_root
        mock_root.absolute.return_value = mock_root
        mock_root.exists.return_value = True
        mock_root.iterdir.side_effect = PermissionError("Access denied")
        mock_root.__hash__.return_value = 1
        mock_root.__eq__.side_effect = lambda x: x is mock_root
        
        file_utils.find_files_parallel([mock_root], "*.txt")
        
        assert mock_logger.error.called
        # We might have multiple error calls, check if any match
        found = any("Permission denied accessing directory" in call.args[0] for call in mock_logger.error.call_args_list)
        assert found

    @patch("ryan_library.functions.file_utils.logger")
    def test_worker_unexpected_error(self, mock_logger, tmp_path):
        """Test unexpected error in worker."""
        root = tmp_path / "root"
        root.mkdir()
        
        mock_root = MagicMock(spec=Path)
        mock_root.expanduser.return_value = mock_root
        mock_root.absolute.return_value = mock_root
        mock_root.exists.return_value = True
        mock_root.iterdir.side_effect = Exception("Unexpected boom")
        mock_root.__hash__.return_value = 1
        
        file_utils.find_files_parallel([mock_root], "*.txt")
        
        found = any("Error accessing directory" in call.args[0] for call in mock_logger.error.call_args_list)
        assert found

    @patch("ryan_library.functions.file_utils.logger")
    def test_subdirectory_permission_error(self, mock_logger, tmp_path):
        """Test PermissionError when accessing subdirectory."""
        root = tmp_path / "root"
        root.mkdir()
        
        mock_sub = MagicMock(spec=Path)
        mock_sub.is_dir.return_value = True
        mock_sub.absolute.side_effect = PermissionError("Sub access denied")
        mock_sub.name = "sub"
        
        mock_root = MagicMock(spec=Path)
        mock_root.expanduser.return_value = mock_root
        mock_root.absolute.return_value = mock_root
        mock_root.exists.return_value = True
        mock_root.iterdir.return_value = iter([mock_sub])
        mock_root.__hash__.return_value = 1
        
        file_utils.find_files_parallel([mock_root], "*.txt")
        
        found = any("Permission denied accessing subdirectory" in call.args[0] for call in mock_logger.error.call_args_list)
        assert found

    @patch("ryan_library.functions.file_utils.logger")
    def test_file_permission_error(self, mock_logger, tmp_path):
        """Test PermissionError when accessing file."""
        root = tmp_path / "root"
        root.mkdir()
        
        mock_file = MagicMock(spec=Path)
        mock_file.is_dir.return_value = False
        mock_file.name = "test.txt"
        mock_file.absolute.side_effect = PermissionError("File access denied")
        
        mock_root = MagicMock(spec=Path)
        mock_root.expanduser.return_value = mock_root
        mock_root.absolute.return_value = mock_root
        mock_root.exists.return_value = True
        mock_root.iterdir.return_value = iter([mock_file])
        mock_root.__hash__.return_value = 1
        
        file_utils.find_files_parallel([mock_root], "*.txt")
        
        found = any("Permission denied accessing file" in call.args[0] for call in mock_logger.error.call_args_list)
        assert found

    @patch("ryan_library.functions.file_utils.logger")
    def test_file_not_found_error(self, mock_logger, tmp_path):
        """Test FileNotFoundError when accessing file (e.g. race condition)."""
        root = tmp_path / "root"
        root.mkdir()
        
        mock_file = MagicMock(spec=Path)
        mock_file.is_dir.return_value = False
        mock_file.name = "test.txt"
        mock_file.absolute.return_value = mock_file
        mock_file.exists.return_value = False
        
        mock_root = MagicMock(spec=Path)
        mock_root.expanduser.return_value = mock_root
        mock_root.absolute.return_value = mock_root
        mock_root.exists.return_value = True
        mock_root.iterdir.return_value = iter([mock_file])
        mock_root.__hash__.return_value = 1
        
        file_utils.find_files_parallel([mock_root], "*.txt")
        
        found = any("File does not exist" in call.args[0] or "might have been moved" in call.args[0] for call in mock_logger.warning.call_args_list)
        assert found
