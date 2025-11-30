
"""Tests for ryan_library.scripts.tuflow.tuflow_results_styling."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from ryan_library.scripts.tuflow import tuflow_results_styling

@pytest.fixture
def styler():
    return tuflow_results_styling.TUFLOWResultsStyler()

def test_get_file_mappings(styler):
    mappings = styler.get_file_mappings()
    assert "d_Max" in mappings
    assert "d_HR_Max" in mappings
    assert mappings["d_Max"]["qml"].name == "depth_for_legend_max2m.qml"

def test_get_qml_content_default(styler, tmp_path):
    # Mock default styles path
    styler.default_styles_path = tmp_path
    (tmp_path / "test.qml").write_text("content")
    
    content = styler.get_qml_content(Path("test.qml"))
    assert content == "content"

def test_get_qml_content_user(styler, tmp_path):
    user_qml = tmp_path / "user.qml"
    user_qml.write_text("user content")
    
    content = styler.get_qml_content(user_qml)
    assert content == "user content"

def test_process_data(styler, tmp_path):
    # Setup
    current_path = tmp_path / "data"
    current_path.mkdir()
    (current_path / "result_d_Max.tif").touch()
    
    qml_path = Path("test.qml")
    
    with patch.object(styler, "get_qml_content", return_value="qml content"):
        styler.process_data("result_d_Max.tif", "qml", current_path, qml_path)
        
        # Check if .qml file was created
        expected_qml = current_path / "result_d_Max.qml"
        assert expected_qml.exists()
        assert expected_qml.read_text(encoding="utf-8") == "qml content"

def test_tree_process(styler, tmp_path):
    # Setup directory structure
    (tmp_path / "file_d_Max.tif").touch()
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "file_h_Max.flt").touch()
    
    # Mock process_data to verify calls
    with patch.object(styler, "process_data") as mock_process:
        styler.tree_process(tmp_path)
        
        # Should be called for d_Max.tif and h_Max.flt
        assert mock_process.call_count == 2
        
        # Check call args
        calls = mock_process.call_args_list
        filenames = [c[0][0] for c in calls]
        assert "file_d_Max.tif" in filenames
        assert "file_h_Max.flt" in filenames

def test_process_gpkg(styler, tmp_path):
    gpkg_path = tmp_path / "test.gpkg"
    gpkg_path.touch()
    
    with patch("sqlite3.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("style1", "xml")]
        
        styler.process_gpkg("test.gpkg", "layer", tmp_path, Path("style.qml"))
        
        mock_connect.assert_called_with(gpkg_path)
        mock_cursor.execute.assert_called()

def test_apply_styles(styler):
    with patch.object(styler, "tree_process") as mock_tree:
        styler.apply_styles()
        mock_tree.assert_called_once()
