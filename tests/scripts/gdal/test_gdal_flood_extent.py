
"""Tests for ryan_library.scripts.gdal.gdal_flood_extent."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.gdal import gdal_flood_extent

@pytest.fixture
def mock_paths():
    return [Path("path/to/files")]

def test_main_processing_success(mock_paths):
    with patch("ryan_library.scripts.gdal.gdal_flood_extent.setup_logger"):
        with patch("ryan_library.scripts.gdal.gdal_flood_extent.setup_environment") as mock_setup:
            with patch("ryan_library.scripts.gdal.gdal_flood_extent.check_required_components") as mock_check:
                with patch("ryan_library.scripts.gdal.gdal_flood_extent.find_files_parallel") as mock_find:
                    with patch("ryan_library.scripts.gdal.gdal_flood_extent.Pool") as mock_pool:
                        mock_find.return_value = [Path("file1.tif")]
                        pool_instance = mock_pool.return_value.__enter__.return_value
                        
                        gdal_flood_extent.main_processing(mock_paths)
                        
                        mock_setup.assert_called_once()
                        mock_check.assert_called_once()
                        mock_find.assert_called_once()
                        pool_instance.map.assert_called_once()

def test_main_processing_no_files(mock_paths):
    with patch("ryan_library.scripts.gdal.gdal_flood_extent.setup_logger"):
        with patch("ryan_library.scripts.gdal.gdal_flood_extent.setup_environment"):
            with patch("ryan_library.scripts.gdal.gdal_flood_extent.check_required_components"):
                with patch("ryan_library.scripts.gdal.gdal_flood_extent.find_files_parallel") as mock_find:
                    mock_find.return_value = []
                    
                    gdal_flood_extent.main_processing(mock_paths)
                    
                    mock_find.assert_called_once()

def test_process_file():
    file_path = Path("test_d_HR_Max.tif")
    with patch("ryan_library.scripts.gdal.gdal_flood_extent.run_gdal_calc") as mock_calc:
        with patch("ryan_library.scripts.gdal.gdal_flood_extent.run_gdal_polygonize") as mock_poly:
            
            gdal_flood_extent.process_file(file_path)
            
            mock_calc.assert_called_once()
            mock_poly.assert_called_once()
            
            # Check args
            args, _ = mock_calc.call_args
            assert args[0] == file_path
            assert args[2] == 0.0 # cutoff

def test_format_cutoff_value():
    assert gdal_flood_extent.format_cutoff_value(0.0) == "0"
    assert gdal_flood_extent.format_cutoff_value(0.5) == "05"
    assert gdal_flood_extent.format_cutoff_value(1.2) == "12"
    assert gdal_flood_extent.format_cutoff_value(10.0) == "10"
