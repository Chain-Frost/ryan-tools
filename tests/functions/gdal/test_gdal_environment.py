"""Tests for ryan_library.functions.gdal.gdal_environment."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import os
from ryan_library.functions.gdal import gdal_environment

class TestDiscovery:
    @patch("ryan_library.functions.gdal.gdal_environment.Path")
    def test_find_qgis_install_path_osgeo(self, mock_path):
        """Test finding OSGeo4W installation."""
        # Setup mock for OSGeo4W path
        mock_osgeo = MagicMock()
        mock_osgeo.exists.return_value = True
        mock_osgeo.parent.parent = Path("C:/OSGeo4W")
        
        # When Path("C:/OSGeo4W/bin/o4w_env.bat") is called, return mock_osgeo
        def side_effect(arg):
            if str(arg) == "C:/OSGeo4W/bin/o4w_env.bat":
                return mock_osgeo
            return MagicMock()
        
        mock_path.side_effect = side_effect
        
        res = gdal_environment.find_qgis_install_path()
        assert res == Path("C:/OSGeo4W")

    @patch("ryan_library.functions.gdal.gdal_environment.Path")
    def test_find_qgis_install_path_qgis(self, mock_path):
        """Test finding QGIS installation in Program Files."""
        # Setup mock for OSGeo4W path (not found)
        mock_osgeo = MagicMock()
        mock_osgeo.exists.return_value = False
        
        # Setup mock for Program Files
        mock_prog_files = MagicMock()
        mock_qgis_dir = MagicMock()
        mock_qgis_dir.name = "QGIS 3.22"
        mock_qgis_dir.__truediv__.return_value.exists.return_value = True # bin/o4w_env.bat exists
        
        mock_prog_files.glob.return_value = [mock_qgis_dir]
        
        def side_effect(arg):
            if str(arg) == "C:/OSGeo4W/bin/o4w_env.bat":
                return mock_osgeo
            if str(arg) == "C:/Program Files":
                return mock_prog_files
            return MagicMock()
            
        mock_path.side_effect = side_effect
        
        res = gdal_environment.find_qgis_install_path()
        assert res == mock_qgis_dir

    def test_find_python_installation(self):
        """Test finding Python installation."""
        mock_qgis = MagicMock()
        mock_py = MagicMock()
        mock_qgis.glob.return_value = [mock_py]
        
        res = gdal_environment.find_python_installation(mock_qgis)
        assert res == mock_py

    def test_find_python_installation_not_found(self):
        """Test error when Python not found."""
        mock_qgis = MagicMock()
        mock_qgis.glob.return_value = []
        
        with pytest.raises(FileNotFoundError):
            gdal_environment.find_python_installation(mock_qgis)

class TestSetup:
    @patch("ryan_library.functions.gdal.gdal_environment.check_executable")
    @patch("ryan_library.functions.gdal.gdal_environment.find_python_installation")
    @patch("ryan_library.functions.gdal.gdal_environment.find_qgis_install_path")
    @patch.dict(os.environ, {"PATH": "C:/Windows/System32"}, clear=True)
    def test_setup_environment(self, mock_find_qgis, mock_find_py, mock_check):
        """Test environment variable setup."""
        mock_qgis = Path("C:/MockQGIS")
        mock_py = Path("C:/MockQGIS/apps/Python39")
        
        mock_find_qgis.return_value = mock_qgis
        mock_find_py.return_value = mock_py
        
        gdal_environment.setup_environment()
        
        assert os.environ["OSGEO4W_ROOT"] == str(mock_qgis)
        assert os.environ["PYTHONHOME"] == str(mock_py)
        assert "GDAL_CALC_PATH" in os.environ
        
        assert mock_check.call_count == 3

    @patch("ryan_library.functions.gdal.gdal_environment.Path")
    def test_check_executable(self, mock_path):
        """Test executable check."""
        mock_path.return_value.exists.return_value = True
        gdal_environment.check_executable("some/path", "tool")
        
        mock_path.return_value.exists.return_value = False
        with pytest.raises(FileNotFoundError):
            gdal_environment.check_executable("some/path", "tool")
