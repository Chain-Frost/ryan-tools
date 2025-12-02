"""Tests for ryan_library.functions.gdal.gdal_runners."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import os
from ryan_library.functions.gdal import gdal_runners

class TestRunners:
    @patch("subprocess.run")
    @patch.dict(os.environ, {"GDAL_CALC_PATH": "path/to/gdal_calc.py"})
    def test_run_gdal_calc(self, mock_run, tmp_path):
        """Test gdal_calc execution."""
        input_file = tmp_path / "input.tif"
        output_file = "output.tif"
        
        gdal_runners.run_gdal_calc(input_file, output_file, 0.5)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0] == "python"
        assert args[1] == "path/to/gdal_calc.py"
        assert '--calc="where(A>=0.5,1,0)"' in args
        assert str(input_file) in args
        assert output_file in args

    @patch("subprocess.run")
    @patch.dict(os.environ, {}, clear=True)
    def test_run_gdal_calc_missing_env(self, mock_run, tmp_path):
        """Test gdal_calc raises error if env var missing."""
        with pytest.raises(EnvironmentError):
            gdal_runners.run_gdal_calc(tmp_path / "in.tif", "out.tif", 0.5)

    @patch("subprocess.run")
    @patch.dict(os.environ, {"GDAL_POLYGONIZE_PATH": "path/to/gdal_polygonize.py"})
    def test_run_gdal_polygonize(self, mock_run):
        """Test gdal_polygonize execution."""
        gdal_runners.run_gdal_polygonize("input.tif", "output.shp")
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0] == "python"
        assert args[1] == "path/to/gdal_polygonize.py"
        assert args[2] == "input.tif"
        assert args[3] == "output.shp"

    @patch("subprocess.run")
    @patch.dict(os.environ, {}, clear=True)
    def test_run_gdal_polygonize_missing_env(self, mock_run):
        """Test gdal_polygonize raises error if env var missing."""
        with pytest.raises(EnvironmentError):
            gdal_runners.run_gdal_polygonize("in.tif", "out.shp")
