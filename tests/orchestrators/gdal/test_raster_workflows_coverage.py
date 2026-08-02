"""Tests for raster_workflows orchestration."""

import pytest
from unittest.mock import patch
from pathlib import Path
import time

from ryan_library.orchestrators.gdal.raster_workflows import convert_rasters, add_overviews, _run_batch


@pytest.fixture
def source_dir(tmp_path):
    d = tmp_path / "src"
    d.mkdir()
    (d / "a.flt").touch()
    (d / "b.asc").touch()
    return d


@patch("ryan_library.orchestrators.gdal.raster_workflows.translate_to_geotiff")
@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_convert_rasters_empty(mock_build, mock_trans, tmp_path):
    assert convert_rasters(tmp_path) == []
    mock_trans.assert_not_called()


@patch("ryan_library.orchestrators.gdal.raster_workflows.translate_to_geotiff")
@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_convert_rasters_serial(mock_build, mock_trans, source_dir):
    res = convert_rasters(source_dir, workers=1, output_suffix="_out")
    assert len(res) == 2
    assert mock_trans.call_count == 2
    assert mock_build.call_count == 2
    assert str(res[0]).endswith("_out.tif")


@patch("ryan_library.orchestrators.gdal.raster_workflows.translate_to_geotiff")
@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_convert_rasters_parallel_skip(mock_build, mock_trans, source_dir):
    # Create an output that is newer to trigger the skip logic
    out_a = source_dir / "a.tif"
    out_a.touch()
    time.sleep(0.01)
    out_a.touch()

    res = convert_rasters(source_dir, workers=2, overwrite=False)
    assert len(res) == 2
    # only one translation (for b.asc), a.flt is skipped
    assert mock_trans.call_count == 1
    # overviews still called but with refresh=needs_conversion (False for a, True for b)
    assert mock_build.call_count == 2


@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_add_overviews_empty(mock_build, tmp_path):
    assert add_overviews(tmp_path) == []


@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_add_overviews_serial(mock_build, tmp_path):
    (tmp_path / "c.tif").touch()
    res = add_overviews(tmp_path, workers=1)
    assert len(res) == 1
    assert mock_build.call_count == 1


@patch("ryan_library.orchestrators.gdal.raster_workflows.build_external_overviews")
def test_add_overviews_parallel(mock_build, tmp_path):
    (tmp_path / "c.tif").touch()
    (tmp_path / "d.tif").touch()
    res = add_overviews(tmp_path, workers=2)
    assert len(res) == 2
    assert mock_build.call_count == 2


def test_run_batch_exception():
    def failing_op(p):
        raise ValueError("Simulated GDAL failure")

    with pytest.raises(ValueError, match="Simulated GDAL failure"):
        _run_batch([Path("x.tif")], 2, failing_op)
