"""Tests for raster_maintenance orchestration."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from concurrent.futures import Future

from ryan_library.orchestrators.gdal.raster_maintenance import set_nodata_in_directory, create_footprints_in_directory


@pytest.fixture
def dummy_dir(tmp_path):
    d = tmp_path / "rasters"
    d.mkdir()
    (d / "r1.tif").touch()
    (d / "r2.tif").touch()
    return d


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_empty(mock_set, tmp_path):
    assert set_nodata_in_directory(tmp_path) == []
    mock_set.assert_not_called()


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_serial(mock_set, dummy_dir):
    mock_set.side_effect = lambda x, *args, **kwargs: x
    res = set_nodata_in_directory(dummy_dir, workers=1)
    assert len(res) == 2
    assert mock_set.call_count == 2


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_parallel(mock_set, dummy_dir):
    mock_set.side_effect = lambda x, *args, **kwargs: x
    res = set_nodata_in_directory(dummy_dir, workers=2)
    assert len(res) == 2
    assert mock_set.call_count == 2


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_empty(mock_create, tmp_path):
    assert create_footprints_in_directory(tmp_path) == []
    mock_create.assert_not_called()


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_serial(mock_create, dummy_dir):
    mock_create.side_effect = lambda x, out, **kw: out
    res = create_footprints_in_directory(dummy_dir, workers=1, vector_format="shp")
    assert len(res) == 2
    assert mock_create.call_count == 2
    assert str(res[0]).endswith(".shp")


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_parallel(mock_create, dummy_dir):
    mock_create.side_effect = lambda x, out, **kw: out
    res = create_footprints_in_directory(dummy_dir, workers=2, vector_format="gpkg")
    assert len(res) == 2
    assert mock_create.call_count == 2
    assert str(res[0]).endswith(".gpkg")


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_skip_existing(mock_create, dummy_dir):
    # Create an existing output footprint with a newer timestamp
    r1 = dummy_dir / "r1.tif"
    out1 = dummy_dir / "r1_footprint.gpkg"
    out1.touch()
    import time

    # ensure out1 is newer than r1
    time.sleep(0.01)
    out1.touch()

    mock_create.side_effect = lambda x, out, **kw: out
    res = create_footprints_in_directory(dummy_dir, workers=1, overwrite=False)
    # r1 skipped, r2 created
    assert len(res) == 2
    assert mock_create.call_count == 1  # only called for r2
