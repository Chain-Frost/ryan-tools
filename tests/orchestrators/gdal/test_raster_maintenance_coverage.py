"""Tests for raster_maintenance orchestration."""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from ryan_library.orchestrators.gdal.raster_maintenance import create_footprints_in_directory, set_nodata_in_directory


def _return_input_raster(input_raster: Path, nodata: float, *, bands: tuple[int, ...] | None) -> Path:
    del nodata, bands
    return input_raster


def _return_output_vector(
    *, input_raster: Path, output_vector: Path, vector_format: str, layer_name: str, overwrite: bool
) -> Path:
    del input_raster, vector_format, layer_name, overwrite
    return output_vector


@pytest.fixture
def dummy_dir(tmp_path: Path) -> Path:
    d = tmp_path / "rasters"
    d.mkdir()
    (d / "r1.tif").touch()
    (d / "r2.tif").touch()
    return d


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_empty(mock_set: MagicMock, tmp_path: Path) -> None:
    assert set_nodata_in_directory(tmp_path) == []
    mock_set.assert_not_called()


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_serial(mock_set: MagicMock, dummy_dir: Path) -> None:
    mock_set.side_effect = _return_input_raster
    res = set_nodata_in_directory(dummy_dir, workers=1)
    assert len(res) == 2
    assert mock_set.call_count == 2


@patch("ryan_library.orchestrators.gdal.raster_maintenance.set_raster_nodata")
def test_set_nodata_in_directory_parallel(mock_set: MagicMock, dummy_dir: Path) -> None:
    mock_set.side_effect = _return_input_raster
    res = set_nodata_in_directory(dummy_dir, workers=2)
    assert len(res) == 2
    assert mock_set.call_count == 2


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_empty(mock_create: MagicMock, tmp_path: Path) -> None:
    assert create_footprints_in_directory(tmp_path) == []
    mock_create.assert_not_called()


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_serial(mock_create: MagicMock, dummy_dir: Path) -> None:
    mock_create.side_effect = _return_output_vector
    res = create_footprints_in_directory(dummy_dir, workers=1, vector_format="shp")
    assert len(res) == 2
    assert mock_create.call_count == 2
    assert str(res[0]).endswith(".shp")
    mock_create.assert_has_calls(
        [
            call(
                input_raster=dummy_dir.resolve() / "r1.tif",
                output_vector=dummy_dir.resolve() / "r1_footprint.shp",
                vector_format="shp",
                layer_name="raster_footprint",
                overwrite=False,
            ),
            call(
                input_raster=dummy_dir.resolve() / "r2.tif",
                output_vector=dummy_dir.resolve() / "r2_footprint.shp",
                vector_format="shp",
                layer_name="raster_footprint",
                overwrite=False,
            ),
        ]
    )


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_parallel(mock_create: MagicMock, dummy_dir: Path) -> None:
    mock_create.side_effect = _return_output_vector
    res = create_footprints_in_directory(dummy_dir, workers=2, vector_format="gpkg")
    assert len(res) == 2
    assert mock_create.call_count == 2
    assert str(res[0]).endswith(".gpkg")
    mock_create.assert_has_calls(
        [
            call(
                input_raster=dummy_dir.resolve() / "r1.tif",
                output_vector=dummy_dir.resolve() / "r1_footprint.gpkg",
                vector_format="gpkg",
                layer_name="raster_footprint",
                overwrite=False,
            ),
            call(
                input_raster=dummy_dir.resolve() / "r2.tif",
                output_vector=dummy_dir.resolve() / "r2_footprint.gpkg",
                vector_format="gpkg",
                layer_name="raster_footprint",
                overwrite=False,
            ),
        ],
        any_order=True,
    )


@patch("ryan_library.orchestrators.gdal.raster_maintenance.create_raster_footprint")
def test_create_footprints_skip_existing(mock_create: MagicMock, dummy_dir: Path) -> None:
    # Create an existing output footprint with a newer timestamp
    r1 = dummy_dir / "r1.tif"
    out1 = dummy_dir / "r1_footprint.gpkg"
    out1.touch()
    import time

    # ensure out1 is newer than r1
    time.sleep(0.01)
    out1.touch()
    assert out1.stat().st_mtime >= r1.stat().st_mtime

    mock_create.side_effect = _return_output_vector
    res = create_footprints_in_directory(dummy_dir, workers=1, overwrite=False)
    # r1 skipped, r2 created
    assert len(res) == 2
    mock_create.assert_called_once_with(
        input_raster=dummy_dir.resolve() / "r2.tif",
        output_vector=dummy_dir.resolve() / "r2_footprint.gpkg",
        vector_format="gpkg",
        layer_name="raster_footprint",
        overwrite=False,
    )
