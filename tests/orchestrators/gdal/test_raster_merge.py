"""Unit tests for ryan_library.orchestrators.gdal.raster_merge."""

from pathlib import Path
from unittest.mock import patch
import pytest

from ryan_library.orchestrators.gdal.raster_merge import (
    merge_directory,
    merge_directory_by_vector_extent,
    merge_rasters_to_temporary_vrt,
    _extents_intersect,
)


@pytest.fixture
def mock_gdal_functions():
    with (
        patch("ryan_library.orchestrators.gdal.raster_merge.build_vrt") as mock_vrt,
        patch("ryan_library.orchestrators.gdal.raster_merge.translate_to_geotiff") as mock_translate,
        patch("ryan_library.orchestrators.gdal.raster_merge.build_external_overviews") as mock_overview,
        patch("ryan_library.orchestrators.gdal.raster_merge.get_vector_extent") as mock_vector_extent,
        patch("ryan_library.orchestrators.gdal.raster_merge.get_raster_extent") as mock_raster_extent,
    ):

        yield {
            "vrt": mock_vrt,
            "translate": mock_translate,
            "overview": mock_overview,
            "vector_extent": mock_vector_extent,
            "raster_extent": mock_raster_extent,
        }


def test_extents_intersect() -> None:
    # (min_x, min_y, max_x, max_y)
    e1 = (0.0, 0.0, 10.0, 10.0)
    e2 = (5.0, 5.0, 15.0, 15.0)  # Overlaps
    e3 = (20.0, 20.0, 30.0, 30.0)  # No overlap

    assert _extents_intersect(e1, e2) is True
    assert _extents_intersect(e1, e3) is False


def test_merge_directory_no_files(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        merge_directory(input_directory=tmp_path)


def test_merge_directory_existing_output(tmp_path: Path) -> None:
    # Create an input file
    (tmp_path / "in1.tif").touch()

    # Create the default output file
    (tmp_path / f"{tmp_path.name}.tif").touch()

    with pytest.raises(FileExistsError):
        merge_directory(input_directory=tmp_path)


def test_merge_directory_success(tmp_path: Path, mock_gdal_functions) -> None:
    file1 = tmp_path / "in1.tif"
    file1.touch()

    vrt, tif = merge_directory(input_directory=tmp_path, build_overviews=True)

    assert vrt == tmp_path / f"{tmp_path.name}.vrt"
    assert tif == tmp_path / f"{tmp_path.name}.tif"

    mock_gdal_functions["vrt"].assert_called_once()
    mock_gdal_functions["translate"].assert_called_once()
    mock_gdal_functions["overview"].assert_called_once()


def test_merge_directory_by_vector_extent_no_files(tmp_path: Path, mock_gdal_functions) -> None:
    mock_gdal_functions["vector_extent"].return_value = (0, 0, 10, 10)

    with pytest.raises(FileNotFoundError):
        merge_directory_by_vector_extent(tmp_path, extent_vector=Path("ext.shp"))


def test_merge_directory_by_vector_extent_success(tmp_path: Path, mock_gdal_functions) -> None:
    file1 = tmp_path / "in1.xyz"
    file1.touch()

    mock_gdal_functions["vector_extent"].return_value = (0, 0, 10, 10)
    # Return an intersecting extent for the raster
    mock_gdal_functions["raster_extent"].return_value = (5, 5, 15, 15)

    res = merge_directory_by_vector_extent(
        tmp_path,
        extent_vector=Path("ext.shp"),
        build_overviews=True,
    )

    assert res == [file1]
    mock_gdal_functions["vrt"].assert_called_once()
    mock_gdal_functions["translate"].assert_called_once()
    mock_gdal_functions["overview"].assert_called_once()


def test_merge_directory_by_vector_extent_list_only(tmp_path: Path, mock_gdal_functions) -> None:
    file1 = tmp_path / "in1.xyz"
    file1.touch()

    mock_gdal_functions["vector_extent"].return_value = (0, 0, 10, 10)
    mock_gdal_functions["raster_extent"].return_value = (5, 5, 15, 15)

    res = merge_directory_by_vector_extent(tmp_path, extent_vector=Path("ext.shp"), list_only=True)

    assert res == [file1]
    mock_gdal_functions["vrt"].assert_not_called()
    mock_gdal_functions["translate"].assert_not_called()


def test_merge_rasters_to_temporary_vrt(mock_gdal_functions, tmp_path: Path) -> None:
    out = tmp_path / "out.tif"
    mock_gdal_functions["translate"].return_value = out

    res = merge_rasters_to_temporary_vrt([Path("a.tif")], out)
    assert res == out
    mock_gdal_functions["vrt"].assert_called_once()
    mock_gdal_functions["translate"].assert_called_once()
