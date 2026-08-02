"""Tests for ryan_library.functions.lidar_processing."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import time

from ryan_library.functions import lidar_processing as lp


@pytest.fixture
def mock_laspy_open():
    with patch("ryan_library.functions.lidar_processing.laspy.open") as mock_open:
        yield mock_open


def test_convert_laz_to_las_success(tmp_path: Path, mock_laspy_open: MagicMock) -> None:
    source = tmp_path / "in.laz"
    source.write_text("dummy laz")
    output = tmp_path / "out.las"

    mock_reader = MagicMock()
    mock_writer = MagicMock()
    mock_reader.header = "dummy_header"
    mock_reader.chunk_iterator.return_value = [["pt1", "pt2"], ["pt3"]]

    # laspy.open returns reader first, then writer
    mock_laspy_open.side_effect = [
        MagicMock(__enter__=MagicMock(return_value=mock_reader)),
        MagicMock(__enter__=MagicMock(return_value=mock_writer)),
    ]

    result = lp.convert_laz_to_las(source, output)
    assert result == output.resolve()
    assert mock_writer.write_points.call_count == 2
    mock_laspy_open.assert_any_call(source.resolve())
    mock_laspy_open.assert_any_call(output.resolve(), mode="w", header="dummy_header", do_compress=False)


def test_convert_laz_to_las_overwrite(tmp_path: Path, mock_laspy_open: MagicMock) -> None:
    source = tmp_path / "in.laz"
    source.write_text("dummy")
    output = tmp_path / "out.las"
    output.write_text("exists")

    with pytest.raises(FileExistsError):
        lp.convert_laz_to_las(source, output, overwrite=False)


def test_convert_laz_to_las_invalid_chunk(tmp_path: Path) -> None:
    source = tmp_path / "in.laz"
    output = tmp_path / "out.las"

    with pytest.raises(ValueError):
        lp.convert_laz_to_las(source, output, chunk_size=0)


def test_convert_laz_directory(tmp_path: Path, mock_laspy_open: MagicMock) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "1.laz").write_text("laz1")
    (src_dir / "2.laz").write_text("laz2")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    mock_laspy_open.return_value.__enter__.return_value = MagicMock()

    results = lp.convert_laz_directory(src_dir, out_dir, workers=1)

    assert len(results) == 2
    assert out_dir.resolve() / "1.las" in results
    assert out_dir.resolve() / "2.las" in results


def test_convert_laz_directory_parallel(tmp_path: Path, mock_laspy_open: MagicMock) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "1.laz").write_text("laz1")
    (src_dir / "2.laz").write_text("laz2")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    mock_laspy_open.return_value.__enter__.return_value = MagicMock()

    results = lp.convert_laz_directory(src_dir, out_dir, workers=2)
    assert len(results) == 2


def test_convert_laz_directory_skips_current(tmp_path: Path, mock_laspy_open: MagicMock) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    f1 = src_dir / "1.laz"
    f1.write_text("laz1")

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    out1 = out_dir / "1.las"
    out1.write_text("las1")

    # Make sure output is newer
    time.sleep(0.01)
    out1.touch()

    # Output exists and is current, should skip conversion
    # So laspy.open should not be called
    results = lp.convert_laz_directory(src_dir, out_dir, workers=1, overwrite=False)
    assert len(results) == 1
    mock_laspy_open.assert_not_called()


def test_convert_laz_directory_empty(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    out_dir = tmp_path / "out"

    results = lp.convert_laz_directory(src_dir, out_dir)
    assert results == []
