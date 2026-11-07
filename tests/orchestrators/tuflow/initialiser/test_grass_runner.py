"""Unit tests do not require installation of GRASS itself."""

from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.initialiser.grass_runner import process_terrain


def test_missing_dem(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        process_terrain(tmp_path / "absent.tif", work_dir=tmp_path / "work")


def test_reject_nonempty_staging(tmp_path: Path) -> None:
    dem = tmp_path / "dem.tif"
    dem.touch()
    work = tmp_path / "work"
    work.mkdir()
    (work / "other").touch()
    with pytest.raises(FileExistsError, match="empty"):
        process_terrain(dem, work_dir=work)
