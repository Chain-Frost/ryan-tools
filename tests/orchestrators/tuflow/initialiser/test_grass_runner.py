"""Unit tests do not require installation of GRASS itself."""

from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.initialiser import grass_runner
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


def test_relative_grass_working_directory_uses_absolute_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The child GRASS subprocess must not double-prefix a relative cwd."""
    monkeypatch.chdir(tmp_path)
    dem = Path("dem.tif")
    dem.touch()
    calls: list[tuple[list[str], Path]] = []

    def fake_run(args: list[str], *, directory: Path) -> None:
        calls.append((args, directory))
        for argument in args:
            if argument.startswith("output=") and argument.endswith(".tif"):
                Path(argument.partition("=")[2]).touch()

    monkeypatch.setattr(grass_runner, "_run", fake_run)
    results = process_terrain(dem, work_dir=Path("work"), grass_executable="grass")
    work = tmp_path / "work"
    assert results.flow_direction == work / "d8_direction.tif"
    assert results.accumulation == work / "upstream_cells.tif"
    assert results.conditioned_dem == work / "hydro_dem.tif"
    assert all(directory == work for _, directory in calls)
    expected_mapset = str(work / "grass_project" / "PERMANENT")
    assert all(args[1] == expected_mapset for args, _ in calls[1:])
