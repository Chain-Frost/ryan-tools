"""Fail-closed checks before expensive GRASS computation."""

from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.initialiser.workflow import BuildConfig, build


def config_for(tmp_path: Path) -> BuildConfig:
    return BuildConfig(
        terrains=(tmp_path / "dem.tif",),
        aoi=tmp_path / "aoi.gpkg",
        output=tmp_path / "output",
        name="Example",
        scenario="baseline",
        prj=tmp_path / "proj.prj",
        tuflow_executable=tmp_path / "TUFLOW.exe",
        rainfall_name="RF01",
    )


def test_missing_files_raise(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build(config_for(tmp_path))


def test_never_overwrites_existing_project(tmp_path: Path) -> None:
    cfg = config_for(tmp_path)
    cfg.output.mkdir()
    with pytest.raises(FileExistsError, match="protected"):
        build(cfg)
