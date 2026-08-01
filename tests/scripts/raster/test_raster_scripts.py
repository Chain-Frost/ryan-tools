"""Integration tests for human-facing raster scripts using synthetic fixtures."""

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
from types import ModuleType

import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]


def _load_script(filename: str) -> ModuleType:
    script = Path(__file__).parents[3] / "ryan-scripts" / "raster-python" / filename
    spec = importlib.util.spec_from_file_location(f"{script.stem}_for_tests", script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_square_raster_cells_preserves_shape_and_creates_square_resolution(
    raster_test_data: Path, tmp_path: Path
) -> None:
    source = raster_test_data / "square_cells" / "non_square_cells.tif"
    output = tmp_path / "square.tif"
    module = _load_script("square_raster_cells.py")

    module.square_raster_cells(
        source, output, 1.0
    )  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

    with rasterio.open(source) as source_dataset, rasterio.open(output) as output_dataset:
        assert output_dataset.shape == source_dataset.shape
        assert source_dataset.res == (2.0, 3.0)
        assert output_dataset.res == (1.0, 1.0)
        assert output_dataset.crs == source_dataset.crs
        assert output_dataset.nodata == source_dataset.nodata


def test_tif_to_xyz_drops_nodata_cells(raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = raster_test_data / "square_cells" / "non_square_cells.tif"
    copied = Path(shutil.copy2(source, tmp_path / source.name))
    output_directory = tmp_path / "xyz"
    output_directory.mkdir()
    module = _load_script("tif_to_xyz_drop_na_v2.py")
    monkeypatch.setattr(module, "OUT_FOLDER", str(output_directory))
    monkeypatch.setattr(module, "DROP_NA", True)

    module.process_tif_file(str(copied))  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

    output = output_directory / "non_square_cells_mod.xyz"
    lines = output.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "x,y,z"
    assert len(lines) == 96  # header plus 95 valid cells from the 8 x 12 source
