"""Exercise velocity_masker.py with aligned synthetic depth and velocity grids."""

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
from types import ModuleType

import numpy as np

from ryan_library.functions.gdal.raster_processing import read_masked_raster_band


def _load_velocity_masker() -> ModuleType:
    script = Path(__file__).parents[3] / "ryan-scripts" / "TUFLOW-python" / "velocity_masker.py"
    spec = importlib.util.spec_from_file_location("velocity_masker_for_tests", script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_velocity_masker_uses_fine_depth_grid_without_mutating_fixture(raster_test_data: Path, tmp_path: Path) -> None:
    source_directory = raster_test_data / "velocity_masker"
    velocity_name = "Synthetic_EXG_01p_060m_TP01_V_Max.tif"
    depth_name = "Synthetic_EXG_01p_060m_TP01_d_HR_Max.tif"
    velocity_path = Path(shutil.copy2(source_directory / velocity_name, tmp_path / velocity_name))
    depth_path = Path(shutil.copy2(source_directory / depth_name, tmp_path / depth_name))
    module = _load_velocity_masker()

    succeeded = module.process_velocity_file(  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
        velocity_path=velocity_path,
        depth_path=depth_path,
        threshold=0.05,
        aep_label="01.00p",
    )

    assert succeeded is True
    assert velocity_path.with_name(f"{velocity_path.stem}_original.tif").is_file()
    masked = read_masked_raster_band(velocity_path)
    assert np.count_nonzero(~masked.mask) == 25
    assert (source_directory / velocity_name).is_file()
