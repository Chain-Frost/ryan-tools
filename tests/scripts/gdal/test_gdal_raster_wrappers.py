"""Run the human-facing GDAL wrappers against copied synthetic raster data."""

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import sys
from types import ModuleType

import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]

SCRIPTS_DIRECTORY = Path(__file__).parents[3] / "ryan-scripts" / "gdal-python"


def _load_script(filename: str) -> ModuleType:
    script = SCRIPTS_DIRECTORY / filename
    module_name = f"{script.stem}_wrapper_test"
    spec = importlib.util.spec_from_file_location(module_name, script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _copy_tifs(source: Path, destination: Path) -> None:
    for path in source.glob("*.tif"):
        shutil.copy2(path, destination / path.name)


def test_translate_wrapper_converts_asc_fixture(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conversion = raster_test_data / "conversion"
    source_directory = tmp_path / "source"
    output_directory = tmp_path / "output"
    source_directory.mkdir()
    for filename in ("surface.asc", "surface.prj"):
        shutil.copy2(conversion / filename, source_directory / filename)
    module = _load_script("gdal_translate_TIF_ovr.py")
    monkeypatch.chdir(tmp_path)

    result = module.main(
        working_directory=source_directory,
        extensions=("asc",),
        output_directory=output_directory,
        build_overviews=False,
        workers=1,
    )

    assert result == 0
    output = output_directory / "surface.tif"
    with rasterio.open(output) as dataset:
        assert dataset.shape == (64, 64)
        assert dataset.dtypes == ("float32",)
        assert dataset.crs is not None and dataset.crs.to_epsg() == 7850


def test_overview_wrapper_writes_external_sidecar(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = raster_test_data / "maintenance" / "nodata_regions.tif"
    copied = Path(shutil.copy2(source, tmp_path / source.name))
    module = _load_script("gdaladdo_tif_pyramids.py")
    monkeypatch.chdir(tmp_path)

    result = module.main(
        working_directory=tmp_path,
        levels=(2, 4, 8),
        workers=1,
        recursive=False,
        refresh=True,
    )

    assert result == 0
    assert copied.with_suffix(".tif.ovr").is_file()
    with rasterio.open(copied) as dataset:
        assert dataset.overviews(1) == [2, 4, 8]


def test_merge_wrappers_use_adjacent_tiles_and_vector_extent(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tiles = tmp_path / "tiles"
    tiles.mkdir()
    _copy_tifs(raster_test_data / "merge" / "adjacent", tiles)
    merge_module = _load_script("gdal_merge.py")
    extent_module = _load_script("gdal_merge_by_extent.py")
    monkeypatch.chdir(tmp_path)

    merge_result = merge_module.main(
        working_directory=tiles,
        file_pattern="*.tif",
        output_basename="wrapper_mosaic",
        nodata_value=-9999.0,
        build_overviews=False,
    )
    extent_result = extent_module.main(
        working_directory=tiles,
        extent_vector=raster_test_data / "merge" / "select_west.gpkg",
        file_pattern="tile_*.tif",
        list_only=True,
    )

    assert merge_result == 0
    assert extent_result == 0
    with rasterio.open(tiles / "wrapper_mosaic.tif") as dataset:
        assert dataset.shape == (32, 64)


def test_grouped_mosaic_wrapper_uses_fixture_names(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _copy_tifs(raster_test_data / "grouped_mosaic", tmp_path)
    module = _load_script("build_VRT.py")
    monkeypatch.chdir(tmp_path)

    result = module.main(
        working_directory=tmp_path,
        group_remove_index=2,
        overview_levels=(2, 4),
        workers=1,
    )

    assert result == 0
    output = tmp_path / "merged_01_DEV_d_HR_Max.tif"
    assert output.is_file()
    assert output.with_suffix(".tif.ovr").is_file()


def test_maintenance_wrappers_modify_only_temporary_copy(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = raster_test_data / "maintenance" / "nodata_regions.tif"
    copied = Path(shutil.copy2(source, tmp_path / source.name))
    nodata_module = _load_script("gdal_set_nodata.py")
    footprint_module = _load_script("gdal_raster_footprint.py")
    monkeypatch.chdir(tmp_path)

    nodata_result = nodata_module.main(
        working_directory=tmp_path,
        file_pattern=copied.name,
        nodata_value=-1234.0,
        recursive=False,
        workers=1,
    )
    footprint_result = footprint_module.main(
        working_directory=tmp_path,
        file_pattern=copied.name,
        recursive=False,
        workers=1,
    )

    assert nodata_result == 0
    assert footprint_result == 0
    with rasterio.open(copied) as dataset:
        assert dataset.nodata == -1234.0
    assert copied.with_name(f"{copied.stem}_footprint.gpkg").is_file()
    with rasterio.open(source) as dataset:
        assert dataset.nodata == -9999.0


def test_flood_extent_wrapper_creates_raster_and_vector(
    raster_test_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = raster_test_data / "maintenance" / "Synthetic_EXG_01p_060m_TP01_d_HR_Max.tif"
    copied = Path(shutil.copy2(source, tmp_path / source.name))
    module = _load_script("gdal_flood_extent.py")
    monkeypatch.chdir(tmp_path)

    result = module.main(
        working_directory=tmp_path,
        paths_to_process=(tmp_path,),
        cutoff_values=(0.05,),
        file_patterns=(copied.name,),
        recursive=False,
        sieve_pixels=2,
        workers=1,
    )

    assert result == 0
    assert copied.with_name(f"{copied.stem}_FE_005m.tif").is_file()
    assert copied.with_name(f"{copied.stem}_FE_005m.gpkg").is_file()
