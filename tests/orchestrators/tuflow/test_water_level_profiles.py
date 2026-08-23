"""Focused tests for the TUFLOW water-level profile workflow."""

# Rasterio and GeoPandas expose incomplete third-party typing.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
from matplotlib.axes import Axes
import numpy as np
import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs]
from shapely.geometry import LineString, MultiLineString

matplotlib.use("Agg")

from ryan_library.orchestrators.tuflow.water_level_profiles import (  # noqa: E402
    WaterLevelProfileConfig,
    discover_tuflow_profile_rasters,
    run_water_level_profile_workflow,
    split_profile_line,
)


def _write_raster(path: Path, value: float, *, crs: str | None = "EPSG:28351") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = np.full((4, 4), value, dtype=np.float64)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float64",
        crs=crs,
        transform=from_origin(0.0, 4.0, 1.0, 1.0),
        nodata=-999.0,
    ) as destination:
        destination.write(values, 1)
    return path


def test_discovery_requires_exactly_one_raster_per_aep(tmp_path: Path) -> None:
    first = tmp_path / "first" / "Model_01.00p_h_HR_Max.tif"
    duplicate = tmp_path / "second" / "Other_01.00p_h_HR_Max.tif"
    first.parent.mkdir()
    duplicate.parent.mkdir()
    first.touch()
    duplicate.touch()

    with pytest.raises(ValueError, match="multiple matches"):
        discover_tuflow_profile_rasters(
            tmp_path,
            target_aeps=("01.00p",),
            target_result_type="h_HR_Max",
        )


def test_discovery_reports_missing_requested_aep(tmp_path: Path) -> None:
    raster = tmp_path / "Model_01.00p_h_HR_Max.tif"
    raster.touch()

    with pytest.raises(ValueError, match="20.00p: no matching"):
        discover_tuflow_profile_rasters(
            tmp_path,
            target_aeps=("01.00p", "20.00p"),
            target_result_type="h_HR_Max",
        )


def test_disconnected_multiline_is_rejected_or_returned_in_full() -> None:
    geometry = MultiLineString((((0.0, 0.0), (1.0, 0.0)), ((2.0, 0.0), (3.0, 0.0))))

    with pytest.raises(ValueError, match="2 disconnected line parts"):
        split_profile_line(geometry, line_name="cross section", disconnected_handling="error")

    parts = split_profile_line(geometry, line_name="cross section", disconnected_handling="separate")
    assert len(parts) == 2
    assert sum(part.length for part in parts) == pytest.approx(2.0)


def test_workflow_infers_missing_line_crs_from_rasters(tmp_path: Path) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    lines = gpd.GeoDataFrame(
        {"Code": ["A/1"]},
        geometry=[LineString(((0.5, 3.5), (3.5, 3.5)))],
        crs=None,
    )
    lines.to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0)
    results = tmp_path / "results"
    _write_raster(results / "01.00p" / "Model_01.00p_h_HR_Max.tif", 2.0)
    _write_raster(results / "20.00p" / "Model_20.00p_h_HR_Max.tif", 3.0)
    output = tmp_path / "plots"

    created = run_water_level_profile_workflow(
        WaterLevelProfileConfig(
            lines_gpkg=lines_path,
            lines_layer_name="profiles",
            name_field="Code",
            lines_crs_if_missing=None,
            terrain_raster=terrain,
            tuflow_results_dir=results,
            output_dir=output,
            target_aeps=("01.00p", "20.00p"),
            spacing=0.5,
        )
    )

    assert created == (output / "A_1_profile.png",)
    assert created[0].is_file()
    assert not tuple(output.glob(".*.tmp.png"))


def test_workflow_uses_scenario_in_title_and_filename_and_adds_minor_gridlines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    lines = gpd.GeoDataFrame(
        {"Code": ["Bogada West"]},
        geometry=[LineString(((0.5, 3.5), (3.5, 3.5)))],
        crs="EPSG:28351",
    )
    lines.to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0)
    results = tmp_path / "results"
    _write_raster(results / "Model_01.00p_h_HR_Max.tif", 2.0)
    output = tmp_path / "plots"

    titles: list[str] = []
    grid_modes: list[str] = []
    minor_ticks_enabled = False
    original_set_title = Axes.set_title
    original_grid = Axes.grid
    original_minorticks_on = Axes.minorticks_on

    def record_title(self: Axes, label: str, *args: Any, **kwargs: Any) -> object:
        titles.append(label)
        return original_set_title(self, label, *args, **kwargs)

    def record_grid(self: Axes, *args: Any, **kwargs: Any) -> None:
        grid_modes.append(str(kwargs.get("which", "major")))
        original_grid(self, *args, **kwargs)

    def record_minor_ticks(self: Axes) -> None:
        nonlocal minor_ticks_enabled
        minor_ticks_enabled = True
        original_minorticks_on(self)

    monkeypatch.setattr(Axes, "set_title", record_title)
    monkeypatch.setattr(Axes, "grid", record_grid)
    monkeypatch.setattr(Axes, "minorticks_on", record_minor_ticks)

    created = run_water_level_profile_workflow(
        WaterLevelProfileConfig(
            lines_gpkg=lines_path,
            lines_layer_name="profiles",
            name_field="Code",
            terrain_raster=terrain,
            tuflow_results_dir=results,
            output_dir=output,
            target_aeps=("01.00p",),
            scenario_name="CIL",
        )
    )

    assert created == (output / "CIL - Bogada West_profile.png",)
    assert titles == ["CIL - Bogada West"]
    assert minor_ticks_enabled
    assert "major" in grid_modes
    assert "minor" in grid_modes


def test_workflow_rejects_blank_scenario_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="scenario_name cannot be blank"):
        run_water_level_profile_workflow(
            WaterLevelProfileConfig(
                lines_gpkg=tmp_path / "profiles.gpkg",
                terrain_raster=tmp_path / "terrain.tif",
                tuflow_results_dir=tmp_path / "results",
                output_dir=tmp_path / "plots",
                target_aeps=("01.00p",),
                scenario_name="  ",
            )
        )


def test_workflow_assumes_shared_coordinates_when_all_crs_metadata_is_missing(
    tmp_path: Path,
) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"Code": ["A"]},
        geometry=[LineString(((0.5, 3.5), (3.5, 3.5)))],
        crs=None,
    ).to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0, crs=None)
    results = tmp_path / "results"
    _write_raster(results / "Model_01.00p_h_HR_Max.tif", 2.0, crs=None)
    output = tmp_path / "plots"

    created = run_water_level_profile_workflow(
        WaterLevelProfileConfig(
            lines_gpkg=lines_path,
            lines_layer_name="profiles",
            lines_crs_if_missing=None,
            name_field="Code",
            terrain_raster=terrain,
            tuflow_results_dir=results,
            output_dir=output,
            target_aeps=("01.00p",),
        )
    )

    assert created == (output / "A_profile.png",)


def test_workflow_infers_missing_raster_crs_from_lines(tmp_path: Path) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"Code": ["A"]},
        geometry=[LineString(((0.5, 3.5), (3.5, 3.5)))],
        crs="EPSG:28351",
    ).to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0, crs=None)
    results = tmp_path / "results"
    _write_raster(results / "Model_01.00p_h_HR_Max.tif", 2.0, crs=None)
    output = tmp_path / "plots"

    created = run_water_level_profile_workflow(
        WaterLevelProfileConfig(
            lines_gpkg=lines_path,
            lines_layer_name="profiles",
            name_field="Code",
            terrain_raster=terrain,
            tuflow_results_dir=results,
            output_dir=output,
            target_aeps=("01.00p",),
        )
    )

    assert created == (output / "A_profile.png",)


def test_workflow_rejects_conflicting_known_raster_crs(tmp_path: Path) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"Code": ["A"]},
        geometry=[LineString(((0.5, 3.5), (3.5, 3.5)))],
        crs="EPSG:28351",
    ).to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0, crs="EPSG:28351")
    results = tmp_path / "results"
    _write_raster(results / "Model_01.00p_h_HR_Max.tif", 2.0, crs="EPSG:28350")

    with pytest.raises(ValueError, match="CRS mismatch"):
        run_water_level_profile_workflow(
            WaterLevelProfileConfig(
                lines_gpkg=lines_path,
                lines_layer_name="profiles",
                name_field="Code",
                terrain_raster=terrain,
                tuflow_results_dir=results,
                output_dir=tmp_path / "plots",
                target_aeps=("01.00p",),
            )
        )


def test_workflow_rejects_sanitized_filename_collisions_before_plotting(
    tmp_path: Path,
) -> None:
    lines_path = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"Code": ["A/B", "A\\B"]},
        geometry=[
            LineString(((0.5, 3.5), (3.5, 3.5))),
            LineString(((0.5, 2.5), (3.5, 2.5))),
        ],
        crs="EPSG:28351",
    ).to_file(lines_path, layer="profiles")
    terrain = _write_raster(tmp_path / "terrain.tif", 1.0)
    results = tmp_path / "results"
    _write_raster(results / "Model_01.00p_h_HR_Max.tif", 2.0)
    output = tmp_path / "plots"

    with pytest.raises(ValueError, match="Duplicate profile output filename"):
        run_water_level_profile_workflow(
            WaterLevelProfileConfig(
                lines_gpkg=lines_path,
                lines_layer_name="profiles",
                name_field="Code",
                terrain_raster=terrain,
                tuflow_results_dir=results,
                output_dir=output,
                target_aeps=("01.00p",),
            )
        )

    assert not output.exists()
