"""Regression checks for editable-default and CLI-override wrapper configuration."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path

from run_grouped_asc_to_asc_statistics import _parse_cli_arguments as parse_asc_groups
from run_python_raster_calculations import _parse_cli_arguments as parse_asc_native
from audit_qgis_projects import _parse_cli_arguments as parse_qgis_audit
from audit_qgis_projects import find_qgis_projects
from batch_vector_clip import _parse_cli_arguments as parse_vector_clip
from gdal_raster_to_xyz import _parse_cli_arguments as parse_raster_xyz
from gdal_retile_wrapper import _parse_cli_arguments as parse_retile
from gdal_stage_storage import _parse_cli_arguments as parse_stage_storage
from gdal_vector_translate import _parse_cli_arguments as parse_vector_translate
from plot_ensemble_results import _parse_cli_arguments as parse_ensemble_plot
from split_vector_by_attribute import _parse_cli_arguments as parse_vector_split


def test_all_candidate_wrappers_accept_editable_defaults_without_required_cli_values() -> None:
    for parser in (
        parse_asc_groups,
        parse_asc_native,
        parse_qgis_audit,
        parse_vector_clip,
        parse_raster_xyz,
        parse_retile,
        parse_stage_storage,
        parse_vector_translate,
        parse_ensemble_plot,
        parse_vector_split,
    ):
        parser([])


def test_cli_values_remain_available_as_explicit_overrides() -> None:
    assert parse_asc_groups(["--mode", "diff", "current", "existing"]).mode == "diff"
    assert parse_vector_clip(["input.shp", "--extents", "extent.shp"]).extents == [Path("extent.shp")]
    assert parse_raster_xyz(["rasters", "--format", "xyz"]).format == "xyz"
    assert parse_retile(["input.tif", "--tile-size", "100", "200"]).tile_size == [100, 200]
    assert parse_stage_storage(["dems", "--step", "0.5", "--no-plot"]).create_plot is False
    translated = parse_vector_translate(["input.dxf", "output.gpkg", "--target-crs", "EPSG:28351"])
    assert translated.target_crs == "EPSG:28351"
    assert parse_vector_split(["input.gpkg", "output", "--attribute", "LayerName"]).attribute == "LayerName"


def test_qgis_discovery_does_not_exclude_filenames_containing_ss(tmp_path: Path) -> None:
    expected = {tmp_path / "Assessment.qgz", tmp_path / "CrossSections.qgs"}
    for project_path in expected:
        project_path.touch()

    assert set(find_qgis_projects(tmp_path)) == expected
