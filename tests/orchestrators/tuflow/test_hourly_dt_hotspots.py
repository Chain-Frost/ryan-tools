"""Synthetic raster regression tests for time-aligned TUFLOW HPC hotspot analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import fiona
import numpy as np
import pytest
import rasterio
from affine import Affine
from rasterio.transform import from_origin

from ryan_library.functions.tuflow.hourly_dt_hotspots import discover_hourly_rasters
from ryan_library.orchestrators.tuflow.hourly_dt_hotspots import HourlyHotspotConfig, run_hourly_dt_hotspot_workflow


def write_grid(path: Path, values: np.ndarray, *, transform: Affine | None = None, crs: str = "EPSG:28351") -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=values.shape[0],
        width=values.shape[1],
        count=1,
        dtype="float32",
        nodata=-9999.0,
        crs=crs,
        transform=transform or from_origin(500000.0, 7000000.0, 10.0, 10.0),
    ) as dst:
        dst.write(values.astype(np.float32), 1)


def write_hour(grid_dir: Path, hour: int, **grids: np.ndarray) -> None:
    for name, grid in grids.items():
        write_grid(grid_dir / f"example_{name}_{hour:03}_00_00.tif", grid)


def records_from_layer(path: Path, layer: str) -> list[dict[str, Any]]:
    with fiona.open(path, layer=layer) as src:
        return [dict(feature["properties"]) for feature in src]


def test_hourly_synchronous_and_overall_summary_ignore_global_min(tmp_path: Path) -> None:
    dt1 = np.full((4, 4), 6.0)
    dt2 = dt1.copy()
    dt1[1, 1] = 2.0
    dt2[2, 2] = 1.0
    nu1 = np.full((4, 4), 0.1)
    nu2 = nu1.copy()
    nu1[1, 1] = 0.90
    nu2[2, 2] = 0.95
    nc = np.full((4, 4), 0.2)
    nd1 = np.full((4, 4), 0.1)
    nd2 = nd1.copy()
    nd1[1, 2] = 0.28
    nd2[2, 2] = 0.28
    write_hour(tmp_path, 1, dt=dt1, Nu=nu1, Nc=nc, Nd=nd1)
    write_hour(tmp_path, 2, dt=dt2, Nu=nu2, Nc=nc, Nd=nd2)
    whole_run_min = dt1.copy()
    whole_run_min[0, 0] = 0.0001
    write_grid(tmp_path / "example_dt_Min.tif", whole_run_min)

    output = tmp_path / "hotspots.gpkg"
    result = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output))
    assert result.hours_processed == 2
    assert result.hourly_dt_hotspots == 2
    assert result.overall_dt_hotspots == 1
    assert set(fiona.listlayers(output)) == {
        "hourly_dt_hotspots",
        "hourly_parameter_hotspots",
        "overall_dt_hotspots",
        "analysis_metadata",
    }
    hourly = records_from_layer(output, "hourly_dt_hotspots")
    assert [item["hour"] for item in hourly] == [1, 2]
    assert [item["min_dt_s"] for item in hourly] == pytest.approx([2.0, 1.0])
    assert hourly[0]["nd_peak"] == pytest.approx(0.1)
    assert hourly[0]["nd_near"] == 0  # Hour-1 Nd peak is outside the hour-1 low-dt patch.
    assert hourly[0]["nu_peak"] == pytest.approx(0.9)
    assert hourly[1]["nd_near"] == 1  # 0.28 is near 0.3, not near 1.0.
    summary = records_from_layer(output, "overall_dt_hotspots")
    assert len(summary) == 1
    assert summary[0]["worst_hour"] == 2
    assert summary[0]["min_dt_s"] == 1
    assert summary[0]["nu_at_worst"] == pytest.approx(0.95)
    assert summary[0]["nd_at_worst"] == pytest.approx(0.28)
    assert summary[0]["highest_ratio_at_worst"] == "Nu"
    assert summary[0]["cells_ever"] == 2
    assert summary[0]["area_ha"] == pytest.approx(0.02)
    metadata = records_from_layer(output, "analysis_metadata")
    assert metadata[0]["run_prefix"] == "example"
    assert metadata[0]["nd_limit"] == pytest.approx(0.3)
    params = records_from_layer(output, "hourly_parameter_hotspots")
    assert any(x["hour"] == 1 and x["parameter"] == "Nd" and x["cells_low_dt"] == 0 for x in params)
    assert result.hourly_parameter_hotspots == len(params)


def test_max_hour_excludes_unselected_hourly_rasters(tmp_path: Path) -> None:
    values = np.full((2, 2), 2.0)
    good = np.full((2, 2), 0.2)
    write_hour(tmp_path, 1, dt=values, Nu=good, Nc=good, Nd=good)
    write_hour(tmp_path, 2, dt=values, Nu=good, Nc=good, Nd=good)
    result = run_hourly_dt_hotspot_workflow(
        HourlyHotspotConfig(grid_dir=tmp_path, output=tmp_path / "out.gpkg", max_hour=1)
    )
    assert result.hours_processed == 1
    assert {x["hour"] for x in records_from_layer(result.output, "hourly_dt_hotspots")} == {1}


def test_missing_control_rejected_even_with_whole_run_min(tmp_path: Path) -> None:
    values = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=values, Nu=values, Nc=values)
    write_grid(tmp_path / "example_dt_Min.tif", values)
    with pytest.raises(ValueError, match="missing Nd"):
        run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, tmp_path / "no.gpkg", dry_run=True))


def test_grid_misalignment_rejected_and_existing_output_protected(tmp_path: Path) -> None:
    values = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=values, Nu=values, Nc=values, Nd=values)
    out = tmp_path / "existing.gpkg"
    out.write_bytes(b"keep me")
    write_grid(tmp_path / "example_Nd_001_00_00.tif", values, transform=from_origin(500001, 7000000, 10, 10))
    with pytest.raises(ValueError, match="grid/CRS"):
        run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, out, overwrite=True))
    assert out.read_bytes() == b"keep me"


def test_empty_hotspot_layers_are_written(tmp_path: Path) -> None:
    high_dt = np.full((2, 2), 10.0)
    small = np.full((2, 2), 0.1)
    write_hour(tmp_path, 1, dt=high_dt, Nu=small, Nc=small, Nd=small)
    result = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, tmp_path / "empty.gpkg"))
    assert result.hourly_dt_hotspots == 0
    assert result.hourly_parameter_hotspots == 0
    assert result.overall_dt_hotspots == 0
    assert len(fiona.listlayers(result.output)) == 4


def test_parameter_limits_and_configuration_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="near_limit_fraction"):
        run_hourly_dt_hotspot_workflow(
            HourlyHotspotConfig(tmp_path, tmp_path / "x.gpkg", near_limit_fraction=float("nan"))
        )
    with pytest.raises(ValueError, match="Nd control limit"):
        run_hourly_dt_hotspot_workflow(
            HourlyHotspotConfig(tmp_path, tmp_path / "x.gpkg", control_limits={"Nu": 1, "Nc": 1, "Nd": 0})
        )


def test_mixed_prefixes_require_selection(tmp_path: Path) -> None:
    value = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=value, Nu=value, Nc=value, Nd=value)
    write_grid(tmp_path / "second_dt_001_00_00.tif", value)
    with pytest.raises(ValueError, match="Multiple simulation prefixes"):
        discover_hourly_rasters(tmp_path)
    prefix, sets = discover_hourly_rasters(tmp_path, prefix="example")
    assert prefix == "example"
    assert len(sets) == 1


def test_overwrite_requires_explicit_switch(tmp_path: Path) -> None:
    value = np.full((2, 2), 2.0)
    write_hour(tmp_path, 1, dt=value, Nu=value, Nc=value, Nd=value)
    output = tmp_path / "out.gpkg"
    run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output))
    with pytest.raises(FileExistsError, match="--overwrite"):
        run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output))
    again = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output, overwrite=True))
    assert again.output == output


def test_dry_run_does_not_create_output(tmp_path: Path) -> None:
    value = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=value, Nu=value, Nc=value, Nd=value)
    output = tmp_path / "out.gpkg"
    report = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output, dry_run=True))
    assert report.dry_run
    assert not output.exists()


def test_nodata_and_diagonal_connectivity(tmp_path: Path) -> None:
    dt = np.full((3, 3), 10.0)
    dt[0, 0] = 2.0
    dt[1, 1] = 3.0
    dt[2, 2] = -9999.0
    control = np.full((3, 3), 0.01)
    write_hour(tmp_path, 1, dt=dt, Nu=control, Nc=control, Nd=control)
    out8 = tmp_path / "eight.gpkg"
    out4 = tmp_path / "four.gpkg"
    result8 = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, out8, connectivity=8))
    result4 = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, out4, connectivity=4))
    assert result8.hourly_dt_hotspots == 1
    assert result4.hourly_dt_hotspots == 2
    assert records_from_layer(out8, "hourly_dt_hotspots")[0]["cells_low"] == 2


def test_geographic_crs_is_not_assumed_to_be_metres(tmp_path: Path) -> None:
    value = np.ones((2, 2))
    for parameter in ("dt", "Nu", "Nc", "Nd"):
        write_grid(tmp_path / f"example_{parameter}_001_00_00.tif", value, crs="EPSG:4326")
    with pytest.raises(ValueError, match="projected CRS"):
        run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, tmp_path / "out.gpkg", dry_run=True))


def test_failure_during_export_preserves_existing_gpkg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=value, Nu=value, Nc=value, Nd=value)
    output = tmp_path / "existing.gpkg"
    output.write_bytes(b"original file to preserve")

    def fail_write(*_args: object, **_kwargs: object) -> None:
        message = "simulated export failure"
        raise RuntimeError(message)

    from ryan_library.orchestrators.tuflow import hourly_dt_hotspots as orchestrator

    monkeypatch.setattr(orchestrator, "_write_records", fail_write)
    with pytest.raises(RuntimeError, match="simulated export failure"):
        run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output, overwrite=True))
    assert output.read_bytes() == b"original file to preserve"
    assert sorted(x.name for x in tmp_path.glob(".existing_*.gpkg")) == []

def test_dry_run_can_validate_when_output_already_exists(tmp_path: Path) -> None:
    value = np.ones((2, 2))
    write_hour(tmp_path, 1, dt=value, Nu=value, Nc=value, Nd=value)
    output = tmp_path / "existing.gpkg"
    output.write_bytes(b"do not modify")
    report = run_hourly_dt_hotspot_workflow(HourlyHotspotConfig(tmp_path, output, dry_run=True))
    assert report.dry_run
    assert output.read_bytes() == b"do not modify"
