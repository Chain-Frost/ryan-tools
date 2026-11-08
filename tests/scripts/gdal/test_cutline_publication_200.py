"""Real GDAL cutline publication and CRS regressions for issue #200."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pytest
from osgeo import gdal, ogr, osr  # type: ignore

# The native GDAL bindings do not ship complete stubs.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
# pyright: reportMissingTypeStubs=false

pytestmark = pytest.mark.gdal


def _script() -> ModuleType:
    path = Path(__file__).absolute().parents[3] / "ryan-scripts/gdal-python/gdalwarp_clip_to_polygon.py"
    spec = importlib.util.spec_from_file_location("cutline_under_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _crs(code: int) -> Any:
    crs = osr.SpatialReference()
    crs.ImportFromEPSG(code)
    crs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return crs


def _case(root: Path, *, raster_crs: int = 4326) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    raster = root / "source.tif"
    ds = gdal.GetDriverByName("GTiff").Create(str(raster), 10, 10, 1, gdal.GDT_Float32)
    ds.SetGeoTransform((115.0, 0.01, 0.0, -31.0, 0.0, -0.01))
    ds.SetProjection(_crs(raster_crs).ExportToWkt())
    values = np.full((10, 10), 7.0, dtype=np.float32)
    values[4, 4] = -9999.0
    ds.GetRasterBand(1).SetNoDataValue(-9999.0)
    ds.GetRasterBand(1).WriteArray(values)
    ds = None
    vector = root / "boundary.gpkg"
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(str(vector))
    layer = ds.CreateLayer("boundary", srs=_crs(3857), geom_type=ogr.wkbPolygon)
    ring = ogr.Geometry(ogr.wkbLinearRing)
    transform = osr.CoordinateTransformation(_crs(4326), _crs(3857))
    for lon, lat in ((115.02, -31.02), (115.08, -31.02), (115.08, -31.08), (115.02, -31.08), (115.02, -31.02)):
        x, y, _z = transform.TransformPoint(lon, lat)
        ring.AddPoint_2D(x, y)
    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetGeometry(polygon)
    assert layer.CreateFeature(feature) == 0
    feature = layer = ds = None
    return raster, vector


def test_metadata_source_crs_and_differing_cutline_crs(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    original = source.read_bytes()
    output = tmp_path / "out.tif"
    assert script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326")
    with gdal.Open(str(output)) as ds:
        assert ds.GetSpatialRef().IsSame(_crs(4326))
        assert (ds.RasterXSize, ds.RasterYSize) == (6, 6)
        assert ds.GetGeoTransform() == pytest.approx((115.02, 0.01, 0.0, -31.02, 0.0, -0.01))
        expected = np.full((6, 6), 7.0, dtype=np.float32)
        expected[2, 2] = -9999.0
        np.testing.assert_array_equal(ds.ReadAsArray(), expected)
        assert ds.GetRasterBand(1).GetNoDataValue() == -9999.0
    assert source.read_bytes() == original


def test_independent_target_crs_and_explicit_source_override(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path, raster_crs=3857)
    output = tmp_path / "out.tif"
    assert script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:3857", source_crs="EPSG:4326")
    with gdal.Open(str(output)) as ds:
        assert ds.GetSpatialRef().IsSame(_crs(3857))
        transform = osr.CoordinateTransformation(_crs(4326), _crs(3857))
        west, north, _z = transform.TransformPoint(115.02, -31.02)
        assert ds.GetGeoTransform()[0] == pytest.approx(west)
        assert ds.GetGeoTransform()[3] == pytest.approx(north)
        values = ds.ReadAsArray()
        assert set(np.unique(values)) == {-9999.0, 7.0}


@pytest.mark.parametrize("overwrite", [False, True])
def test_source_hardlink_alias_is_never_overwritten(tmp_path: Path, overwrite: bool) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    original = source.read_bytes()
    alias = tmp_path / "alias.tif"
    os.link(source, alias)
    assert not script.run_gdalwarp(source, alias, cutline, "boundary", "EPSG:4326", overwrite=overwrite)
    assert alias.read_bytes() == source.read_bytes() == original


def test_batch_collision_is_rejected_before_any_output(tmp_path: Path) -> None:
    script = _script()
    first, cutline = _case(tmp_path / "first")
    _case(tmp_path / "second")
    output = tmp_path / "output"
    assert (
        script.main(input_directories=[first.parent, tmp_path / "second"], cutline=cutline, output_directory=output)
        == 1
    )
    assert not output.exists()


def test_output_aliases_another_batch_source(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    other = tmp_path / "source_clipped.tif"
    other.write_bytes(source.read_bytes())
    original = other.read_bytes()
    assert script.main(input_directories=tmp_path, cutline=cutline, output_directory=tmp_path, overwrite=True) == 1
    assert other.read_bytes() == original
    assert not (tmp_path / "source_clipped_clipped.tif").exists()


def test_failed_real_warp_preserves_existing_output_and_foreign_stage(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "out.tif"
    output.write_bytes(b"previous")
    foreign = tmp_path / ".out.tmp.tif"
    foreign.write_bytes(b"other owner")
    assert not script.run_gdalwarp(source, output, cutline, "missing-layer", "EPSG:4326", overwrite=True)
    assert output.read_bytes() == b"previous"
    assert foreign.read_bytes() == b"other owner"
    assert not list(tmp_path.glob(".out.tif.staging-*"))


def test_late_creator_survives_real_warp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "out.tif"
    warp = script.gdal.Warp

    def competing_warp(*args: Any, **kwargs: Any) -> Any:
        ds = warp(*args, **kwargs)
        output.write_bytes(b"late creator")
        return ds

    monkeypatch.setattr(script.gdal, "Warp", competing_warp)
    assert not script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326")
    assert output.read_bytes() == b"late creator"
    assert not list(tmp_path.glob(".out.tif.staging-*"))


def test_existing_output_requires_explicit_overwrite(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "out.tif"
    output.write_bytes(b"previous")
    assert not script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326")
    assert output.read_bytes() == b"previous"
    assert script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326", overwrite=True)
    assert output.read_bytes()[:4] in (b"II*\0", b"II+\0")


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "output"
    assert script.main(input_directories=source.parent, cutline=cutline, output_directory=output, dry_run=True) == 0
    assert not output.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junction")
def test_junction_destination_is_rejected(tmp_path: Path) -> None:
    import _winapi

    script = _script()
    source, cutline = _case(tmp_path / "input")
    real = tmp_path / "real"
    real.mkdir()
    junction = tmp_path / "linked"
    try:
        _winapi.CreateJunction(str(real), str(junction))
    except OSError as exc:
        pytest.skip(f"Filesystem cannot create directory junctions: {exc}")
    try:
        assert script.main(input_directories=source.parent, cutline=cutline, output_directory=junction) == 1
        assert not list(real.iterdir())
    finally:
        junction.rmdir()


def test_case_alias_is_preserved_even_with_overwrite(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    alias = tmp_path / "OUT.TIF"
    alias.write_bytes(b"case owner")
    assert not script.run_gdalwarp(source, tmp_path / "out.tif", cutline, "boundary", "EPSG:4326", overwrite=True)
    assert alias.read_bytes() == b"case owner"


def test_old_output_sidecar_is_preserved(tmp_path: Path) -> None:
    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "out.tif"
    sidecar = tmp_path / "out.tif.aux.xml"
    sidecar.write_bytes(b"old metadata")
    assert not script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326", overwrite=True)
    assert sidecar.read_bytes() == b"old metadata"
    assert not output.exists()


def test_concurrent_real_warps_publish_once(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    script = _script()
    source, cutline = _case(tmp_path)
    barrier = Barrier(2)
    output = tmp_path / "out.tif"

    def attempt(_index: int) -> bool:
        barrier.wait(timeout=10)
        return bool(script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326"))

    with ThreadPoolExecutor(max_workers=2) as workers:
        assert sorted(workers.map(attempt, (0, 1))) == [False, True]
    with gdal.Open(str(output)) as ds:
        assert ds.RasterXSize == 6
    assert not list(tmp_path.glob(".out.tif.staging-*"))


@pytest.mark.skipif(os.name != "nt", reason="Windows sharing-mode lock")
def test_locked_output_is_preserved(tmp_path: Path) -> None:
    import ctypes
    from ctypes import wintypes

    script = _script()
    source, cutline = _case(tmp_path)
    output = tmp_path / "out.tif"
    output.write_bytes(b"locked owner")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    handle = create_file(str(output), 0x80000000, 1, None, 3, 0, None)
    assert handle != ctypes.c_void_p(-1).value
    try:
        assert not script.run_gdalwarp(source, output, cutline, "boundary", "EPSG:4326", overwrite=True)
        assert output.read_bytes() == b"locked owner"
    finally:
        close(handle)
    assert not list(tmp_path.glob(".out.tif.staging-*"))
