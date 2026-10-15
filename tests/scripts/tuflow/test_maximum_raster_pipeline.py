"""Drop-in defaults, configurable grouping and GDAL maximum regression tests."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest

gdal = pytest.importorskip("osgeo.gdal", reason="Requires QGIS 4/Python GDAL bindings")
osr = pytest.importorskip("osgeo.osr")

from ryan_library.functions.gdal.raster_processing import build_vrt

SCRIPT = (
    Path(__file__).absolute().parents[3]
    / "ryan-scripts"
    / "TUFLOW-python"
    / "raster_processing"
    / "tuflow_maximum_pipeline.py"
)


def load_pipeline():
    spec = importlib.util.spec_from_file_location("testable_tuflow_maximum_pipeline", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_raster(path: Path, values: np.ndarray, *, origin_x: float, epsg: int = 28350) -> None:
    driver = gdal.GetDriverByName("GTiff")
    rows, columns = values.shape
    dataset = driver.Create(str(path), columns, rows, 1, gdal.GDT_Float32)
    assert dataset is not None
    dataset.SetGeoTransform((origin_x, 1.0, 0.0, 2.0, 0.0, -1.0))
    spatial_reference = osr.SpatialReference()
    spatial_reference.ImportFromEPSG(epsg)
    dataset.SetProjection(spatial_reference.ExportToWkt())
    band = dataset.GetRasterBand(1)
    band.SetNoDataValue(-9999.0)
    band.WriteArray(values.astype(np.float32))
    dataset.FlushCache()
    dataset = None


def test_original_defaults_produce_original_output_locations(tmp_path: Path) -> None:
    pipeline = load_pipeline()
    assert pipeline.VERSION_NAMES == ("v06", "v07")
    assert pipeline.AEP_NAMES == ("01.0p", "05.0p", "10.0p", "20.0p", "50.0p")
    assert pipeline.GRID_TYPES == ("d_HR_Max", "h_HR_Max", "V_Max", "Z0_Max")
    assert [(group.model, group.extents, group.zone_set) for group in pipeline.DEFAULT_GROUPS] == [
        ("FG", ("FG01", "FG02"), "fg"),
        ("FG03", ("FG03",), "fg"),
    ]
    output = pipeline.output_path("v06", "GD01", "01.0p", "d_HR_Max", False, output_root=tmp_path)
    assert output == tmp_path / "01.0p" / "RTIO_v06_GD01_EXG_CCNear_01.0p_MAXIMUM_04M_d_HR_Max.tif"
    fg_output = pipeline.output_path("v06", "FG", "01.0p", "V_Max", True, output_root=tmp_path)
    assert fg_output == tmp_path / "FG" / "01.0p" / "RTIO_v06_FG_EXG_CCNear_01.0p_MAXIMUM_04M_V_Max-clip.tif"


def test_generic_source_group_parser() -> None:
    pipeline = load_pipeline()
    assert pipeline.parse_source_group("main:WEST=WEST01,WEST02").extents == ("WEST01", "WEST02")
    assert pipeline.parse_source_group("fg:FG=FG01,FG02").zone_set == "fg"
    with pytest.raises(ValueError, match="Duplicate"):
        pipeline.parse_source_group("OUT=A,A")


def test_default_group_discovery_and_missing_source_is_error(tmp_path: Path) -> None:
    pipeline = load_pipeline()
    results_root = tmp_path / "results"
    for extent in ("GD01", "FG01", "FG03"):
        folder = results_root / "v06" / extent / "01.0p" / "grids"
        folder.mkdir(parents=True)
        (folder / f"{extent}_d_HR_Max.tif").touch()
    main = pipeline.ZoneCatalog(tmp_path / "main.gpkg", "zones", "Model", {"gd01": "GD01"})
    fg = pipeline.ZoneCatalog(tmp_path / "fg.gpkg", "zones", "Model", {"fg": "FG", "fg03": "FG03"})
    jobs, _ = pipeline.discover_jobs(
        ("v06",), ("01.0p",), ("d_HR_Max",), None, main, fg,
        input_root=results_root, output_root=tmp_path / "out",
    )
    assert [job.model for job in jobs] == ["FG", "FG03", "GD01"]
    combined = next(job for job in jobs if job.model == "FG")
    assert combined.target_aligned
    assert "FG02" in (combined.discovery_error or "")
    assert len(combined.sources) == 1
    assert not next(job for job in jobs if job.model == "FG03").target_aligned
    assert next(job for job in jobs if job.model == "GD01").discovery_error is None


def test_custom_non_fg_composite_discovery(tmp_path: Path) -> None:
    pipeline = load_pipeline()
    results_root = tmp_path / "results"
    for extent in ("A", "B"):
        folder = results_root / "v09" / extent / "02.0p" / "grids"
        folder.mkdir(parents=True)
        (folder / f"grid_{extent}_h_HR_Max.tif").touch()
    main = pipeline.ZoneCatalog(tmp_path / "zones.gpkg", "zones", "Area", {"west": "WEST"})
    jobs, _ = pipeline.discover_jobs(
        ("v09",), ("02.0p",), ("h_HR_Max",), None, main, None,
        input_root=results_root, output_root=tmp_path / "out",
        output_template="{model}_{version}_{aep}_{grid}.tif",
        groups=(pipeline.SourceGroup("WEST", ("A", "B")),),
    )
    assert len(jobs) == 1
    assert jobs[0].output.name == "WEST_v09_02.0p_h_HR_Max.tif"
    assert jobs[0].zones.field_name == "Area"
    assert len(jobs[0].sources) == 2
    assert jobs[0].discovery_error is None


def test_pixel_function_is_maximum_not_last_source_mosaic(tmp_path: Path) -> None:
    if int(gdal.VersionInfo("VERSION_NUM")) < 3120000:
        pytest.skip("GDAL 3.12+ required")
    first, second = tmp_path / "a.tif", tmp_path / "b.tif"
    make_raster(first, np.array([[2, -9999], [5, 7]]), origin_x=0)
    make_raster(second, np.array([[9, 3], [1, 10]]), origin_x=1)
    result = tmp_path / "maximum.vrt"
    build_vrt(
        [first, second], result, source_nodata=-9999.0, vrt_nodata=-9999.0,
        pixel_function="max", resolution="highest", strict=True,
    )
    dataset = gdal.Open(str(result))
    assert dataset is not None
    np.testing.assert_array_equal(
        dataset.ReadAsArray(), np.array([[2, 9, 3], [5, 7, 10]], dtype=np.float32)
    )
    assert dataset.GetGeoTransform() == (0.0, 1.0, 0.0, 2.0, 0.0, -1.0)
    dataset = None


def test_reject_known_different_source_crs(tmp_path: Path) -> None:
    pipeline = load_pipeline()
    first, second = tmp_path / "a.tif", tmp_path / "b.tif"
    make_raster(first, np.ones((2, 2)), origin_x=0, epsg=28350)
    make_raster(second, np.ones((2, 2)), origin_x=0, epsg=28351)
    with pytest.raises(RuntimeError, match="CRS differs"):
        pipeline.inspect_sources((first, second))


def test_publish_failure_restores_previous_raster_overview_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = load_pipeline()
    destination = tmp_path / "result.tif"
    destination_overview = pipeline.overview_path(destination)
    temporary = tmp_path / "new.tif"
    temporary_overview = pipeline.overview_path(temporary)
    destination.write_bytes(b"previous-raster")
    destination_overview.write_bytes(b"previous-overview")
    temporary.write_bytes(b"replacement-raster")
    temporary_overview.write_bytes(b"replacement-overview")

    real_replace = pipeline.os.replace

    def fail_overview_publication(source: Path, target: Path) -> None:
        if source == temporary_overview and target == destination_overview:
            msg = "simulated overview rename failure"
            raise PermissionError(msg)
        real_replace(source, target)

    monkeypatch.setattr(pipeline.os, "replace", fail_overview_publication)
    with pytest.raises(PermissionError, match="simulated"):
        pipeline.publish_output(temporary, destination, build_overviews=True)
    assert destination.read_bytes() == b"previous-raster"
    assert destination_overview.read_bytes() == b"previous-overview"
    assert temporary_overview.read_bytes() == b"replacement-overview"



def test_colliding_output_template_is_rejected_before_any_writes(tmp_path: Path) -> None:
    pipeline = load_pipeline()
    root = tmp_path / "results"
    for name in ("GD01", "GD02"):
        folder = root / "v06" / name / "01.0p" / "grids"
        folder.mkdir(parents=True)
        (folder / f"{name}_V_Max.tif").touch()
    catalog = pipeline.ZoneCatalog(
        path=tmp_path / "zones.gpkg",
        layer_name="zones",
        field_name="Model",
        models={"gd01": "GD01", "gd02": "GD02"},
    )
    with pytest.raises(ValueError, match="Multiple maximum-raster jobs target"):
        pipeline.discover_jobs(
            ("v06",), ("01.0p",), ("V_Max",), None, catalog, None,
            input_root=root, output_root=tmp_path / "out",
            output_template="{grid}.tif", groups=(),
        )
    assert not (tmp_path / "out").exists()


def test_shared_vrt_builder_preserves_vsimem_identifier(tmp_path: Path) -> None:
    if int(gdal.VersionInfo("VERSION_NUM")) < 3120000:
        pytest.skip("GDAL 3.12+ required")
    path = tmp_path / "a.tif"
    make_raster(path, np.array([[7, 8], [9, 10]]), origin_x=0)
    vrt_name = "/vsimem/test_maximum_vrt_no_windows_drive.vrt"
    try:
        build_vrt([path], vrt_name, source_nodata=-9999.0, vrt_nodata=-9999.0, pixel_function="max")
        dataset = gdal.Open(vrt_name)
        assert dataset is not None
        np.testing.assert_array_equal(
            dataset.ReadAsArray(), np.array([[7, 8], [9, 10]], dtype=np.float32),
        )
        dataset = None
    finally:
        gdal.Unlink(vrt_name)


def test_local_crs_assumption_is_not_copied_from_zone_crs(tmp_path: Path) -> None:
    from osgeo import ogr

    pipeline = load_pipeline()
    maximum = gdal.GetDriverByName("MEM").Create("", 2, 2, 1, gdal.GDT_Float32)
    assert maximum is not None
    maximum.SetGeoTransform((500000.0, 1.0, 0.0, 7600002.0, 0.0, -1.0))
    legacy = osr.SpatialReference()
    legacy.SetLocalCS("GDA94_MG")
    maximum.SetProjection(legacy.ExportToWkt())

    source_crs = osr.SpatialReference()
    source_crs.ImportFromEPSG(28350)
    zone_crs = osr.SpatialReference()
    zone_crs.ImportFromEPSG(28351)
    source_crs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    zone_crs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in (
        (500000, 7600000), (500002, 7600000),
        (500002, 7600002), (500000, 7600002), (500000, 7600000),
    ):
        ring.AddPoint_2D(x, y)
    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)
    polygon.Transform(osr.CoordinateTransformation(source_crs, zone_crs))
    path = tmp_path / "zones.gpkg"
    data = ogr.GetDriverByName("GPKG").CreateDataSource(str(path))
    assert data is not None
    layer = data.CreateLayer("zones", srs=zone_crs, geom_type=ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("Model", ogr.OFTString))
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetField("Model", "GD01")
    feature.SetGeometry(polygon)
    assert layer.CreateFeature(feature) == 0
    feature = None
    data = None

    job = pipeline.Job(
        version="v06", model="GD01", aep="01.0p", grid_type="V_Max",
        sources=(tmp_path / "source.tif",), output=tmp_path / "result.tif",
        zones=pipeline.ZoneCatalog(path, "zones", "Model", {"gd01": "GD01"}),
    )
    bounds, assumed_wkt = pipeline.clip_bounds_and_crs(maximum, job)
    assert assumed_wkt is not None
    actual_crs = osr.SpatialReference()
    actual_crs.ImportFromWkt(assumed_wkt)
    assert actual_crs.IsSame(source_crs)
    assert not actual_crs.IsSame(zone_crs)
    assert bounds == (500000.0, 7600000.0, 500002.0, 7600002.0)
    maximum = None
