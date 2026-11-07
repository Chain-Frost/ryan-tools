"""TUFLOW terrain source precedence and GDAL reprojection tests."""

from pathlib import Path

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin

from ryan_library.orchestrators.tuflow.initialiser.model_terrain import set_model_terrain


def _terrain(path: Path, epsg: int) -> None:
    transform = from_origin(115.0, -31.0, 0.0003, 0.0003) if epsg == 4326 else from_origin(300000, 6500000, 30, 30)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=10,
        width=10,
        count=1,
        crs=CRS.from_epsg(epsg),
        transform=transform,
        dtype="float32",
        nodata=-9999.0,
    ) as output:
        output.write(np.full((10, 10), 123, dtype="float32"), 1)


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    model = project / "model"
    model.mkdir(parents=True)
    (model / "GEOM_01.tgc").write_text(
        "Set Zpts == 2000\n! Read GRID Zpts == ..\\terrain\\YOUR_DEM.tif\n",
        encoding="utf-8",
    )
    return project


def test_tuflow_reads_low_priority_first_and_highest_last(tmp_path: Path) -> None:
    project = _project(tmp_path)
    lidar = tmp_path / "lidar.tif"
    srtm = tmp_path / "srtm.tif"
    _terrain(lidar, 28350)
    _terrain(srtm, 28350)
    set_model_terrain(project_dir=project, sources=[lidar, srtm], target_crs=CRS.from_epsg(28350))
    text = (project / "model" / "GEOM_01.tgc").read_text(encoding="utf-8")
    assert text.index(str(srtm.resolve())) < text.index(str(lidar.resolve()))
    assert "priority 2" in text
    assert "priority 1" in text
    assert "YOUR_DEM" not in text


def test_geographic_fallback_is_reprojected_for_tuflow(tmp_path: Path) -> None:
    project = _project(tmp_path)
    lidar = tmp_path / "lidar.tif"
    srtm = tmp_path / "geographic.tif"
    _terrain(lidar, 28350)
    _terrain(srtm, 4326)
    set_model_terrain(project_dir=project, sources=[lidar, srtm], target_crs=CRS.from_epsg(28350))
    prepared = project / "terrain" / "prepared" / "terrain_priority_002_reprojected.tif"
    assert prepared.exists()
    with rasterio.open(prepared) as raster:
        assert raster.crs == CRS.from_epsg(28350)
        assert raster.count == 1
    text = (project / "model" / "GEOM_01.tgc").read_text(encoding="utf-8")
    assert text.index("terrain_priority_002_reprojected") < text.index(str(lidar.resolve()))
