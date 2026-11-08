"""Create TUFLOW terrain import commands with explicit elevation precedence.

Inputs are declared highest-to-lowest priority. TUFLOW's sequential
Read Grid Zpts layering is low-to-high priority (last takes precedence).
Source rasters with different CRS are individually GDAL-warped for TUFLOW,
preserving each source's own raster resolution rather than resampling all
hydraulic topography to the coarser watershed analysis grid.
"""

from collections.abc import Sequence
from pathlib import Path

import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.shutil import copy as raster_copy
from rasterio.vrt import WarpedVRT


def set_model_terrain(
    *,
    project_dir: Path,
    sources: Sequence[Path],
    target_crs: CRS,
) -> None:
    """Populate GEOM_01.tgc without disturbing original topography sources."""
    if not sources:
        msg = "No terrain sources supplied"
        raise ValueError(msg)
    tgc = project_dir / "model" / "GEOM_01.tgc"
    template = tgc.read_text(encoding="utf-8")
    marker = "! Read GRID Zpts == ..\\terrain\\YOUR_DEM.tif"
    if marker not in template:
        msg = "Expected TUFLOW geometry template terrain marker is missing"
        raise ValueError(msg)
    prepared_dir = project_dir / "terrain" / "prepared"
    commands: list[str] = []

    for rank, path in reversed(tuple(enumerate(sources, start=1))):
        source = path.absolute()
        with rasterio.open(source) as dem:
            if dem.count != 1 or dem.crs is None:
                msg = f"Cannot use invalid source DEM in TUFLOW geometry: {source}"
                raise ValueError(msg)
            if dem.crs == target_crs:
                input_path = str(source)
            else:
                prepared_dir.mkdir(parents=True, exist_ok=True)
                name = f"terrain_priority_{rank:03d}_reprojected.tif"
                output = prepared_dir / name
                if output.exists():
                    raise FileExistsError(output)
                with WarpedVRT(
                    dem,
                    crs=target_crs,
                    resampling=Resampling.bilinear,
                    nodata=-9999.0,
                ) as vrt:
                    raster_copy(vrt, output, driver="GTiff", compress="deflate", BIGTIFF="IF_SAFER")
                with rasterio.open(output) as reprojected:
                    if reprojected.crs != target_crs or reprojected.count != 1:
                        msg = f"Invalid reprojected source DEM: {output}"
                        raise ValueError(msg)
                input_path = str(Path("..") / "terrain" / "prepared" / name)
        if "|" in input_path or "\n" in input_path:
            msg = "Terrain paths cannot contain TUFLOW field separators or newlines"
            raise ValueError(msg)
        commands.append(f"Read Grid Zpts == {input_path} ! terrain priority {rank} (1 = highest)")

    tgc.write_text(template.replace(marker, "\n".join(commands)), encoding="utf-8")
