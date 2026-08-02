"""Reproject a raster onto square cells while retaining its array dimensions.

Edit ``SOURCE_RASTER`` and ``CELL_SIZE`` near the top, then run
``python square_raster_cells.py`` from a directory where the source path
resolves. The output is named ``<stem>_square_<size>m.tif`` beside the source and
uses nearest-neighbour resampling for every band.

The width and height are deliberately retained, so changing cell size changes
the covered extent. Review the printed resolutions, sizes, CRS, nodata value,
and spatial extent before using the result.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import Affine
from rasterio.warp import Resampling, reproject

from ryan_library.functions.gdal.raster_processing import read_raster_band

SOURCE_RASTER = Path("model/grid/LeveeDesign.tif")
CELL_SIZE = 0.5


def cell_size_label(cell_size: float) -> str:
    return f"{cell_size:g}".replace(".", "p")


def output_path_for(src_path: Path, cell_size: float) -> Path:
    suffix: str = f"_square_{cell_size_label(cell_size)}m"
    return src_path.with_name(f"{src_path.stem}{suffix}{src_path.suffix}")


def square_raster_cells(src_path: Path, dst_path: Path, cell_size: float) -> None:
    with rasterio.open(src_path) as src:
        dst_transform = Affine(
            cell_size,
            0.0,
            src.bounds.left,
            0.0,
            -cell_size,
            src.bounds.top,
        )

        profile = src.profile.copy()
        profile.update(
            transform=dst_transform,
            width=src.width,
            height=src.height,
            crs=src.crs,
            dtype=src.dtypes[0],
            count=src.count,
            nodata=src.nodata,
        )

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(dst_path, "w", **profile) as dst:
            for band_index in range(1, src.count + 1):
                src_data = read_raster_band(raster=src_path, band=band_index)
                fill_value = src.nodata if src.nodata is not None else 0
                dst_data = np.full(src_data.shape, fill_value, dtype=src_data.dtype)

                reproject(
                    source=src_data,
                    destination=dst_data,
                    src_transform=src.transform,
                    src_crs=src.crs,
                    src_nodata=src.nodata,
                    dst_transform=dst_transform,
                    dst_crs=src.crs,
                    dst_nodata=src.nodata,
                    resampling=Resampling.nearest,
                )
                dst.write(dst_data, band_index)


def main() -> None:
    output_raster: Path = output_path_for(src_path=SOURCE_RASTER, cell_size=CELL_SIZE)

    square_raster_cells(src_path=SOURCE_RASTER, dst_path=output_raster, cell_size=CELL_SIZE)

    with rasterio.open(SOURCE_RASTER) as src, rasterio.open(output_raster) as dst:
        print(f"Created: {output_raster}")
        print(f"Source resolution: {src.res}")
        print(f"Output resolution: {dst.res}")
        print(f"Source size: {src.width} x {src.height}")
        print(f"Output size: {dst.width} x {dst.height}")
        print(f"Output CRS: {dst.crs}")
        print(f"Output nodata: {dst.nodata}")


if __name__ == "__main__":
    main()
