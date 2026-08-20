from __future__ import annotations

from pathlib import Path
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]
import pytest

from ryan_library.functions.gdal.stage_storage import compute_stage_storage


class _RasterWriter(Protocol):
    def write(self, data: npt.NDArray[np.float32], indexes: int) -> None: ...

    def close(self) -> None: ...


def _write_dem(dem_path: Path, data: npt.NDArray[np.float32]) -> None:
    destination = cast(
        _RasterWriter,
        rasterio.open(  # pyright: ignore[reportUnknownMemberType]
            dem_path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs="EPSG:32750",
            transform=from_origin(0.0, 10.0, 1.0, 1.0),
            nodata=-9999.0,
        ),
    )
    try:
        destination.write(data, 1)
    finally:
        destination.close()


def test_compute_stage_storage_for_known_cells(tmp_path: Path) -> None:
    dem_path = tmp_path / "bowl.tif"
    data = np.full((4, 10), -9999.0, dtype=np.float32)
    data[0, 0:5] = 1.0
    data[1, 0:10] = 2.0
    data[2:4, 0:10] = 3.0
    _write_dem(dem_path, data)

    volumes = compute_stage_storage(dem_path, levels=[0.5, 1.5, 2.5, 3.5])

    assert volumes == {0.5: 0.0, 1.5: 2.5, 2.5: 12.5, 3.5: 37.5}


def test_compute_stage_storage_rejects_unsorted_or_duplicate_levels(tmp_path: Path) -> None:
    dem_path = tmp_path / "dem.tif"
    _write_dem(dem_path, np.ones((2, 2), dtype=np.float32))

    with pytest.raises(ValueError, match="strictly increasing"):
        compute_stage_storage(dem_path, levels=[1.0, 1.0])
