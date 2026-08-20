from __future__ import annotations

# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]
import pytest

import gdal_stage_storage
from ryan_library.functions.gdal.stage_storage import compute_stage_storage


class _RasterWriter(Protocol):
    def write(self, data: npt.NDArray[np.float32], indexes: int) -> None: ...

    def close(self) -> None: ...


def _change_working_directory_succeeds(*, target_dir: Path) -> bool:
    del target_dir
    return True


def _write_dem(dem_path: Path, data: npt.NDArray[np.float32], crs: str | None = "EPSG:32750") -> None:
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
            crs=crs,
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


@pytest.mark.parametrize(
    ("crs", "message"),
    [(None, "CRS metadata"), ("EPSG:4326", "projected metre-based"), ("EPSG:2277", "metre-based DEM coordinates")],
)
def test_compute_stage_storage_rejects_crs_that_cannot_produce_cubic_metres(
    tmp_path: Path, crs: str | None, message: str
) -> None:
    dem_path = tmp_path / "dem.tif"
    _write_dem(dem_path, np.ones((2, 2), dtype=np.float32), crs)

    with pytest.raises(ValueError, match=message):
        compute_stage_storage(dem_path, levels=[1.0, 2.0])


def test_stage_storage_wrapper_regenerates_a_missing_requested_plot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dem_path = tmp_path / "dem.tif"
    _write_dem(dem_path, np.array([[1.0, 2.0], [2.0, 3.0]], dtype=np.float32))
    csv_path = tmp_path / "volumes_dem.csv"
    png_path = tmp_path / "volumes_dem.png"
    csv_path.write_text("existing CSV from a no-plot run", encoding="utf-8")
    monkeypatch.setattr(gdal_stage_storage, "change_working_directory", _change_working_directory_succeeds)
    args = gdal_stage_storage._parse_cli_arguments([str(tmp_path), "--step", "1"])

    assert gdal_stage_storage.main(args) == 0
    assert csv_path.read_text(encoding="utf-8").startswith("Level (m),Volume (m3)")
    assert png_path.is_file()
