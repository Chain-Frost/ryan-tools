"""Metadata-only raster inspection tests for the deferred TUFLOW bulk stage."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.tuflow.raster_metadata_dependencies import inspect_raster_metadata


def test_raster_metadata_is_read_once_per_unique_path(tmp_path: Path) -> None:
    raster = tmp_path / "rf.tif"
    with rasterio.open(
        raster,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="float32",
        transform=from_origin(100, 200, 1, 1),
        crs="EPSG:28350",
    ) as dataset:
        dataset.write(np.ones((2, 2), dtype=np.float32), 1)

    results, opens = inspect_raster_metadata([raster, raster, raster])
    assert opens == 1
    assert results[raster].status == "present"


def test_raster_metadata_invalid_file_fails_closed(tmp_path: Path) -> None:
    invalid = tmp_path / "rainfall.tif"
    invalid.write_text("not a valid TIFF", encoding="utf-8")
    missing = tmp_path / "missing.tif"

    results, opens = inspect_raster_metadata({invalid, missing})
    assert opens == 2
    assert results[invalid].status == "unresolved"
    assert results[missing].status == "unresolved"
    assert "Cannot inspect raster" in results[invalid].detail


def test_netcdf_container_with_raster_subdataset_is_valid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """NetCDF root datasets can expose bands only through GDAL subdatasets."""
    from typing import Self

    class Dataset:
        def __init__(self, *, bands: int, subdatasets: tuple[str, ...] = ()) -> None:
            self.width = 2 if bands else 0
            self.height = 2 if bands else 0
            self.count = bands
            self.subdatasets = subdatasets

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_arguments: object) -> None:
            pass

    root = tmp_path / "rainfall.nc"
    root.touch()
    calls: list[str] = []

    def fake_open(path: object, mode: str = "r") -> Dataset:
        assert mode == "r"
        calls.append(str(path))
        if str(path) == str(root):
            return Dataset(bands=0, subdatasets=("NETCDF:rainfall.nc:rainfall",))
        return Dataset(bands=1)

    monkeypatch.setattr(rasterio, "open", fake_open)
    results, opens = inspect_raster_metadata((root, root))
    assert results[root].status == "present"
    assert opens == 2
    assert calls == [str(root), "NETCDF:rainfall.nc:rainfall"]
