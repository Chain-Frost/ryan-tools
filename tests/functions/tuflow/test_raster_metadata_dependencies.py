"""Metadata-only raster inspection tests for the deferred TUFLOW bulk stage."""

from __future__ import annotations

from pathlib import Path

import numpy as np
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
