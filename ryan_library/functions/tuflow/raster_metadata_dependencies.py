"""Deduplicated read-only raster metadata validation for TUFLOW input grids.

Open each distinct raster once after deferred run contexts are collected.
Do not read pixel arrays or use metadata checks as a substitute for input
existence, model-data parity or a complete rainfall index.
"""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

import rasterio
from rasterio.errors import RasterioError

from .dependency_index import DependencyResult


def inspect_raster_metadata(paths: Collection[Path]) -> tuple[dict[Path, DependencyResult], int]:
    """Return per-path raster header status with one open per unique source.

    Rasterio/GDAL reads headers only. Invalid datasets stay unresolved; absence
    remains the responsibility of the shared filesystem inventory.
    """
    results: dict[Path, DependencyResult] = {}
    opened = 0
    for path in set(paths):
        try:
            opened += 1
            with rasterio.open(path, mode="r") as dataset:
                if dataset.width >= 1 and dataset.height >= 1 and dataset.count >= 1:
                    results[path] = DependencyResult("present")
                elif dataset.subdatasets:
                    # GDAL NetCDF container datasets may expose no bands of
                    # their own. Their variables are described as subdatasets
                    # instead; inspect one usable subdataset's header before
                    # declaring the source valid. Never read pixel arrays.
                    valid = False
                    for subdataset in dataset.subdatasets:
                        opened += 1
                        with rasterio.open(subdataset, mode="r") as source:
                            if source.width >= 1 and source.height >= 1 and source.count >= 1:
                                valid = True
                                break
                    results[path] = (
                        DependencyResult("present")
                        if valid
                        else DependencyResult("unresolved", "Raster subdatasets have no usable grid dimensions or bands")
                    )
                else:
                    results[path] = DependencyResult("unresolved", "Raster has no usable grid dimensions or bands")
        except (OSError, RasterioError, ValueError) as exc:
            results[path] = DependencyResult("unresolved", f"Cannot inspect raster metadata: {exc}")
    return results, opened
