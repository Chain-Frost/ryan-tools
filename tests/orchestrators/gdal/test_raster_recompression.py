"""Synthetic coverage for safe GeoTIFF recompression."""

# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.gdal.raster_recompression import (
    inspect_geotiff,
    verify_rasters_exact,
)
from ryan_library.orchestrators.gdal.raster_recompression import (
    audit_geotiffs,
    recompress_geotiffs,
)


def _write_raster(path: Path, values: np.ndarray, *, dtype: str = "float32") -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[-1],
        height=values.shape[-2],
        count=1,
        dtype=dtype,
        crs="EPSG:7850",
        transform=from_origin(100.0, 200.0, 2.0, 2.0),
        nodata=-9999 if dtype == "float32" else 65535,
    ) as dataset:
        dataset.write(values.astype(dtype), 1)
        dataset.update_tags(project="synthetic")


def test_auto_stream_replaces_float32_exactly(tmp_path: Path) -> None:
    source = tmp_path / "depth.tif"
    values = np.arange(600, dtype=np.float32).reshape(20, 30)
    _write_raster(source, values)
    before = tmp_path / "before.tif"
    before.write_bytes(source.read_bytes())

    results = recompress_geotiffs(tmp_path, recursive=False)

    result = next(item for item in results if item.path.name == "depth.tif")
    assert result.status == "replaced"
    assert result.strategy == "stream"
    assert inspect_geotiff(source).compression == "DEFLATE"
    assert inspect_geotiff(source).overview_count == 0
    assert verify_rasters_exact(before, source) is None


def test_auto_uses_translate_and_local_stage_for_other_dtype(tmp_path: Path) -> None:
    source_dir = tmp_path / "rasters"
    stage = tmp_path / "stage"
    source_dir.mkdir()
    source = source_dir / "classes.tif"
    _write_raster(
        source, np.arange(100, dtype=np.uint16).reshape(10, 10), dtype="uint16"
    )

    results = recompress_geotiffs(source_dir, staging_directory=stage)

    assert results[0].status == "replaced"
    assert results[0].strategy == "translate"
    assert inspect_geotiff(source).compression == "DEFLATE"
    assert not tuple(stage.iterdir())


def test_audit_is_read_only_and_compressed_files_are_skipped(tmp_path: Path) -> None:
    source = tmp_path / "depth.tif"
    _write_raster(source, np.ones((5, 5), dtype=np.float32))
    original = source.read_bytes()

    inspections = audit_geotiffs(tmp_path)

    assert inspections[0].compression == "NONE"
    assert source.read_bytes() == original
    assert recompress_geotiffs(tmp_path)[0].status == "replaced"
    assert recompress_geotiffs(tmp_path)[0].status == "skipped"


def test_linked_tiff_is_rejected_without_changing_target(tmp_path: Path) -> None:
    source = tmp_path / "source.tif"
    _write_raster(source, np.ones((5, 5), dtype=np.float32))
    alias_dir = tmp_path / "links"
    alias_dir.mkdir()
    alias = alias_dir / "alias.tif"
    try:
        alias.symlink_to(source)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"File symlinks unavailable: {exc}")
    original = source.read_bytes()

    result = recompress_geotiffs(alias_dir, recursive=False)[0]

    assert result.status == "failed"
    assert "symlink" in result.reason
    assert source.read_bytes() == original
    assert alias.is_symlink()


def test_raster_sidecar_is_rejected_without_replacing_tiff(tmp_path: Path) -> None:
    source = tmp_path / "sidecar.tif"
    _write_raster(source, np.ones((5, 5), dtype=np.float32))
    with rasterio.open(source, "r+") as dataset:
        with rasterio.Env(GDAL_PAM_ENABLED="YES"):
            pass
    sidecar = tmp_path / "sidecar.tif.aux.xml"
    sidecar.write_text("<PAMDataset><Metadata><MDI key='a'>b</MDI></Metadata></PAMDataset>")
    original = source.read_bytes()

    result = recompress_geotiffs(tmp_path, recursive=False)[0]

    assert result.status == "failed"
    assert "sidecars" in result.reason
    assert source.read_bytes() == original
    assert sidecar.exists()
