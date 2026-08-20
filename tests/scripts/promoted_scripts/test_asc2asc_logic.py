from __future__ import annotations

from pathlib import Path
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]
import pytest

from asc2asc_py import (
    DEFAULT_INPUT_FILES,
    DEFAULT_OUTPUT_FILE,
    _parse_cli_arguments,  # pyright: ignore[reportPrivateUsage]
    resolve_configuration,
)
from ryan_library.functions.tuflow.local_raster_calc import compute_diff, compute_max, compute_stat


class _RasterWriter(Protocol):
    def write(self, data: npt.NDArray[np.float32], indexes: int) -> None: ...

    def close(self) -> None: ...


class _RasterReader(Protocol):
    def read(self, indexes: int) -> npt.NDArray[np.float32]: ...

    def close(self) -> None: ...


def _write_raster(path: Path, data: npt.NDArray[np.float32]) -> None:
    destination = cast(
        _RasterWriter,
        rasterio.open(  # pyright: ignore[reportUnknownMemberType]
            path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs="EPSG:4326",
            transform=from_origin(150.0, -30.0, 1.0, 1.0),
            nodata=-9999.0,
        ),
    )
    try:
        destination.write(data, 1)
    finally:
        destination.close()


def _read_raster(path: Path) -> npt.NDArray[np.float32]:
    source = cast(_RasterReader, rasterio.open(path))  # pyright: ignore[reportUnknownMemberType]
    try:
        return source.read(1).astype(np.float32)
    finally:
        source.close()


def test_compute_max_and_mean(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    maximum = tmp_path / "maximum.tif"
    mean = tmp_path / "mean.tif"
    _write_raster(first, np.array([[1.0, 4.0], [-9999.0, 8.0]], dtype=np.float32))
    _write_raster(second, np.array([[3.0, 2.0], [5.0, -9999.0]], dtype=np.float32))

    compute_max([str(first), str(second)], str(maximum))
    compute_stat("mean", [str(first), str(second)], str(mean))

    np.testing.assert_array_equal(_read_raster(maximum), np.array([[3.0, 4.0], [5.0, 8.0]], dtype=np.float32))
    np.testing.assert_array_equal(_read_raster(mean), np.array([[2.0, 3.0], [-9999.0, -9999.0]], dtype=np.float32))


def test_compute_diff_preserves_shared_valid_cells(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "difference.tif"
    _write_raster(first, np.array([[5.0, -9999.0]], dtype=np.float32))
    _write_raster(second, np.array([[2.0, 4.0]], dtype=np.float32))

    compute_diff(str(first), str(second), str(output), nowetdry=True)

    np.testing.assert_array_equal(_read_raster(output), np.array([[3.0, -9999.0]], dtype=np.float32))


def test_compute_diff_rejects_misaligned_rasters_without_final_output(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "difference.tif"
    _write_raster(first, np.ones((2, 2), dtype=np.float32))
    _write_raster(second, np.ones((3, 2), dtype=np.float32))

    with pytest.raises(ValueError, match="dimensions"):
        compute_diff(str(first), str(second), str(output))

    assert not output.exists()


def test_wrapper_defaults_are_used_and_cli_values_override_them() -> None:
    defaults = resolve_configuration(_parse_cli_arguments([]))
    overrides = resolve_configuration(
        _parse_cli_arguments(["-max", "-out", "maximum.tif", "one.tif", "two.tif", "--no-pause"])
    )

    assert defaults.input_files == [str(path) for path in DEFAULT_INPUT_FILES]
    assert defaults.output_file == str(DEFAULT_OUTPUT_FILE)
    assert defaults.operation == "diff"
    assert overrides.input_files == ["one.tif", "two.tif"]
    assert overrides.output_file == "maximum.tif"
    assert overrides.operation == "max"
    assert not overrides.pause
