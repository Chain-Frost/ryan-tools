"""Experimental, local raster calculations for the adjacent ASC-to-ASC candidates."""

from __future__ import annotations

from contextlib import ExitStack, closing
from pathlib import Path
import time
from typing import Iterable, Protocol, cast
from uuid import uuid4

import numpy as np
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.windows import Window  # pyright: ignore[reportMissingTypeStubs]


class _RasterDataset(Protocol):
    width: int
    height: int
    count: int
    transform: object
    crs: object | None
    nodata: float | int | None
    dtypes: tuple[str, ...]
    meta: dict[str, object]

    def block_windows(self, band_index: int) -> Iterable[tuple[tuple[int, int], object]]: ...

    def read(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.generic]: ...

    def write(self, data: npt.NDArray[np.generic], band_index: int, *, window: object | None = None) -> None: ...

    def close(self) -> None: ...


def _open_raster(path: Path, mode: str = "r", profile: dict[str, object] | None = None) -> _RasterDataset:
    if profile is None:
        return cast(
            _RasterDataset,
            rasterio.open(path, mode),  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        )
    return cast(
        _RasterDataset,
        rasterio.open(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType, reportArgumentType]
            path, mode, **profile  # pyright: ignore[reportArgumentType]
        ),
    )


def _parse_creation_options(extra_args: list[str] | None) -> dict[str, object]:
    """Translate repeated ``-co NAME=VALUE`` arguments into Rasterio profile entries."""
    if not extra_args:
        return {}
    if len(extra_args) % 2:
        raise ValueError("Creation options must use repeated '-co NAME=VALUE' pairs")

    options: dict[str, object] = {}
    for flag, expression in zip(extra_args[::2], extra_args[1::2], strict=True):
        if flag.lower() != "-co" or "=" not in expression:
            raise ValueError(f"Invalid creation option: {flag} {expression}")
        key, value = expression.split("=", maxsplit=1)
        if not key or not value:
            raise ValueError(f"Invalid creation option: {expression}")
        options[key.lower()] = value
    return options


def _temporary_output(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.stem}.{uuid4().hex}.tmp{output_path.suffix}")


def _valid_mask(data: npt.NDArray[np.float64], nodata: float | int | None) -> npt.NDArray[np.bool_]:
    valid = np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        valid &= data != float(nodata)
    return valid


def _validate_inputs(datasets: list[_RasterDataset], paths: list[Path]) -> None:
    if not datasets:
        raise ValueError("At least one input raster is required")
    reference = datasets[0]
    if reference.count < 1:
        raise ValueError(f"Input raster has no bands: {paths[0]}")

    for path, dataset in zip(paths[1:], datasets[1:], strict=True):
        if dataset.count < 1:
            raise ValueError(f"Input raster has no bands: {path}")
        if (dataset.width, dataset.height) != (reference.width, reference.height):
            raise ValueError(f"Raster dimensions do not match the first input: {path}")
        if dataset.transform != reference.transform:
            raise ValueError(f"Raster transform does not match the first input: {path}")
        if dataset.crs != reference.crs:
            raise ValueError(f"Raster CRS does not match the first input: {path}")


def _output_profile(reference: _RasterDataset, extra_args: list[str] | None) -> tuple[dict[str, object], float]:
    profile = reference.meta.copy()
    profile.update(_parse_creation_options(extra_args))
    profile["count"] = 1
    profile["dtype"] = "float64" if reference.dtypes[0].lower() == "float64" else "float32"
    source_nodata = reference.nodata
    output_nodata = float(source_nodata) if source_nodata is not None and np.isfinite(source_nodata) else -9999.0
    profile["nodata"] = output_nodata
    return profile, output_nodata


def _open_inputs(stack: ExitStack, input_files: list[str]) -> tuple[list[Path], list[_RasterDataset]]:
    paths = [Path(value).resolve() for value in input_files]
    if not paths:
        raise ValueError("At least one input raster is required")
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Input raster does not exist: {missing[0]}")
    datasets = [stack.enter_context(closing(_open_raster(path))) for path in paths]
    _validate_inputs(datasets, paths)
    return paths, datasets


def _replace_output(temporary_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # On Windows, background indexers or antivirus may briefly lock a recently closed file.
    for attempt in range(5):
        try:
            temporary_path.replace(output_path)
            return
        except PermissionError:
            if attempt < 4:
                time.sleep(0.5)
            else:
                raise


def _chunk_windows(
    dataset: _RasterDataset, num_datasets: int = 1, memory_limit_mb: int = 2048
) -> Iterable[tuple[tuple[int, int], object]]:
    """Yield rectangular windows of rows to optimize I/O, safely bounded by a memory limit.
    
    The peak memory for a statistical operation on N aligned chunks of float64 data is
    conservatively around 200 bytes per pixel per dataset (factoring in float64 arrays,
    valid masks, np.stack allocations, and output arrays).
    """
    bytes_per_pixel_per_dataset = 200
    max_pixels = (memory_limit_mb * 1024 * 1024) // (bytes_per_pixel_per_dataset * num_datasets)
    
    height = dataset.height
    width = dataset.width
    # Ensure we always read at least 1 row, even if memory limit is tiny
    chunk_rows = max(1, max_pixels // width)
    
    for row_off in range(0, height, chunk_rows):
        row_count = min(chunk_rows, height - row_off)
        # Yield a dummy index (0, row_off) to match the signature of block_windows
        yield ((0, row_off), Window(0, row_off, width, row_count))


def compute_max(input_files: list[str], output_file: str, extra_args: list[str] | None = None) -> None:
    """Write the cell-wise maximum of one or more aligned rasters."""
    output_path = Path(output_file).resolve()
    temporary_path = _temporary_output(output_path)
    try:
        with ExitStack() as stack:
            stack.enter_context(rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            _, datasets = _open_inputs(stack, input_files)
            profile, output_nodata = _output_profile(datasets[0], extra_args)
            destination = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            for _, window in _chunk_windows(datasets[0], num_datasets=len(datasets)):
                maximum: npt.NDArray[np.float64] | None = None
                any_valid: npt.NDArray[np.bool_] | None = None
                for dataset in datasets:
                    data = np.asarray(dataset.read(1, window=window), dtype=np.float64)
                    valid = _valid_mask(data, dataset.nodata)
                    values = np.where(valid, data, -np.inf)
                    maximum = values if maximum is None else np.maximum(maximum, values)
                    any_valid = valid if any_valid is None else any_valid | valid
                if maximum is None or any_valid is None:
                    raise RuntimeError("No raster values were read")
                result = np.where(any_valid, maximum, output_nodata)
                destination.write(result, 1, window=window)
        _replace_output(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def compute_diff(
    file1: str,
    file2: str,
    output_file: str,
    change: bool = False,
    nowetdry: bool = False,
    extra_args: list[str] | None = None,
) -> None:
    """Write ``file1 - file2`` for two aligned rasters and optionally a wet/dry classification raster."""
    output_path = Path(output_file).resolve()
    wet_dry_path = output_path.with_name(f"{output_path.stem}_wd{output_path.suffix}")
    temporary_output = _temporary_output(output_path)
    temporary_wet_dry = _temporary_output(wet_dry_path)
    try:
        with ExitStack() as stack:
            stack.enter_context(rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            _, datasets = _open_inputs(stack, [file1, file2])
            first, second = datasets
            profile, output_nodata = _output_profile(first, extra_args)
            destination = stack.enter_context(closing(_open_raster(temporary_output, "w", profile)))
            wet_dry_destination = (
                None
                if change or nowetdry
                else stack.enter_context(closing(_open_raster(temporary_wet_dry, "w", profile)))
            )

            for _, window in _chunk_windows(first, num_datasets=len(datasets)):
                first_data = np.asarray(first.read(1, window=window), dtype=np.float64)
                second_data = np.asarray(second.read(1, window=window), dtype=np.float64)
                first_valid = _valid_mask(first_data, first.nodata)
                second_valid = _valid_mask(second_data, second.nodata)

                if change:
                    difference = np.where(first_valid, first_data, 0.0) - np.where(second_valid, second_data, 0.0)
                    difference[~first_valid & ~second_valid] = output_nodata
                else:
                    both_valid = first_valid & second_valid
                    difference = np.full(first_data.shape, output_nodata, dtype=np.float64)
                    difference[both_valid] = first_data[both_valid] - second_data[both_valid]
                destination.write(difference, 1, window=window)

                if wet_dry_destination is not None:
                    wet_dry = np.full(first_data.shape, output_nodata, dtype=np.float64)
                    wet_dry[~first_valid & second_valid] = -99.0
                    wet_dry[first_valid & ~second_valid] = 99.0
                    wet_dry_destination.write(wet_dry, 1, window=window)

        _replace_output(temporary_output, output_path)
        if not change and not nowetdry:
            _replace_output(temporary_wet_dry, wet_dry_path)
    finally:
        temporary_output.unlink(missing_ok=True)
        temporary_wet_dry.unlink(missing_ok=True)


def compute_stat(stat_type: str, input_files: list[str], output_file: str, extra_args: list[str] | None = None) -> None:
    """Write a cell-wise statistic for aligned rasters, requiring every input cell to be valid."""
    normalized_stat = stat_type.lower().removeprefix("-stat")
    operations = {
        "mean": np.mean,
        "median": np.median,
        "min": np.min,
        "max": np.max,
    }
    operation = operations.get(normalized_stat)
    if operation is None:
        raise ValueError(f"Unsupported stat_type: {stat_type}")

    output_path = Path(output_file).resolve()
    temporary_path = _temporary_output(output_path)
    try:
        with ExitStack() as stack:
            stack.enter_context(rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            _, datasets = _open_inputs(stack, input_files)
            profile, output_nodata = _output_profile(datasets[0], extra_args)
            destination = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            for _, window in _chunk_windows(datasets[0], num_datasets=len(datasets)):
                arrays = [np.asarray(dataset.read(1, window=window), dtype=np.float64) for dataset in datasets]
                valid_arrays = [
                    _valid_mask(data, dataset.nodata) for data, dataset in zip(arrays, datasets, strict=True)
                ]
                stack_data = np.stack(arrays, axis=0)
                all_valid = np.all(np.stack(valid_arrays, axis=0), axis=0)
                statistic = operation(stack_data, axis=0)
                result = np.where(all_valid, statistic, output_nodata)
                destination.write(result, 1, window=window)
        _replace_output(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
