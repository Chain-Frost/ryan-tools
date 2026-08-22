"""Native raster operations modelled on a supported subset of TUFLOW ASC_to_ASC.

The public functions reproduce a supported subset of ASC_to_ASC operations
without launching ``asc_to_asc.exe``:

* :func:`compute_max` corresponds to ``-max`` but does not create ``_src``.
* :func:`compute_diff` corresponds to ``-dif``/``-diff`` and supports
  ``-change`` and ``-nowetdry`` semantics. ``combine_wd`` is a Python-only
  extension that writes the ``-99``/``+99`` wet/dry classes into the result.
* :func:`compute_stat` supports ``-statMean``, ``-statMedian``, ``-statMin``
  and ``-statMax`` value rasters, source rasters and source legends.

``-statFrac``, ``-statRank`` and ``-statAll`` are intentionally unsupported.
The configurable NoData policies are project-aware Python behaviour and are
not a claim of exact ASC_to_ASC NoData parity.
"""

from __future__ import annotations

import csv
from contextlib import ExitStack, closing
from pathlib import Path
import time
from typing import Iterable, Literal, Protocol, cast
from uuid import uuid4

import numpy as np
from numpy._typing._array_like import NDArray
import numpy.typing as npt
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.windows import Window  # pyright: ignore[reportMissingTypeStubs]

type NodataPolicy = Literal["require_all", "zero", "exclude"]
type MeanValueMethod = Literal["closest_source", "arithmetic"]
type StatisticType = Literal["mean", "median", "min", "max"]


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
    reference: _RasterDataset = datasets[0]
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
    profile: dict[str, object] = reference.meta.copy()
    profile.update(_parse_creation_options(extra_args))
    profile["count"] = 1
    profile["dtype"] = "float64" if reference.dtypes[0].lower() == "float64" else "float32"
    source_nodata: float | int | None = reference.nodata
    output_nodata = float(source_nodata) if source_nodata is not None and np.isfinite(source_nodata) else -9999.0
    profile["nodata"] = output_nodata
    return profile, output_nodata


def _open_inputs(stack: ExitStack, input_files: list[str]) -> tuple[list[Path], list[_RasterDataset]]:
    paths: list[Path] = [Path(value).resolve() for value in input_files]
    if not paths:
        raise ValueError("At least one input raster is required")
    missing: list[Path] = [path for path in paths if not path.is_file()]
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


def _source_output_paths(
    output_path: Path,
    source_output_file: str | None,
    source_legend_file: str | None,
) -> tuple[Path, Path]:
    """Return explicit or collision-safe default paths for source outputs."""
    source_path = (
        Path(source_output_file).resolve()
        if source_output_file is not None
        else output_path.with_name(f"{output_path.stem}_src{output_path.suffix}")
    )
    legend_path = (
        Path(source_legend_file).resolve()
        if source_legend_file is not None
        else output_path.with_name(f"{output_path.stem}_src_legend.csv")
    )
    if source_path == output_path:
        raise ValueError("Source raster path must differ from the value raster path")
    return source_path, legend_path


def _write_source_legend(legend_path: Path, input_paths: list[Path]) -> None:
    """Atomically write the 1-based source IDs used by the source raster."""
    temporary_path = _temporary_output(legend_path)
    try:
        temporary_path.parent.mkdir(parents=True, exist_ok=True)
        with temporary_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["source_id", "source_file"])
            writer.writerows((source_id, str(path)) for source_id, path in enumerate(input_paths, start=1))
        _replace_output(temporary_path, legend_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _chunk_windows(
    dataset: _RasterDataset, num_datasets: int = 1, memory_limit_mb: int = 2048
) -> Iterable[tuple[tuple[int, int], object]]:
    """Yield rectangular windows of rows to optimize I/O, safely bounded by a memory limit.

    The peak memory for a statistical operation on N aligned chunks of float64 data is
    conservatively around 200 bytes per pixel per dataset (factoring in float64 arrays,
    valid masks, np.stack allocations, and output arrays).
    """
    bytes_per_pixel_per_dataset = 200
    max_pixels: int = (memory_limit_mb * 1024 * 1024) // (bytes_per_pixel_per_dataset * num_datasets)

    height: int = dataset.height
    width: int = dataset.width
    # Ensure we always read at least 1 row, even if memory limit is tiny
    chunk_rows: int = max(1, max_pixels // width)

    for row_off in range(0, height, chunk_rows):
        row_count: int = min(chunk_rows, height - row_off)
        # Yield a dummy index (0, row_off) to match the signature of block_windows
        yield ((0, row_off), Window(0, row_off, width, row_count))  # pyright: ignore[reportCallIssue]


def compute_max(input_files: list[str], output_file: str, extra_args: list[str] | None = None) -> None:
    """Write the cell-wise maximum of one or more aligned rasters."""
    output_path: Path = Path(output_file).resolve()
    temporary_path: Path = _temporary_output(output_path)
    try:
        with ExitStack() as stack:
            stack.enter_context(rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            _, datasets = _open_inputs(stack, input_files)
            profile, output_nodata = _output_profile(datasets[0], extra_args)
            destination: _RasterDataset = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            for _, window in _chunk_windows(datasets[0], num_datasets=len(datasets)):
                maximum = None
                any_valid = None
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
    combine_wd: bool = False,
    extra_args: list[str] | None = None,
) -> None:
    """Write ``file1 - file2`` using ASC_to_ASC difference conventions.

    By default the numeric difference is written only where both inputs are
    valid and a separate ``_wd`` raster identifies wet-to-dry as ``-99`` and
    dry-to-wet as ``+99``. ``change`` treats NoData as zero. ``nowetdry``
    suppresses the separate classification raster. ``combine_wd`` is a
    Python-only extension that places the classifications in the main output.
    """
    output_path: Path = Path(output_file).resolve()
    wet_dry_path: Path = output_path.with_name(f"{output_path.stem}_wd{output_path.suffix}")
    temporary_output: Path = _temporary_output(output_path)
    temporary_wet_dry: Path = _temporary_output(wet_dry_path)
    try:
        with ExitStack() as stack:
            stack.enter_context(cm=rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            _, datasets = _open_inputs(stack, [file1, file2])
            first, second = datasets
            profile, output_nodata = _output_profile(first, extra_args)
            destination: _RasterDataset = stack.enter_context(closing(_open_raster(temporary_output, "w", profile)))
            wet_dry_destination: None | _RasterDataset = (
                None
                if change or nowetdry or combine_wd
                else stack.enter_context(closing(_open_raster(temporary_wet_dry, "w", profile)))
            )

            for _, window in _chunk_windows(first, num_datasets=len(datasets)):
                first_data = np.asarray(first.read(1, window=window), dtype=np.float64)
                second_data = np.asarray(second.read(1, window=window), dtype=np.float64)
                first_valid: NDArray[np.bool_] = _valid_mask(first_data, first.nodata)
                second_valid: NDArray[np.bool_] = _valid_mask(second_data, second.nodata)

                if change:
                    difference = np.where(first_valid, first_data, 0.0) - np.where(second_valid, second_data, 0.0)
                    difference[~first_valid & ~second_valid] = output_nodata
                else:
                    both_valid = first_valid & second_valid
                    difference = np.full(first_data.shape, output_nodata, dtype=np.float64)
                    difference[both_valid] = first_data[both_valid] - second_data[both_valid]

                if combine_wd and not change and not nowetdry:
                    difference[~first_valid & second_valid] = -99.0
                    difference[first_valid & ~second_valid] = 99.0

                destination.write(difference, 1, window=window)

                if wet_dry_destination is not None:
                    wet_dry = np.full(first_data.shape, output_nodata, dtype=np.float64)
                    wet_dry[~first_valid & second_valid] = -99.0
                    wet_dry[first_valid & ~second_valid] = 99.0
                    wet_dry_destination.write(wet_dry, 1, window=window)

        _replace_output(temporary_output, output_path)
        if not change and not nowetdry and not combine_wd:
            _replace_output(temporary_wet_dry, wet_dry_path)
    finally:
        temporary_output.unlink(missing_ok=True)
        temporary_wet_dry.unlink(missing_ok=True)


def compute_stat(
    stat_type: str,
    input_files: list[str],
    output_file: str,
    extra_args: list[str] | None = None,
    nodata_policy: NodataPolicy = "require_all",
    mean_value_method: MeanValueMethod = "closest_source",
    write_source: bool = False,
    source_output_file: str | None = None,
    source_legend_file: str | None = None,
) -> None:
    """Write a supported cell-wise ASC_to_ASC-style statistic.

    ``require_all`` writes NoData unless every input is valid. ``zero`` includes
    invalid inputs as zeroes. ``exclude`` calculates from the valid inputs and
    writes NoData only where every input is invalid.

    Median uses ASC_to_ASC's upper-median convention: for an even number of
    contributing values, index ``n // 2`` in ascending order is returned.

    For mean, the default ``closest_source`` method first calculates the
    arithmetic mean under the selected NoData policy, then returns the nearest
    policy-adjusted contributing value. Equal-distance ties select the higher
    value. Choose ``arithmetic`` to write the numeric mean itself. Under the
    ``zero`` policy, substituted NoData zeroes are eligible source values.

    With ``write_source=True``, a 1-based source-ID raster named
    ``<output_stem>_src`` and a ``<output_stem>_src_legend.csv`` mapping are
    written beside the value raster. IDs follow the caller-provided input
    order. Equal values select the first matching input. A source cell is zero
    (the source raster's NoData) wherever the value output is NoData. Source
    output is disabled by default.

    Only mean, median, minimum and maximum statistics are supported.
    """
    normalized_stat: str = stat_type.lower().removeprefix("-stat")
    if normalized_stat not in {"mean", "median", "min", "max"}:
        raise ValueError(f"Unsupported stat_type: {stat_type}")
    if nodata_policy not in {"require_all", "zero", "exclude"}:
        raise ValueError(f"Unsupported nodata_policy: {nodata_policy}")
    if mean_value_method not in {"closest_source", "arithmetic"}:
        raise ValueError(f"Unsupported mean_value_method: {mean_value_method}")
    if not write_source and (source_output_file is not None or source_legend_file is not None):
        raise ValueError("Source output paths cannot be supplied when write_source is False")

    output_path: Path = Path(output_file).resolve()
    temporary_path: Path = _temporary_output(output_path)
    source_path, legend_path = _source_output_paths(output_path, source_output_file, source_legend_file)
    temporary_source_path: Path = _temporary_output(source_path)
    input_paths: list[Path] = []
    try:
        with ExitStack() as stack:
            stack.enter_context(rasterio.Env(GDAL_CACHEMAX=512, VSI_CACHE=True))
            input_paths, datasets = _open_inputs(stack, input_files)
            if len(datasets) > np.iinfo(np.int32).max:
                raise ValueError("Too many input rasters for a 32-bit source-ID raster")
            profile, output_nodata = _output_profile(datasets[0], extra_args)
            destination = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            source_destination: _RasterDataset | None = None
            if write_source:
                source_profile = profile.copy()
                source_profile["dtype"] = "int32"
                source_profile["nodata"] = 0
                source_destination = stack.enter_context(
                    closing(_open_raster(temporary_source_path, "w", source_profile))
                )
            for _, window in _chunk_windows(datasets[0], num_datasets=len(datasets)):
                arrays = [np.asarray(dataset.read(1, window=window), dtype=np.float64) for dataset in datasets]
                valid_arrays = [
                    _valid_mask(data, dataset.nodata) for data, dataset in zip(arrays, datasets, strict=True)
                ]
                stack_data = np.stack(arrays, axis=0)
                stack_valid = np.stack(valid_arrays, axis=0)
                if nodata_policy == "require_all":
                    adjusted_data = stack_data
                    eligible = stack_valid
                    output_valid = np.all(stack_valid, axis=0)
                    valid_count = np.full(stack_data.shape[1:], stack_data.shape[0], dtype=np.intp)
                elif nodata_policy == "zero":
                    adjusted_data = np.where(stack_valid, stack_data, 0.0)
                    eligible = np.ones(stack_valid.shape, dtype=np.bool_)
                    output_valid = np.ones(stack_valid.shape[1:], dtype=np.bool_)
                    valid_count = np.full(stack_data.shape[1:], stack_data.shape[0], dtype=np.intp)
                else:
                    adjusted_data = stack_data
                    eligible = stack_valid
                    valid_count = np.sum(stack_valid, axis=0, dtype=np.intp)
                    output_valid = valid_count > 0

                if normalized_stat == "mean":
                    valid_sum = np.sum(np.where(eligible, adjusted_data, 0.0), axis=0, dtype=np.float64)
                    arithmetic_mean = np.zeros(valid_sum.shape, dtype=np.float64)
                    np.divide(valid_sum, valid_count, out=arithmetic_mean, where=output_valid)
                    distances = np.where(eligible, np.abs(adjusted_data - arithmetic_mean), np.inf)
                    minimum_distance = np.min(distances, axis=0)
                    closest = eligible & np.isclose(distances, minimum_distance, rtol=0.0, atol=0.0)
                    closest_value = np.max(np.where(closest, adjusted_data, -np.inf), axis=0)
                    selected = closest & (adjusted_data == closest_value)
                    source_indexes = np.argmax(selected, axis=0)
                    statistic = arithmetic_mean if mean_value_method == "arithmetic" else closest_value
                elif normalized_stat == "max":
                    selectable = np.where(eligible, adjusted_data, -np.inf)
                    source_indexes = np.argmax(selectable, axis=0)
                    statistic = np.take_along_axis(selectable, source_indexes[np.newaxis, ...], axis=0)[0]
                elif normalized_stat == "min":
                    selectable = np.where(eligible, adjusted_data, np.inf)
                    source_indexes = np.argmin(selectable, axis=0)
                    statistic = np.take_along_axis(selectable, source_indexes[np.newaxis, ...], axis=0)[0]
                else:
                    selectable = np.where(eligible, adjusted_data, np.inf)
                    sorted_indexes = np.argsort(selectable, axis=0, kind="stable")
                    upper_indexes = valid_count // 2
                    source_indexes = np.take_along_axis(sorted_indexes, upper_indexes[np.newaxis, ...], axis=0)[0]
                    statistic = np.take_along_axis(selectable, source_indexes[np.newaxis, ...], axis=0)[0]

                result: npt.NDArray[np.float64] = np.asarray(
                    np.where(output_valid, statistic, output_nodata), dtype=np.float64
                )
                destination.write(result, 1, window=window)
                if source_destination is not None:
                    source_ids = np.asarray(np.where(output_valid, source_indexes + 1, 0), dtype=np.int32)
                    source_destination.write(source_ids, 1, window=window)
        _replace_output(temporary_path, output_path)
        if write_source:
            _replace_output(temporary_source_path, source_path)
            _write_source_legend(legend_path, input_paths)
    finally:
        temporary_path.unlink(missing_ok=True)
        temporary_source_path.unlink(missing_ok=True)
