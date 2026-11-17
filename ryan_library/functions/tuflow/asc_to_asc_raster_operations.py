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
import time
from collections.abc import Generator, Iterable
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import uuid4

import numpy as np
import numpy.typing as npt
import rasterio
from rasterio.enums import MaskFlags
from rasterio.windows import Window

from ..gdal.raster_processing import geotiff_creation_options
from .raster_performance import RasterExecutionOptions, RasterOperationTimings

type NodataPolicy = Literal["require_all", "zero", "exclude"]
type MeanValueMethod = Literal["closest_source", "arithmetic", "asc_to_asc"]
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

    def read_masks(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.uint8]: ...

    def write(self, data: npt.NDArray[np.generic], band_index: int, *, window: object | None = None) -> None: ...

    def close(self) -> None: ...


def _open_raster(path: Path, mode: str = "r", profile: dict[str, object] | None = None) -> _RasterDataset:
    if profile is None:
        return cast(
            "_RasterDataset",
            rasterio.open(path, mode),
        )
    return cast(
        "_RasterDataset",
        rasterio.open(
            path,
            mode,
            **cast("dict[str, Any]", profile),
        ),
    )


def _parse_creation_options(extra_args: list[str] | None) -> dict[str, object]:
    """Translate repeated ``-co NAME=VALUE`` arguments into Rasterio profile entries."""
    if not extra_args:
        return {}
    if len(extra_args) % 2:
        msg = "Creation options must use repeated '-co NAME=VALUE' pairs"
        raise ValueError(msg)

    options: dict[str, object] = {}
    for flag, expression in zip(extra_args[::2], extra_args[1::2], strict=True):
        if flag.lower() != "-co" or "=" not in expression:
            msg = f"Invalid creation option: {flag} {expression}"
            raise ValueError(msg)
        key, value = expression.split("=", maxsplit=1)
        if not key or not value:
            msg = f"Invalid creation option: {expression}"
            raise ValueError(msg)
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
        msg = "At least one input raster is required"
        raise ValueError(msg)
    reference: _RasterDataset = datasets[0]
    if reference.count < 1:
        msg = f"Input raster has no bands: {paths[0]}"
        raise ValueError(msg)

    for path, dataset in zip(paths[1:], datasets[1:], strict=True):
        if dataset.count < 1:
            msg = f"Input raster has no bands: {path}"
            raise ValueError(msg)
        if (dataset.width, dataset.height) != (reference.width, reference.height):
            msg = f"Raster dimensions do not match the first input: {path}"
            raise ValueError(msg)
        if dataset.transform != reference.transform:
            msg = f"Raster transform does not match the first input: {path}"
            raise ValueError(msg)
        if dataset.crs != reference.crs:
            msg = f"Raster CRS does not match the first input: {path}"
            raise ValueError(msg)


def _creation_option_arguments(options: list[str]) -> list[str]:
    """Convert GDAL ``NAME=VALUE`` options to repeated ``-co`` arguments."""
    return [item for option in options for item in ("-co", option)]


def _tuflow_creation_profile(reference: _RasterDataset, *, threads: str = "1") -> dict[str, object]:
    """Apply lossless GeoTIFF creation options explicitly: dataset.meta omits compression."""
    profile: dict[str, object] = reference.meta.copy()
    if str(profile.get("driver", "")).casefold() == "gtiff":
        profile.update(
            _parse_creation_options(
                _creation_option_arguments(geotiff_creation_options(profile="tuflow", threads=threads))
            )
        )
    return profile


def _source_index_dtype(maximum_source_id: int) -> str:
    """Use the smallest unsigned GeoTIFF type that represents every 1-based source ID."""
    if maximum_source_id <= 255:
        return "uint8"
    if maximum_source_id <= 65535:
        return "uint16"
    return "uint32"


def _output_profile(
    reference: _RasterDataset, extra_args: list[str] | None, *, threads: str = "1"
) -> tuple[dict[str, object], float]:
    profile = _tuflow_creation_profile(reference, threads=threads)
    profile.update(_parse_creation_options(extra_args))
    profile["count"] = 1
    profile["dtype"] = "float64" if reference.dtypes[0].lower() == "float64" else "float32"
    source_nodata: float | int | None = reference.nodata
    output_nodata = float(source_nodata) if source_nodata is not None and np.isfinite(source_nodata) else -9999.0
    profile["nodata"] = output_nodata
    return profile, output_nodata


def _open_inputs(stack: ExitStack, input_files: list[str]) -> tuple[list[Path], list[_RasterDataset]]:
    paths: list[Path] = [Path(value).absolute() for value in input_files]
    if not paths:
        msg = "At least one input raster is required"
        raise ValueError(msg)
    missing: list[Path] = [path for path in paths if not path.is_file()]
    if missing:
        msg = f"Input raster does not exist: {missing[0]}"
        raise FileNotFoundError(msg)
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
        Path(source_output_file).absolute()
        if source_output_file is not None
        else output_path.with_name(f"{output_path.stem}_src{output_path.suffix}")
    )
    legend_path = (
        Path(source_legend_file).absolute()
        if source_legend_file is not None
        else output_path.with_name(f"{output_path.stem}_src_legend.csv")
    )
    if source_path == output_path or (
        source_path.exists() and output_path.exists() and source_path.samefile(output_path)
    ):
        msg = "Source raster path must differ from the value raster path"
        raise ValueError(msg)
    return source_path, legend_path


def source_output_paths(
    output_file: str,
    source_output_file: str | None = None,
    source_legend_file: str | None = None,
) -> tuple[Path, Path]:
    """Return the source raster and legend paths for a value raster."""
    return _source_output_paths(Path(output_file).absolute(), source_output_file, source_legend_file)


def _write_source_legend(legend_path: Path, input_paths: list[Path], source_counts: npt.NDArray[np.int64]) -> None:
    """Atomically write source IDs, original paths, and selected cell counts."""
    if source_counts.shape != (len(input_paths) + 1,):
        msg = "Source counts must contain one bin per input plus NoData source ID 0"
        raise ValueError(msg)
    temporary_path = _temporary_output(legend_path)
    try:
        temporary_path.parent.mkdir(parents=True, exist_ok=True)
        with temporary_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["source_id", "source_file", "cell_count"])
            writer.writerows(
                (source_id, str(path), int(source_counts[source_id]))
                for source_id, path in enumerate(input_paths, start=1)
            )
        _replace_output(temporary_path, legend_path)
    finally:
        temporary_path.unlink(missing_ok=True)


@contextmanager
def _raster_stack(options: RasterExecutionOptions, timings: RasterOperationTimings) -> Generator[ExitStack]:
    """Keep final TIFF compression/flush time separate from individual writes."""
    stack = ExitStack()
    try:
        stack.enter_context(rasterio.Env(**cast("dict[str, Any]", options.rasterio_env_options())))
        yield stack
    finally:
        started = time.perf_counter()
        try:
            stack.close()
        finally:
            timings.close_seconds += time.perf_counter() - started


def _block_shape(dataset: _RasterDataset) -> tuple[int, int]:
    shapes = cast("list[tuple[int, int]] | None", getattr(dataset, "block_shapes", None))
    if shapes:
        return max(1, shapes[0][0]), max(1, shapes[0][1])
    return 1, dataset.width


def _record_inputs(
    datasets: list[_RasterDataset], options: RasterExecutionOptions, timings: RasterOperationTimings
) -> None:
    timings.width = datasets[0].width
    timings.height = datasets[0].height
    timings.input_count = len(datasets)
    timings.block_shape = _block_shape(datasets[0])
    timings.gdal_cache_bytes = options.gdal_cache_bytes
    timings.gdal_threads = options.threads


def _needs_independent_mask(dataset: _RasterDataset) -> bool:
    """Only skip masks whose flags prove that the numeric validity test covers them."""
    flags = cast("tuple[list[MaskFlags], ...] | None", getattr(dataset, "mask_flag_enums", None))
    if not flags:
        return True
    band_flags = set(flags[0])
    return band_flags not in ({MaskFlags.all_valid}, {MaskFlags.nodata})


def _read_values_and_validity(
    dataset: _RasterDataset,
    window: object,
    timings: RasterOperationTimings,
    *,
    independent_mask: bool,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.bool_]]:
    started = time.perf_counter()
    values = np.asarray(dataset.read(1, window=window), dtype=np.float64)
    timings.read_seconds += time.perf_counter() - started
    started = time.perf_counter()
    valid = _valid_mask(values, dataset.nodata)
    if independent_mask:
        valid &= dataset.read_masks(1, window=window) != 0
    timings.mask_seconds += time.perf_counter() - started
    return values, valid


def _nested_originals(
    nested_source_files: list[str], original_input_groups: list[list[str]]
) -> tuple[list[Path], list[int]]:
    if not nested_source_files:
        msg = "At least one nested source raster is required"
        raise ValueError(msg)
    if len(nested_source_files) != len(original_input_groups):
        msg = "Nested source rasters and original input groups must have equal lengths"
        raise ValueError(msg)
    if any(not group for group in original_input_groups):
        msg = "Every nested source raster must have at least one original input"
        raise ValueError(msg)
    original_paths = [Path(value).absolute() for group in original_input_groups for value in group]
    missing = [path for path in original_paths if not path.is_file()]
    if missing:
        msg = f"Original source raster does not exist: {missing[0]}"
        raise FileNotFoundError(msg)
    if len(original_paths) > np.iinfo(np.int32).max:
        msg = "Too many original rasters for a 32-bit source-ID raster"
        raise ValueError(msg)
    offsets: list[int] = []
    running_offset = 0
    for group in original_input_groups:
        offsets.append(running_offset)
        running_offset += len(group)
    return original_paths, offsets


def _flatten_source_window(
    selected_groups: npt.NDArray[np.integer[Any]],
    nested_sources: list[_RasterDataset],
    original_input_groups: list[list[str]],
    group_offsets: list[int],
    window: object,
    timings: RasterOperationTimings,
) -> npt.NDArray[np.int32]:
    """Compose IDs in memory, retaining the legacy zero-for-invalid-local-ID rule."""
    started = time.perf_counter()
    read_before = timings.read_seconds
    flattened_ids = np.zeros(selected_groups.shape, dtype=np.int32)
    for group_index, (nested_source, originals, offset) in enumerate(
        zip(nested_sources, original_input_groups, group_offsets, strict=True), start=1
    ):
        group_cells = selected_groups == group_index
        if not np.any(group_cells):
            continue
        read_started = time.perf_counter()
        local_ids = np.asarray(nested_source.read(1, window=window), dtype=np.int64)
        timings.read_seconds += time.perf_counter() - read_started
        selected_cells = group_cells & (local_ids >= 1) & (local_ids <= len(originals))
        flattened_ids[selected_cells] = np.asarray(offset + local_ids[selected_cells], dtype=np.int32)
    timings.provenance_seconds += time.perf_counter() - started - (timings.read_seconds - read_before)
    return flattened_ids


def flatten_nested_source_provenance(
    *,
    output_file: str,
    nested_source_files: list[str],
    original_input_groups: list[list[str]],
    source_output_file: str | None = None,
    source_legend_file: str | None = None,
    execution_options: RasterExecutionOptions | None = None,
    timings: RasterOperationTimings | None = None,
) -> None:
    """Replace an intermediate source mapping with original-input source IDs.

    ``compute_stat`` initially records which intermediate input supplied each
    output cell. Each intermediate input has its own source raster whose local
    IDs identify the original rasters in the corresponding input group. This
    function composes those two mappings into one 1-based source raster and a
    legend that points directly to the original rasters.
    """
    options = execution_options or RasterExecutionOptions()
    measured = timings if timings is not None else RasterOperationTimings()
    operation_started = time.perf_counter()
    source_path, legend_path = source_output_paths(output_file, source_output_file, source_legend_file)
    temporary_source_path: Path = _temporary_output(source_path)
    original_paths, group_offsets = _nested_originals(nested_source_files, original_input_groups)
    source_counts = np.zeros(len(original_paths) + 1, dtype=np.int64)
    try:
        with _raster_stack(options, measured) as stack:
            _, datasets = _open_inputs(stack, [str(source_path), *nested_source_files])
            _record_inputs(datasets, options, measured)
            selector, *nested_sources = datasets
            # Flattening creates a fresh TIFF: selector.meta does not retain
            # its DEFLATE compression settings, so reapply the shared profile.
            profile = _tuflow_creation_profile(selector, threads=options.threads_text)
            profile["count"] = 1
            profile["dtype"] = _source_index_dtype(len(original_paths))
            profile["nodata"] = 0
            destination = stack.enter_context(closing(_open_raster(temporary_source_path, "w", profile)))

            for _, window in _chunk_windows(selector, memory_limit_mb=options.chunk_mb, bytes_per_pixel=64):
                measured.windows += 1
                started = time.perf_counter()
                selected_groups = np.asarray(selector.read(1, window=window), dtype=np.int64)
                measured.read_seconds += time.perf_counter() - started
                flattened_ids = _flatten_source_window(
                    selected_groups, nested_sources, original_input_groups, group_offsets, window, measured
                )
                started = time.perf_counter()
                destination.write(flattened_ids, 1, window=window)
                measured.write_seconds += time.perf_counter() - started
                started = time.perf_counter()
                source_counts += np.bincount(flattened_ids.ravel(), minlength=source_counts.size)
                measured.source_seconds += time.perf_counter() - started
        started = time.perf_counter()
        _replace_output(temporary_source_path, source_path)
        _write_source_legend(legend_path, original_paths, source_counts)
        measured.write_seconds += time.perf_counter() - started
    finally:
        temporary_source_path.unlink(missing_ok=True)
        measured.total_seconds += time.perf_counter() - operation_started


def _chunk_windows(
    dataset: _RasterDataset,
    num_datasets: int = 1,
    memory_limit_mb: int = 256,
    *,
    bytes_per_pixel: int | None = None,
) -> Iterable[tuple[tuple[int, int], object]]:
    """Prefer full-width block rows; split tiled rows when needed to bound arrays.

    Output storage remains the conservative striped TUFLOW profile. Processing
    windows follow input blocks where the workspace budget permits; an unusually
    large single source block may require sub-block windows. GDAL's separately
    budgeted cache retains decoded blocks across those windows.
    """
    pixel_bytes = bytes_per_pixel if bytes_per_pixel is not None else 96 + 17 * num_datasets
    max_pixels = max(1, memory_limit_mb * 1024 * 1024 // pixel_bytes)
    block_rows, block_cols = _block_shape(dataset)
    width, height = dataset.width, dataset.height
    full_width_rows = max_pixels // width
    if full_width_rows >= min(block_rows, height):
        chunk_cols = width
        chunk_rows = max(block_rows, full_width_rows // block_rows * block_rows)
    else:
        chunk_rows = min(block_rows, height, max_pixels)
        available_cols = max(1, max_pixels // chunk_rows)
        chunk_cols = min(width, max(block_cols, available_cols // block_cols * block_cols))
        if chunk_cols * chunk_rows > max_pixels:
            chunk_cols = min(width, available_cols)
    for row_off in range(0, height, chunk_rows):
        row_count = min(chunk_rows, height - row_off)
        for col_off in range(0, width, chunk_cols):
            col_count = min(chunk_cols, width - col_off)
            yield ((col_off, row_off), Window(col_off, row_off, col_count, row_count))


def compute_max(
    input_files: list[str],
    output_file: str,
    extra_args: list[str] | None = None,
    *,
    execution_options: RasterExecutionOptions | None = None,
    timings: RasterOperationTimings | None = None,
) -> None:
    """Write the cell-wise maximum of one or more aligned rasters."""
    output_path: Path = Path(output_file).absolute()
    temporary_path: Path = _temporary_output(output_path)
    options = execution_options or RasterExecutionOptions()
    measured = timings if timings is not None else RasterOperationTimings()
    operation_started = time.perf_counter()
    try:
        with _raster_stack(options, measured) as stack:
            _, datasets = _open_inputs(stack, input_files)
            _record_inputs(datasets, options, measured)
            profile, output_nodata = _output_profile(datasets[0], extra_args, threads=options.threads_text)
            destination: _RasterDataset = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            for _, window in _chunk_windows(datasets[0], memory_limit_mb=options.chunk_mb, bytes_per_pixel=64):
                measured.windows += 1
                maximum = None
                any_valid = None
                for dataset in datasets:
                    data, valid = _read_values_and_validity(dataset, window, measured, independent_mask=False)
                    started = time.perf_counter()
                    np.copyto(data, -np.inf, where=~valid)
                    if maximum is None or any_valid is None:
                        maximum = data
                        any_valid = valid
                    else:
                        np.maximum(maximum, data, out=maximum)
                        np.logical_or(any_valid, valid, out=any_valid)
                    measured.calculate_seconds += time.perf_counter() - started
                if maximum is None or any_valid is None:
                    msg = "No raster values were read"
                    raise RuntimeError(msg)
                started = time.perf_counter()
                maximum[~any_valid] = output_nodata
                measured.calculate_seconds += time.perf_counter() - started
                started = time.perf_counter()
                destination.write(maximum, 1, window=window)
                measured.write_seconds += time.perf_counter() - started
        started = time.perf_counter()
        _replace_output(temporary_path, output_path)
        measured.write_seconds += time.perf_counter() - started
    finally:
        temporary_path.unlink(missing_ok=True)
        measured.total_seconds += time.perf_counter() - operation_started


def compute_diff(
    file1: str,
    file2: str,
    output_file: str,
    change: bool = False,
    nowetdry: bool = False,
    combine_wd: bool = False,
    extra_args: list[str] | None = None,
    *,
    execution_options: RasterExecutionOptions | None = None,
    timings: RasterOperationTimings | None = None,
) -> None:
    """Write ``file1 - file2`` using ASC_to_ASC difference conventions.

    By default the numeric difference is written only where both inputs are
    valid and a separate ``_wd`` raster identifies wet-to-dry as ``-99`` and
    dry-to-wet as ``+99``. ``change`` treats NoData as zero. ``nowetdry``
    suppresses the separate classification raster. ``combine_wd`` is a
    Python-only extension that places the classifications in the main output.
    """
    output_path: Path = Path(output_file).absolute()
    wet_dry_path: Path = output_path.with_name(f"{output_path.stem}_wd{output_path.suffix}")
    temporary_output: Path = _temporary_output(output_path)
    temporary_wet_dry: Path = _temporary_output(wet_dry_path)
    options = execution_options or RasterExecutionOptions()
    measured = timings if timings is not None else RasterOperationTimings()
    operation_started = time.perf_counter()
    try:
        with _raster_stack(options, measured) as stack:
            _, datasets = _open_inputs(stack, [file1, file2])
            _record_inputs(datasets, options, measured)
            first, second = datasets
            profile, output_nodata = _output_profile(first, extra_args, threads=options.threads_text)
            destination: _RasterDataset = stack.enter_context(closing(_open_raster(temporary_output, "w", profile)))
            wet_dry_destination: _RasterDataset | None = (
                None
                if change or nowetdry or combine_wd
                else stack.enter_context(closing(_open_raster(temporary_wet_dry, "w", profile)))
            )

            for _, window in _chunk_windows(first, memory_limit_mb=options.chunk_mb, bytes_per_pixel=96):
                measured.windows += 1
                first_data, first_valid = _read_values_and_validity(first, window, measured, independent_mask=False)
                second_data, second_valid = _read_values_and_validity(second, window, measured, independent_mask=False)
                started = time.perf_counter()
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

                measured.calculate_seconds += time.perf_counter() - started
                started = time.perf_counter()
                destination.write(difference, 1, window=window)
                measured.write_seconds += time.perf_counter() - started
                if wet_dry_destination is not None:
                    started = time.perf_counter()
                    wet_dry = np.full(first_data.shape, output_nodata, dtype=np.float64)
                    wet_dry[~first_valid & second_valid] = -99.0
                    wet_dry[first_valid & ~second_valid] = 99.0
                    measured.calculate_seconds += time.perf_counter() - started
                    started = time.perf_counter()
                    wet_dry_destination.write(wet_dry, 1, window=window)
                    measured.write_seconds += time.perf_counter() - started
        started = time.perf_counter()
        _replace_output(temporary_output, output_path)
        if not change and not nowetdry and not combine_wd:
            _replace_output(temporary_wet_dry, wet_dry_path)
        measured.write_seconds += time.perf_counter() - started
    finally:
        temporary_output.unlink(missing_ok=True)
        temporary_wet_dry.unlink(missing_ok=True)
        measured.total_seconds += time.perf_counter() - operation_started


def _read_stat_values(
    datasets: list[_RasterDataset],
    mask_reads: list[bool],
    window: object,
    nodata_policy: NodataPolicy,
    *,
    median: bool,
    timings: RasterOperationTimings,
) -> tuple[
    npt.NDArray[np.float64],
    npt.NDArray[np.bool_] | None,
    npt.NDArray[np.bool_],
    int | npt.NDArray[np.intp],
]:
    """Fill one contiguous stack directly, without list/stack/adjusted-stack copies."""
    raster_window = cast("Window", window)
    shape = (int(raster_window.height), int(raster_window.width))
    started = time.perf_counter()
    values = np.empty((len(datasets), *shape), dtype=np.float64)
    eligibility = None if nodata_policy == "zero" else np.empty(values.shape, dtype=np.bool_)
    counts = np.zeros(shape, dtype=np.intp)
    timings.calculate_seconds += time.perf_counter() - started
    for index, (dataset, independent_mask) in enumerate(zip(datasets, mask_reads, strict=True)):
        data, valid = _read_values_and_validity(dataset, window, timings, independent_mask=independent_mask)
        started = time.perf_counter()
        values[index] = data
        np.copyto(values[index], np.inf if median and nodata_policy != "zero" else 0.0, where=~valid)
        counts += valid
        if eligibility is not None:
            eligibility[index] = valid
        timings.calculate_seconds += time.perf_counter() - started
    started = time.perf_counter()
    output_valid = counts == len(datasets) if nodata_policy == "require_all" else counts > 0
    divisor = counts if nodata_policy == "exclude" else len(datasets)
    timings.calculate_seconds += time.perf_counter() - started
    return values, eligibility, output_valid, divisor


def _sum_mean_values(
    values: npt.NDArray[np.float64], reference: _RasterDataset, window: object
) -> npt.NDArray[np.float64]:
    """Retain the former reduction order even at new single-pixel tile corners.

    NumPy uses a different summation path for a contiguous one-pixel reduction.
    The former full-width windows used the constants below, so changing window
    geometry alone must not change Float64 means or their selected source IDs.
    """
    legacy_rows = max(1, ((2048 * 1024 * 1024) // (200 * values.shape[0])) // reference.width)
    all_legacy_singletons = reference.width == 1 and legacy_rows == 1
    legacy_single_row = reference.height - 1 if reference.width == 1 and reference.height % legacy_rows == 1 else -1
    raster_window = cast("Window", window)
    row_offset = int(raster_window.row_off)
    if values.shape[1:] == (1, 1) and not all_legacy_singletons and row_offset != legacy_single_row:
        padded = np.zeros((values.shape[0], 1, 2), dtype=np.float64)
        padded[:, :, :1] = values
        return np.sum(padded, axis=0, dtype=np.float64)[:, :1].copy()
    result = np.sum(values, axis=0, dtype=np.float64)
    if all_legacy_singletons:
        for local_row in range(values.shape[1]):
            result[local_row, 0] = np.sum(values[:, local_row, 0], dtype=np.float64)
        return result
    if row_offset <= legacy_single_row < row_offset + values.shape[1]:
        local_row = legacy_single_row - row_offset
        result[local_row, 0] = np.sum(values[:, local_row, 0], dtype=np.float64)
    return result


def _mean_source_values(
    values: npt.NDArray[np.float64],
    eligibility: npt.NDArray[np.bool_] | None,
    arithmetic_mean: npt.NDArray[np.float64],
    method: MeanValueMethod,
    *,
    write_source: bool,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.int32] | None]:
    """Select mean sources with pixel-sized buffers and stable first-source ties."""
    source_ids = np.ones(arithmetic_mean.shape, dtype=np.int32) if write_source else None
    best_value = np.full(arithmetic_mean.shape, np.inf if method == "asc_to_asc" else -np.inf, dtype=np.float64)
    best_distance = None if method == "asc_to_asc" else np.full(arithmetic_mean.shape, np.inf, dtype=np.float64)
    for index, data in enumerate(values):
        distance = None
        if best_distance is None:
            update = (data >= arithmetic_mean) & (data < best_value)
        else:
            distance = np.abs(data - arithmetic_mean)
            update = (distance < best_distance) | ((distance == best_distance) & (data > best_value))
        if eligibility is not None:
            update &= eligibility[index]
        if best_distance is None:
            np.copyto(best_value, data, where=update)
        elif distance is not None:
            # The old np.max chose the last equal value's representation
            # (signed zero), while np.argmax attributed it to the first input.
            value_tie = (distance == best_distance) & (data == best_value)
            if eligibility is not None:
                value_tie &= eligibility[index]
            np.copyto(best_value, data, where=update | value_tie)
        if best_distance is not None and distance is not None:
            np.copyto(best_distance, distance, where=update)
        if source_ids is not None:
            np.copyto(source_ids, index + 1, where=update)
    return best_value, source_ids


def _mean_or_median_window(
    datasets: list[_RasterDataset],
    mask_reads: list[bool],
    window: object,
    statistic_type: str,
    nodata_policy: NodataPolicy,
    mean_value_method: MeanValueMethod,
    *,
    write_source: bool,
    timings: RasterOperationTimings,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.bool_], npt.NDArray[np.int32] | None]:
    median = statistic_type == "median"
    values, eligibility, output_valid, counts = _read_stat_values(
        datasets, mask_reads, window, nodata_policy, median=median, timings=timings
    )
    started = time.perf_counter()
    if median:
        sorted_indexes = np.argsort(values, axis=0, kind="stable")
        upper_indexes = np.broadcast_to(np.asarray(counts // 2, dtype=np.int64), values.shape[1:])
        source_indexes = np.take_along_axis(sorted_indexes, upper_indexes[np.newaxis, ...], axis=0)[0]
        statistic = np.take_along_axis(values, source_indexes[np.newaxis, ...], axis=0)[0]
        timings.calculate_seconds += time.perf_counter() - started
        started = time.perf_counter()
        source_ids = np.asarray(source_indexes + 1, dtype=np.int32) if write_source else None
    else:
        valid_sum = _sum_mean_values(values, datasets[0], window)
        statistic = np.zeros(valid_sum.shape, dtype=np.float64)
        np.divide(valid_sum, counts, out=statistic, where=output_valid)
        timings.calculate_seconds += time.perf_counter() - started
        started = time.perf_counter()
        source_ids = None
        if write_source or mean_value_method == "closest_source":
            selected_values, source_ids = _mean_source_values(
                values, eligibility, statistic, mean_value_method, write_source=write_source
            )
            if mean_value_method == "closest_source":
                statistic = selected_values
    timings.source_seconds += time.perf_counter() - started
    return statistic, output_valid, source_ids


def _extrema_window(
    datasets: list[_RasterDataset],
    mask_reads: list[bool],
    window: object,
    statistic_type: str,
    nodata_policy: NodataPolicy,
    *,
    write_source: bool,
    timings: RasterOperationTimings,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.bool_], npt.NDArray[np.int32] | None]:
    """Reduce extrema while retaining first-value ties and valid-zero provenance."""
    statistic: npt.NDArray[np.float64] | None = None
    output_valid: npt.NDArray[np.bool_] | None = None
    selected_valid: npt.NDArray[np.bool_] | None = None
    source_ids: npt.NDArray[np.int32] | None = None
    invalid_value = 0.0 if nodata_policy == "zero" else (-np.inf if statistic_type == "max" else np.inf)
    for index, (dataset, independent_mask) in enumerate(zip(datasets, mask_reads, strict=True)):
        data, valid = _read_values_and_validity(dataset, window, timings, independent_mask=independent_mask)
        started = time.perf_counter()
        np.copyto(data, invalid_value, where=~valid)
        if statistic is None or output_valid is None:
            statistic = data
            output_valid = valid.copy()
            selected_valid = valid
            source_ids = np.ones(data.shape, dtype=np.int32) if write_source else None
            timings.calculate_seconds += time.perf_counter() - started
            continue
        better = data > statistic if statistic_type == "max" else data < statistic
        np.copyto(statistic, data, where=better)
        if nodata_policy == "require_all":
            output_valid &= valid
        else:
            output_valid |= valid
        timings.calculate_seconds += time.perf_counter() - started
        if source_ids is not None:
            started = time.perf_counter()
            # A valid-zero tie changes only the ID, not the value selected
            # earlier (including its signed zero), matching the old two passes.
            update = better
            if nodata_policy == "zero" and selected_valid is not None:
                update = better | (valid & ~selected_valid & (data == statistic))
                np.copyto(selected_valid, valid, where=update)
            np.copyto(source_ids, index + 1, where=update)
            timings.source_seconds += time.perf_counter() - started
    if statistic is None or output_valid is None:
        msg = "No raster values were read"
        raise RuntimeError(msg)
    return statistic, output_valid, source_ids


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
    *,
    execution_options: RasterExecutionOptions | None = None,
    timings: RasterOperationTimings | None = None,
    nested_source_files: list[str] | None = None,
    original_input_groups: list[list[str]] | None = None,
) -> None:
    """Write a supported cell-wise ASC_to_ASC-style statistic.

    ``require_all`` writes NoData unless every input is valid. ``zero`` includes
    invalid inputs as zeroes where at least one input is valid, preserving
    NoData where all inputs are invalid (outside the model extent).
    ``exclude`` calculates from valid inputs and writes NoData where none
    are valid.

    Median uses ASC_to_ASC's upper-median convention: for an even number of
    contributing values, index ``n // 2`` in ascending order is returned.

    For mean, the default ``closest_source`` method first calculates the
    arithmetic mean under the selected NoData policy, then returns the nearest
    policy-adjusted contributing value. Equal-distance ties select the higher
    value. Choose ``arithmetic`` to write the numeric mean itself while using
    the same closest-source rule, or ``asc_to_asc`` to write the numeric mean
    while selecting the lowest contributing source value at or above it, as
    ASC_to_ASC does. Under the ``zero`` policy, substituted NoData zeroes are
    eligible source values.

    With ``write_source=True``, a 1-based source-ID raster named
    ``<output_stem>_src`` and a ``<output_stem>_src_legend.csv`` mapping are
    written beside the value raster. IDs follow the caller-provided input
    order. Equal values select the first matching input. A source cell is zero
    (the source raster's NoData) wherever the value output is NoData. Source
    output is disabled by default.

    Only mean, median, minimum and maximum statistics are supported.

    For a maximum over duration statistics, ``nested_source_files`` and
    ``original_input_groups`` compose the original TP IDs while writing the
    source raster. This retains the intermediate value rasters and their
    stored precision, but avoids a second complete source-raster rewrite.
    """
    normalized_stat: str = stat_type.lower().removeprefix("-stat")
    if normalized_stat not in {"mean", "median", "min", "max"}:
        msg = f"Unsupported stat_type: {stat_type}"
        raise ValueError(msg)
    if nodata_policy not in {"require_all", "zero", "exclude"}:
        msg = f"Unsupported nodata_policy: {nodata_policy}"
        raise ValueError(msg)
    if mean_value_method not in {"closest_source", "arithmetic", "asc_to_asc"}:
        msg = f"Unsupported mean_value_method: {mean_value_method}"
        raise ValueError(msg)
    if not write_source and (source_output_file is not None or source_legend_file is not None):
        msg = "Source output paths cannot be supplied when write_source is False"
        raise ValueError(msg)
    if (nested_source_files is None) != (original_input_groups is None):
        msg = "Nested source files and original input groups must be supplied together"
        raise ValueError(msg)
    if nested_source_files is not None and (normalized_stat != "max" or not write_source):
        msg = "Nested source provenance requires a maximum statistic with source output enabled"
        raise ValueError(msg)
    if nested_source_files is not None and len(nested_source_files) != len(input_files):
        msg = "Every intermediate value raster must have one nested source raster"
        raise ValueError(msg)

    options = execution_options or RasterExecutionOptions()
    measured = timings if timings is not None else RasterOperationTimings()
    operation_started = time.perf_counter()
    output_path: Path = Path(output_file).absolute()
    temporary_path: Path = _temporary_output(output_path)
    source_path, legend_path = _source_output_paths(output_path, source_output_file, source_legend_file)
    temporary_source_path: Path = _temporary_output(source_path)
    input_paths: list[Path] = []
    try:
        with _raster_stack(options, measured) as stack:
            input_paths, datasets = _open_inputs(stack, input_files)
            if len(datasets) > np.iinfo(np.int32).max:
                msg = "Too many input rasters for a 32-bit source-ID raster"
                raise ValueError(msg)
            nested_sources: list[_RasterDataset] = []
            group_offsets: list[int] = []
            legend_inputs = input_paths
            if nested_source_files is not None and original_input_groups is not None:
                legend_inputs, group_offsets = _nested_originals(nested_source_files, original_input_groups)
                nested_paths, nested_sources = _open_inputs(stack, nested_source_files)
                _validate_inputs([datasets[0], *nested_sources], [input_paths[0], *nested_paths])
            _record_inputs([*datasets, *nested_sources], options, measured)
            profile, output_nodata = _output_profile(datasets[0], extra_args, threads=options.threads_text)
            destination = stack.enter_context(closing(_open_raster(temporary_path, "w", profile)))
            source_destination: _RasterDataset | None = None
            source_counts = np.zeros(len(legend_inputs) + 1, dtype=np.int64)
            if write_source:
                source_profile = profile.copy()
                source_profile["dtype"] = _source_index_dtype(len(legend_inputs))
                source_profile["nodata"] = 0
                source_destination = stack.enter_context(
                    closing(_open_raster(temporary_source_path, "w", source_profile))
                )
            mask_reads = [_needs_independent_mask(dataset) for dataset in datasets]
            pixel_bytes = (
                96
                if normalized_stat in {"max", "min"}
                else 96 + len(datasets) * (17 if normalized_stat == "median" else 9)
            )
            for _, window in _chunk_windows(datasets[0], memory_limit_mb=options.chunk_mb, bytes_per_pixel=pixel_bytes):
                measured.windows += 1
                if normalized_stat in {"max", "min"}:
                    statistic, output_valid, source_ids = _extrema_window(
                        datasets,
                        mask_reads,
                        window,
                        normalized_stat,
                        nodata_policy,
                        write_source=write_source,
                        timings=measured,
                    )
                else:
                    statistic, output_valid, source_ids = _mean_or_median_window(
                        datasets,
                        mask_reads,
                        window,
                        normalized_stat,
                        nodata_policy,
                        mean_value_method,
                        write_source=write_source,
                        timings=measured,
                    )
                started = time.perf_counter()
                statistic[~output_valid] = output_nodata
                measured.calculate_seconds += time.perf_counter() - started
                started = time.perf_counter()
                destination.write(statistic, 1, window=window)
                measured.write_seconds += time.perf_counter() - started
                if source_destination is not None and source_ids is not None:
                    started = time.perf_counter()
                    source_ids[~output_valid] = 0
                    measured.source_seconds += time.perf_counter() - started
                    if nested_sources and original_input_groups is not None:
                        source_ids = _flatten_source_window(
                            source_ids, nested_sources, original_input_groups, group_offsets, window, measured
                        )
                    started = time.perf_counter()
                    source_destination.write(source_ids, 1, window=window)
                    measured.write_seconds += time.perf_counter() - started
                    started = time.perf_counter()
                    source_counts += np.bincount(source_ids.ravel(), minlength=source_counts.size)
                    measured.source_seconds += time.perf_counter() - started
        started = time.perf_counter()
        _replace_output(temporary_path, output_path)
        if write_source:
            _replace_output(temporary_source_path, source_path)
            _write_source_legend(legend_path, legend_inputs, source_counts)
        measured.write_seconds += time.perf_counter() - started
    finally:
        temporary_path.unlink(missing_ok=True)
        temporary_source_path.unlink(missing_ok=True)
        measured.total_seconds += time.perf_counter() - operation_started
