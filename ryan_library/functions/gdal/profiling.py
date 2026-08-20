"""Functions for sampling raster data along geometries."""

from __future__ import annotations

# Rasterio does not currently provide complete type information.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from math import ceil, isfinite
from pathlib import Path
from typing import Literal

from loguru import logger
import numpy as np
from numpy.typing import NDArray
import rasterio  # pyright: ignore[reportMissingTypeStubs]
import rasterio.windows  # pyright: ignore[reportMissingTypeStubs]
from shapely.geometry import LineString

type FloatArray = NDArray[np.float64]
type BoolArray = NDArray[np.bool_]
type SamplingMethod = Literal["bilinear", "bilinear_valid", "bilinear_masked", "nearest"]


class RasterSamplingError(RuntimeError):
    """Raised when a raster cannot be sampled along a supplied line."""


def _nearest_sample(band: FloatArray, rows: FloatArray, cols: FloatArray) -> FloatArray:
    """Return nearest-cell values for fractional centre-based indices."""
    row_indices = np.floor(rows + 0.5).astype(np.int64)
    col_indices = np.floor(cols + 0.5).astype(np.int64)
    height, width = band.shape
    out_of_bounds = (row_indices < 0) | (row_indices >= height) | (col_indices < 0) | (col_indices >= width)
    clipped_rows = np.clip(row_indices, 0, height - 1)
    clipped_cols = np.clip(col_indices, 0, width - 1)
    result = np.asarray(band[clipped_rows, clipped_cols], dtype=np.float64)
    result[out_of_bounds] = np.nan
    return result


def _containing_cells_are_valid(band: FloatArray, rows: FloatArray, cols: FloatArray) -> BoolArray:
    """Return whether each query lies inside a valid containing raster cell."""
    row_indices = np.floor(rows + 0.5).astype(np.int64)
    col_indices = np.floor(cols + 0.5).astype(np.int64)
    height, width = band.shape
    in_bounds = (row_indices >= 0) & (row_indices < height) & (col_indices >= 0) & (col_indices < width)
    clipped_rows = np.clip(row_indices, 0, height - 1)
    clipped_cols = np.clip(col_indices, 0, width - 1)
    return np.asarray(in_bounds & ~np.isnan(band[clipped_rows, clipped_cols]), dtype=np.bool_)


def _bilinear_sample(
    band: FloatArray,
    rows: FloatArray,
    cols: FloatArray,
    *,
    use_valid_neighbours: bool,
) -> FloatArray:
    """Return bilinear values for fractional centre-based array indices."""
    row0 = np.floor(rows).astype(np.int64)
    col0 = np.floor(cols).astype(np.int64)
    row1 = row0 + 1
    col1 = col0 + 1
    row_fraction = rows - row0
    col_fraction = cols - col0

    height, width = band.shape
    row0_clipped = np.clip(row0, 0, height - 1)
    row1_clipped = np.clip(row1, 0, height - 1)
    col0_clipped = np.clip(col0, 0, width - 1)
    col1_clipped = np.clip(col1, 0, width - 1)
    values = np.stack(
        (
            band[row0_clipped, col0_clipped],
            band[row0_clipped, col1_clipped],
            band[row1_clipped, col0_clipped],
            band[row1_clipped, col1_clipped],
        ),
        axis=0,
    )
    weights = np.stack(
        (
            (1.0 - row_fraction) * (1.0 - col_fraction),
            (1.0 - row_fraction) * col_fraction,
            row_fraction * (1.0 - col_fraction),
            row_fraction * col_fraction,
        ),
        axis=0,
    )
    in_bounds = np.stack(
        (
            (row0 >= 0) & (row0 < height) & (col0 >= 0) & (col0 < width),
            (row0 >= 0) & (row0 < height) & (col1 >= 0) & (col1 < width),
            (row1 >= 0) & (row1 < height) & (col0 >= 0) & (col0 < width),
            (row1 >= 0) & (row1 < height) & (col1 >= 0) & (col1 < width),
        ),
        axis=0,
    )
    positive_weight = weights > np.finfo(np.float64).eps
    valid = in_bounds & ~np.isnan(values)

    if use_valid_neighbours:
        usable_weights = np.where(valid, weights, 0.0)
        weight_sum = usable_weights.sum(axis=0)
        result = np.divide(
            (np.where(valid, values, 0.0) * usable_weights).sum(axis=0),
            weight_sum,
            out=np.full(rows.shape, np.nan, dtype=np.float64),
            where=weight_sum > 0.0,
        )
        return np.asarray(result, dtype=np.float64)

    invalid_required_neighbour = (positive_weight & ~valid).any(axis=0)
    result = (np.where(valid, values, 0.0) * weights).sum(axis=0)
    result[invalid_required_neighbour] = np.nan
    return np.asarray(result, dtype=np.float64)


def interpolate_short_nan_gaps(values: FloatArray, *, max_gap: int) -> FloatArray:
    """Interpolate complete internal NaN runs no longer than ``max_gap``.

    Leading, trailing, and over-limit gaps remain entirely unchanged.
    """
    if max_gap < 0:
        raise ValueError(f"max_gap must be non-negative, received {max_gap}.")
    result = np.asarray(values, dtype=np.float64).copy()
    if max_gap == 0 or result.size < 3:
        return result

    nan_mask = np.isnan(result)
    index = 0
    while index < result.size:
        if not nan_mask[index]:
            index += 1
            continue
        gap_start = index
        while index < result.size and nan_mask[index]:
            index += 1
        gap_end = index
        gap_length = gap_end - gap_start
        if gap_start == 0 or gap_end == result.size or gap_length > max_gap:
            continue
        result[gap_start:gap_end] = np.linspace(
            result[gap_start - 1],
            result[gap_end],
            gap_length + 2,
            dtype=np.float64,
        )[1:-1]
    return result


def sample_raster_along_line(
    line: LineString,
    raster_path: str | Path,
    spacing: float | None = None,
    *,
    method: SamplingMethod = "bilinear_masked",
) -> tuple[FloatArray, FloatArray]:
    """Sample raster values at evenly spaced stations along a line.

    Stations include both endpoints and never exceed the requested spacing.
    Bilinear coordinates are aligned to raster cell centres.

    Args:
        line: Non-empty LineString geometry to sample.
        raster_path: Raster containing the values to sample.
        spacing: Maximum station spacing in line/raster CRS units. When omitted,
            half the smallest raster cell dimension is used.
        method: ``bilinear_masked`` requires the containing cell to be valid
            and renormalises valid neighbours. ``bilinear`` propagates any
            positively weighted NoData neighbour, ``bilinear_valid``
            renormalises neighbours without a containing-cell mask, and
            ``nearest`` returns the containing raster cell.

    Raises:
        ValueError: If the geometry, spacing, or method is invalid.
        RasterSamplingError: If the raster cannot be opened or sampled.
    """
    if line.is_empty or line.length <= 0.0:
        raise ValueError("The profile line must be non-empty and have positive length.")
    if spacing is not None and (not isfinite(spacing) or spacing <= 0.0):
        raise ValueError(f"spacing must be a finite positive number, received {spacing!r}.")
    if method not in {"bilinear", "bilinear_valid", "bilinear_masked", "nearest"}:
        raise ValueError(f"Unsupported sampling method: {method!r}.")

    source_path = Path(raster_path)
    try:
        with rasterio.open(source_path) as source:
            cell_x = abs(float(source.res[0]))
            cell_y = abs(float(source.res[1]))
            effective_spacing = spacing if spacing is not None else min(cell_x, cell_y) / 2.0
            if not isfinite(effective_spacing) or effective_spacing <= 0.0:
                raise ValueError(f"Raster resolution produced invalid spacing {effective_spacing!r}.")
            if spacing is None:
                logger.debug("Auto-detected spacing {:.3f} for {}", effective_spacing, source_path.name)

            length = float(line.length)
            interval_count = max(1, ceil(length / effective_spacing))
            distances = np.linspace(0.0, length, interval_count + 1, dtype=np.float64)
            coordinates = np.asarray(
                [(point.x, point.y) for point in (line.interpolate(distance) for distance in distances)],
                dtype=np.float64,
            )

            pad = max(cell_x, cell_y) * 2.0
            minimum = coordinates.min(axis=0) - pad
            maximum = coordinates.max(axis=0) + pad
            window = (
                rasterio.windows.from_bounds(
                    float(minimum[0]),
                    float(minimum[1]),
                    float(maximum[0]),
                    float(maximum[1]),
                    transform=source.transform,
                )
                .round_offsets()
                .round_lengths()
            )
            window_transform = source.window_transform(window)
            masked_band = source.read(1, window=window, boundless=True, masked=True)
            band = np.asarray(np.ma.filled(masked_band.astype(np.float64), np.nan), dtype=np.float64)
            if source.nodata is not None:
                band[np.isclose(band, float(source.nodata), atol=1e-5, rtol=1e-5)] = np.nan

            inverse_transform = ~window_transform
            pixel_coordinates = np.asarray(
                [inverse_transform * (float(x), float(y)) for x, y in coordinates],
                dtype=np.float64,
            )
            # Affine pixel coordinates address cell corners. Array values are
            # located at cell centres, hence the half-cell shift.
            cols = np.asarray(pixel_coordinates[:, 0] - 0.5, dtype=np.float64)
            rows = np.asarray(pixel_coordinates[:, 1] - 0.5, dtype=np.float64)

            if method == "nearest":
                values = _nearest_sample(band, rows, cols)
            else:
                values = _bilinear_sample(
                    band,
                    rows,
                    cols,
                    use_valid_neighbours=method in {"bilinear_valid", "bilinear_masked"},
                )
                if method == "bilinear_masked":
                    values[~_containing_cells_are_valid(band, rows, cols)] = np.nan
            return distances, values
    except ValueError, RasterSamplingError:
        raise
    except Exception as exc:
        raise RasterSamplingError(f"Failed to sample raster {source_path}: {exc}") from exc
