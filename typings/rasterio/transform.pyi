"""Narrow rasterio transform constructor used by synthetic GeoTIFF fixtures."""

from affine import Affine


def from_origin(west: float, north: float, xsize: float, ysize: float) -> Affine: ...
