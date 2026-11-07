"""Concrete GIS records used at the otherwise untyped library boundaries."""

from typing import TypedDict

from affine import Affine
from rasterio.crs import CRS


class LayerSchema(TypedDict):
    """Fiona field definitions and geometry type for a populated layer."""

    geometry: str
    properties: dict[str, str]


class RasterProfile(TypedDict):
    """GeoTIFF output options used by the blockwise terrain mosaic."""

    driver: str
    dtype: str
    count: int
    height: int
    width: int
    crs: CRS
    transform: Affine
    nodata: float
    compress: str
    BIGTIFF: str
