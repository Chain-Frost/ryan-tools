"""Rasterio CRS properties used by the hourly raster validation workflow."""


class CRS:
    is_projected: bool
    linear_units_factor: tuple[str, float]

    def to_wkt(self) -> str: ...
