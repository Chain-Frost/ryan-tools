"""Rasterio CRS properties used by the hourly raster validation workflow."""


class CRS:
    is_projected: bool
    linear_units_factor: tuple[str, float]

    @classmethod
    def from_user_input(cls, value: str | int) -> CRS: ...

    def to_wkt(self) -> str: ...
