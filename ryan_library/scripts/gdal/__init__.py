"""Compatibility package for GDAL orchestrators."""

from ryan_library.orchestrators.gdal import gdal_flood_extent as gdal_flood_extent
from ryan_library.scripts._compat import warn_deprecated

warn_deprecated("ryan_library.scripts.gdal", "ryan_library.orchestrators.gdal")

__all__ = ["gdal_flood_extent"]
