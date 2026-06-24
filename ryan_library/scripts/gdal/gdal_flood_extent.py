"""Compatibility wrapper for the relocated GDAL flood extent orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.gdal.gdal_flood_extent")

from ryan_library.orchestrators.gdal.gdal_flood_extent import *  # noqa: F401,F403

