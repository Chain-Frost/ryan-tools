"""External OSR SWIG boundary; runtime CRS checks use the real osgeo extension."""

from typing import Any, TypeAlias

SpatialReference: TypeAlias = Any

def __getattr__(name: str) -> Any: ...
