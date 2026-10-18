"""External OGR SWIG boundary; runtime geometries are validated with actual GDAL."""

from typing import Any, TypeAlias

DataSource: TypeAlias = Any
Layer: TypeAlias = Any

class Geometry:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def __getattr__(self, name: str) -> Any: ...

def __getattr__(name: str) -> Any: ...
