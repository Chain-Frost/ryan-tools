"""External OGR SWIG boundary; no replacement Python implementation."""

from typing import Any, TypeAlias

DataSource: TypeAlias = Any
Layer: TypeAlias = Any
Geometry: TypeAlias = Any

def __getattr__(name: str) -> Any: ...
