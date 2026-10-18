"""External GDAL SWIG boundary; runtime functions are validated in GDAL tests."""

from typing import Any, TypeAlias

Dataset: TypeAlias = Any

def __getattr__(name: str) -> Any: ...
