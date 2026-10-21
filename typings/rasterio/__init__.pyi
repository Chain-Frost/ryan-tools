"""Narrow rasterio dataset API used by shared metadata checks and fixtures."""

from collections.abc import Sequence
from typing import Self

from numpy import ndarray

class Dataset:
    width: int
    height: int
    count: int

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def write(self, array: ndarray, indexes: int = 1) -> None: ...

def open(fp: str | bytes | object, mode: str = "r", **kwargs: object) -> Dataset: ...
