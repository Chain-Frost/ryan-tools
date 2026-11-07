"""Narrow rasterio dataset API used by shared metadata checks and fixtures."""

from typing import Self


class Dataset:
    width: int
    height: int
    count: int
    subdatasets: tuple[str, ...]

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def write(self, array: object, indexes: int = 1) -> None: ...


def open(fp: object, mode: str = "r", **kwargs: object) -> Dataset: ...
