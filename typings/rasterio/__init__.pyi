"""Narrow rasterio dataset and environment APIs used by raster workflows and fixtures."""

from typing import Self

import numpy as np
import numpy.typing as npt

class Env:
    def __init__(self, **options: object) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...

class Dataset:
    width: int
    height: int
    count: int
    subdatasets: tuple[str, ...]

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def write(self, array: object, indexes: int = 1) -> None: ...
    def read(self, indexes: int | list[int] | None = None, **kwargs: object) -> npt.NDArray[np.generic]: ...

def open(fp: object, mode: str = "r", **kwargs: object) -> Dataset: ...  # noqa: A001 - Rasterio's public API.
