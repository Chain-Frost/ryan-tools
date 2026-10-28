"""Narrow typed rasterio dataset surface used by maintained raster workflows."""

from collections.abc import Iterable, Iterator
from typing import Self

import numpy as np
from affine import Affine
from numpy.typing import NDArray

from .crs import CRS


class Dataset:
    width: int
    height: int
    count: int
    subdatasets: tuple[str, ...]
    crs: CRS | None
    transform: Affine

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def read(self, indexes: int = 1) -> NDArray[np.generic]: ...
    def read_masks(self, indexes: int = 1) -> NDArray[np.uint8]: ...
    def sample(
        self, coordinates: Iterable[tuple[float, float]], indexes: int = 1, masked: bool = False
    ) -> Iterator[NDArray[np.float32]]: ...
    def write(self, array: object, indexes: int = 1) -> None: ...


def open(fp: object, mode: str = "r", **kwargs: object) -> Dataset: ...
