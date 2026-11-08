from types import TracebackType
from typing import Any, Literal, NamedTuple, Self, overload

import numpy as np
from affine import Affine
from numpy.typing import NDArray

from .crs import CRS
from .windows import Window

class BoundingBox(NamedTuple):
    left: float
    bottom: float
    right: float
    top: float

type MaskedBand = np.ma.MaskedArray[Any, np.dtype[np.number[Any]]]

class DatasetReader:
    name: str
    width: int
    height: int
    count: int
    crs: CRS | None
    transform: Affine
    bounds: BoundingBox
    res: tuple[float, float]
    shape: tuple[int, int]
    nodata: float | None
    subdatasets: tuple[str, ...]
    dtypes: tuple[str, ...]
    profile: dict[str, Any]
    @overload
    def read(
        self,
        indexes: int | None = ...,
        *,
        masked: Literal[False] = ...,
        window: Window | None = ...,
        boundless: bool = ...,
    ) -> NDArray[np.number[Any]]: ...
    @overload
    def read(
        self, indexes: int | None = ..., *, masked: Literal[True], window: Window | None = ..., boundless: bool = ...
    ) -> MaskedBand: ...
    def window_transform(self, window: Window) -> Affine: ...
    def read_masks(self, indexes: int) -> NDArray[np.uint8]: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None: ...

class DatasetWriter(DatasetReader):
    def write(
        self, arr: NDArray[np.number[Any]], indexes: int | None = ..., *, window: Window | None = ...
    ) -> None: ...
