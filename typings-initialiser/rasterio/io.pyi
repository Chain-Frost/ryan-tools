from types import TracebackType
from typing import Literal, NamedTuple, Protocol, Self, overload

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

class MaskedBand(Protocol):
    def filled(self, fill_value: float) -> NDArray[np.number]: ...

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
    @overload
    def read(
        self, indexes: int = ..., *, masked: Literal[False] = ..., window: Window | None = ...
    ) -> NDArray[np.number]: ...
    @overload
    def read(self, indexes: int = ..., *, masked: Literal[True], window: Window | None = ...) -> MaskedBand: ...
    def read_masks(self, indexes: int) -> NDArray[np.uint8]: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None: ...

class DatasetWriter(DatasetReader):
    def write(self, arr: NDArray[np.number], indexes: int | None = ..., *, window: Window | None = ...) -> None: ...
