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
    @property
    def crs(self) -> CRS | None: ...
    transform: Affine
    bounds: BoundingBox
    res: tuple[float, float]
    shape: tuple[int, int]
    nodata: float | None
    subdatasets: tuple[str, ...]
    dtypes: tuple[str, ...]
    profile: dict[str, Any]
    meta: dict[str, Any]
    scales: tuple[float, ...]
    offsets: tuple[float, ...]
    def overviews(self, bidx: int) -> list[int]: ...
    @overload
    def read(
        self,
        indexes: int | None = ...,
        *,
        masked: Literal[False] = ...,
        window: Window | tuple[tuple[int, int], tuple[int, int]] | None = ...,
        boundless: bool = ...,
    ) -> NDArray[np.number[Any]]: ...
    @overload
    def read(
        self,
        indexes: int | None = ...,
        *,
        masked: Literal[True],
        window: Window | tuple[tuple[int, int], tuple[int, int]] | None = ...,
        boundless: bool = ...,
    ) -> MaskedBand: ...
    def window_transform(self, window: Window) -> Affine: ...
    def read_masks(self, indexes: int) -> NDArray[np.uint8]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None: ...

class DatasetWriter(DatasetReader):
    @property
    def crs(self) -> CRS | None: ...
    @crs.setter
    def crs(self, value: str | CRS | None) -> None: ...
    def update_tags(self, bidx: int = ..., ns: str | None = ..., **kwargs: object) -> None: ...
    def write(
        self, arr: NDArray[np.number[Any]], indexes: int | None = ..., *, window: Window | None = ...
    ) -> None: ...
