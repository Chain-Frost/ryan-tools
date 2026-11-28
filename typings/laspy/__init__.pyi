from collections.abc import Iterator
from os import PathLike
from typing import Any, Literal, Self, overload

import numpy as np
from numpy.typing import ArrayLike, NDArray

class LasHeader:
    offsets: NDArray[np.float64]
    scales: NDArray[np.float64]
    def __init__(self, *, point_format: int = ..., version: str = ...) -> None: ...

class ScaledArrayView:
    def __len__(self) -> int: ...
    def __array__(self, dtype: np.dtype[Any] | None = ..., copy: bool | None = ...) -> NDArray[np.generic[Any]]: ...

class ScaleAwarePointRecord:
    x: ScaledArrayView
    y: ScaledArrayView
    z: ScaledArrayView
    def __getitem__(self, item: NDArray[np.bool_]) -> ScaleAwarePointRecord: ...

class LasReader:
    header: LasHeader
    def chunk_iterator(self, points_per_iteration: int) -> Iterator[ScaleAwarePointRecord]: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...

class LasWriter:
    def write_points(self, points: ScaleAwarePointRecord) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...

class LasData:
    x: ArrayLike
    y: ArrayLike
    z: ArrayLike
    def __init__(self, header: LasHeader) -> None: ...
    def write(self, destination: str | PathLike[str]) -> None: ...

@overload
def open(source: str | PathLike[str], mode: Literal["r"] = ..., **kwargs: object) -> LasReader: ...  # noqa: A001 - public dependency API
@overload
def open(source: str | PathLike[str], mode: Literal["w"], *, header: LasHeader, **kwargs: object) -> LasWriter: ...  # noqa: A001 - public dependency API
