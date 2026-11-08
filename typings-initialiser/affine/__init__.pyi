from collections.abc import Iterator
from typing import overload

class Affine:
    a: float
    b: float
    c: float
    d: float
    e: float
    f: float
    @classmethod
    def translation(cls, xoff: float, yoff: float) -> Affine: ...
    @classmethod
    def scale(cls, scaling: float, yscale: float | None = ...) -> Affine: ...
    @overload
    def __mul__(self, other: Affine) -> Affine: ...
    @overload
    def __mul__(self, other: tuple[float, float]) -> tuple[float, float]: ...
    def __iter__(self) -> Iterator[float]: ...
