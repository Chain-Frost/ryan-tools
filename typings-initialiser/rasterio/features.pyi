from collections.abc import Iterable, Iterator

import numpy as np
from affine import Affine
from numpy.typing import NDArray
from shapely.geometry.base import BaseGeometry

def rasterize(
    shapes: Iterable[tuple[BaseGeometry | dict[str, object], int]],
    *,
    out_shape: tuple[int, int],
    transform: Affine,
    all_touched: bool = ...,
    fill: int = ...,
    dtype: str = ...,
) -> NDArray[np.integer]: ...
def shapes(
    source: NDArray[np.integer], *, mask: NDArray[np.bool_] = ..., transform: Affine = ...
) -> Iterator[tuple[dict[str, object], float]]: ...
