from os import PathLike
from typing import Literal, overload

from affine import Affine as Affine

from . import crs as crs
from . import io as io
from . import windows as windows
from .crs import CRS
from .io import DatasetReader, DatasetWriter

@overload
def open(fp: str | PathLike[str], mode: Literal["r"] = ..., **kwargs: object) -> DatasetReader: ...  # noqa: A001 - third-party public API name
@overload
def open(  # noqa: A001 - third-party public API name
    fp: str | PathLike[str],
    mode: Literal["w", "w+"],
    *,
    driver: str = ...,
    width: int = ...,
    height: int = ...,
    count: int = ...,
    crs: str | CRS = ...,
    transform: Affine = ...,
    dtype: str = ...,
    nodata: float = ...,
    **kwargs: object,
) -> DatasetWriter: ...
