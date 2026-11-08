from os import PathLike
from typing import Literal, overload

from affine import Affine as Affine

from . import crs as crs
from . import io as io
from . import windows as windows
from .crs import CRS as CRS
from .io import DatasetReader as DatasetReader
from .io import DatasetWriter as DatasetWriter

# Compatibility with the shared fixture surface; both runtime dataset modes
# provide these reader attributes and context-manager operations.
Dataset = DatasetReader

@overload
def open(fp: str | PathLike[str], mode: Literal["r"] = ..., **kwargs: object) -> DatasetReader: ...
@overload
def open(
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
