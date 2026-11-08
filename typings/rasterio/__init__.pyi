from os import PathLike
from typing import Literal, Self, overload

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

class Env:
    def __init__(self, **options: object) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...

@overload
def open(fp: str | PathLike[str], mode: Literal["r"] = ..., **kwargs: object) -> DatasetReader: ...
@overload
def open(
    fp: str | PathLike[str],
    mode: Literal["w", "w+", "r+"],
    **kwargs: object,
) -> DatasetWriter: ...
@overload
def open(fp: str | PathLike[str], mode: str, **kwargs: object) -> DatasetReader | DatasetWriter: ...
