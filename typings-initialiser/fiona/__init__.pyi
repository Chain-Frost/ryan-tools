from collections.abc import Iterator
from os import PathLike
from types import TracebackType
from typing import Self, TypedDict

from . import errors as errors
from . import model as model
from .crs import CRS
from .model import Feature, FeatureRecord

class Schema(TypedDict):
    geometry: str
    properties: dict[str, str]

class Collection:
    name: str
    crs: CRS
    crs_wkt: str | None
    schema: Schema
    def __iter__(self) -> Iterator[Feature]: ...
    def __len__(self) -> int: ...
    def get(self, item: int | str) -> Feature | None: ...
    def write(self, record: FeatureRecord) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None: ...

def open(  # noqa: A001 - third-party public API name
    fp: str | PathLike[str],
    mode: str = ...,
    *,
    driver: str = ...,
    layer: str = ...,
    schema: Schema = ...,
    crs_wkt: str | None = ...,
    encoding: str = ...,
) -> Collection: ...
def listlayers(fp: str | PathLike[str]) -> list[str]: ...
