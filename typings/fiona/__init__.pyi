"""Narrow Fiona layer API for typed GeoPackage point and metadata workflows."""

from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Self, TypedDict


class Feature(TypedDict):
    properties: Mapping[str, object]
    geometry: object


class Collection:
    schema: Mapping[str, object]

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def __iter__(self) -> Iterator[Feature]: ...
    def write(self, record: Mapping[str, object]) -> None: ...
    def writerecords(self, records: Iterable[Mapping[str, object]]) -> None: ...


def open(fp: str | Path, mode: str = "r", **kwargs: object) -> Collection: ...

def listlayers(fp: str | Path, **kwargs: object) -> list[str]: ...
