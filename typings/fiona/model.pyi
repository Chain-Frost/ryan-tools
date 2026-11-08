from collections.abc import Mapping
from typing import Literal, TypedDict, overload

type FieldValue = str | int | float | None

class Geometry:
    @property
    def __geo_interface__(self) -> dict[str, object]: ...

class Feature:
    geometry: Geometry | None
    properties: Mapping[str, FieldValue]
    @overload
    def __getitem__(self, key: Literal["geometry"]) -> Geometry | None: ...
    @overload
    def __getitem__(self, key: Literal["properties"]) -> Mapping[str, FieldValue]: ...
    @overload
    def get(self, key: Literal["geometry"]) -> Geometry | None: ...
    @overload
    def get(self, key: Literal["properties"]) -> Mapping[str, FieldValue]: ...

class FeatureRecord(TypedDict):
    geometry: Mapping[str, object] | None
    properties: Mapping[str, FieldValue]
