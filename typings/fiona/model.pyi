from collections.abc import Mapping
from typing import TypedDict

type FieldValue = str | int | float | None

class Geometry:
    @property
    def __geo_interface__(self) -> dict[str, object]: ...

class Feature:
    geometry: Geometry | None
    properties: Mapping[str, FieldValue]

class FeatureRecord(TypedDict):
    geometry: dict[str, object] | None
    properties: dict[str, FieldValue]
