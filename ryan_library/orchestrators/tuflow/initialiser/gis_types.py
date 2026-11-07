"""Concrete GIS records used at the otherwise untyped library boundaries."""

from typing import TypedDict


class LayerSchema(TypedDict):
    """Fiona field definitions and geometry type for a populated layer."""

    geometry: str
    properties: dict[str, str]
