"""Compatibility exports for the canonical PyRORB parameter_reader module."""

from runoff_routing.rorb_readers.parameter_reader import (
    read_cmd_parameters,
    read_saved_settings,
    resembles_saved_settings,
)
from runoff_routing.rorb_readers.reader_types import (
    RorbDocument,
    Section,
    integer,
    number,
)

__all__ = [
    "RorbDocument",
    "Section",
    "integer",
    "number",
    "read_cmd_parameters",
    "read_saved_settings",
    "resembles_saved_settings",
]
