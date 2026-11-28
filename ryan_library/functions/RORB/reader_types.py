"""Compatibility exports for the canonical PyRORB reader_types module."""

from runoff_routing.rorb_readers.reader_types import (
    NUMBER,
    Diagnostic,
    Record,
    RorbDocument,
    Section,
    Value,
    exact_number,
    integer,
    number,
    validate_series,
)

__all__ = [
    "NUMBER",
    "Diagnostic",
    "Record",
    "RorbDocument",
    "Section",
    "Value",
    "exact_number",
    "integer",
    "number",
    "validate_series",
]
