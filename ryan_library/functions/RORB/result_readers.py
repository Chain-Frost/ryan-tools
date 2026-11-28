"""Compatibility exports for the canonical PyRORB result_readers module."""

from runoff_routing.rorb_readers.reader_types import (
    Record,
    RorbDocument,
    Section,
    integer,
    number,
    validate_series,
)
from runoff_routing.rorb_readers.result_readers import (
    read_batch,
    read_csv,
    read_sites,
    report_metadata,
)

__all__ = [
    "Record",
    "RorbDocument",
    "Section",
    "integer",
    "number",
    "read_batch",
    "read_csv",
    "read_sites",
    "report_metadata",
    "validate_series",
]
