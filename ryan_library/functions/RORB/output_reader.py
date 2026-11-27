"""Compatibility exports for the canonical PyRORB output_reader module."""

from runoff_routing.rorb_readers.output_reader import (
    HydrographSiteColumn,
    bind_output_site_columns,
    read_output,
)
from runoff_routing.rorb_readers.reader_types import (
    Record,
    RorbDocument,
    Section,
    exact_number,
    integer,
    number,
    validate_series,
)
from runoff_routing.rorb_readers.result_readers import (
    read_sites,
    report_metadata,
)

__all__ = [
    "HydrographSiteColumn",
    "Record",
    "RorbDocument",
    "Section",
    "bind_output_site_columns",
    "exact_number",
    "integer",
    "number",
    "read_output",
    "read_sites",
    "report_metadata",
    "validate_series",
]
