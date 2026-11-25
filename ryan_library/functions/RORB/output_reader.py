"""Compatibility exports for the canonical PyRORB output_reader module."""

from runoff_routing.rorb_readers.output_reader import (
    HydrographSiteColumn,
    Record,
    RorbDocument,
    Section,
    bind_output_site_columns,
    exact_number,
    integer,
    number,
    read_output,
    read_sites,
    report_metadata,
    validate_series,
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
