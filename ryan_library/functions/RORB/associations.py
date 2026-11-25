"""Compatibility exports for the canonical PyRORB associations module."""

from runoff_routing.rorb_readers.associations import (
    BatchEventKey,
    Record,
    RorbDocument,
    Section,
    associate_batch_csvs,
    batch_event_key,
    compare_output_csv,
    read_rorb_file,
)

__all__ = [
    "BatchEventKey",
    "Record",
    "RorbDocument",
    "Section",
    "associate_batch_csvs",
    "batch_event_key",
    "compare_output_csv",
    "read_rorb_file",
]
