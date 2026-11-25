"""Compatibility exports for the canonical PyRORB readers module."""

from runoff_routing.rorb_readers.readers import (
    MAX_FILE_BYTES,
    RorbDocument,
    read_batch,
    read_cmd_parameters,
    read_csv,
    read_output,
    read_rorb_bytes,
    read_rorb_file,
    read_rorb_table_bytes,
    read_rorb_table_file,
    read_saved_settings,
    resembles_saved_settings,
)

__all__ = [
    "MAX_FILE_BYTES",
    "RorbDocument",
    "read_batch",
    "read_cmd_parameters",
    "read_csv",
    "read_output",
    "read_rorb_bytes",
    "read_rorb_file",
    "read_rorb_table_bytes",
    "read_rorb_table_file",
    "read_saved_settings",
    "resembles_saved_settings",
]
