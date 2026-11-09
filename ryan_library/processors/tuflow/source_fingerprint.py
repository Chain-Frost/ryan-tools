"""Bounded source identity for processed TUFLOW results.

The fingerprint is captured before processing, not when exporting a cache. It
intentionally avoids hashing potentially enormous solver outputs. A user who
needs stronger provenance should retain immutable source files.
"""

from __future__ import annotations

from pathlib import Path


def source_signature(source: Path) -> dict[str, object]:
    """Record the source file's path, size, nanosecond mtime and GPKG WAL state."""
    path = source.absolute()
    if not path.is_file():
        message = f"Processor source is missing or not a file: {path}"
        raise FileNotFoundError(message)
    stat = path.stat()
    result: dict[str, object] = {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    if path.suffix.casefold() == ".gpkg":
        wal = path.with_name(path.name + "-wal")
        if wal.is_file():
            wal_stat = wal.stat()
            result["wal"] = {"size": wal_stat.st_size, "mtime_ns": wal_stat.st_mtime_ns}
        else:
            result["wal"] = None
    return result
