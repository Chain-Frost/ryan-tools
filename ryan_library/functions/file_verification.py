"""Shared filesystem snapshots, path-link checks and streaming SHA-256 hashing.

These primitives do not infer whether replacing or deleting a file is safe.
Callers must enforce their own validation, confirmation and dependency rules.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class FileSnapshot:
    """Inexpensive evidence for detecting changes before file mutations."""

    path: Path
    size: int
    mtime_ns: int


def snapshot_file(path: Path) -> FileSnapshot:
    """Record metadata using an absolute path without dereferencing links."""
    absolute_path = path.absolute()
    stat = absolute_path.stat()
    return FileSnapshot(path=absolute_path, size=stat.st_size, mtime_ns=stat.st_mtime_ns)


def snapshot_is_unchanged(snapshot: FileSnapshot) -> bool:
    """Check metadata only; callers separately enforce link restrictions."""
    try:
        return snapshot_file(snapshot.path) == snapshot
    except OSError:
        return False


def is_junction(path: Path) -> bool:
    """Detect Windows mount-point reparse tags without following the target.

    Path.is_junction was added in Python 3.12. lstat retains the same protection
    on Python 3.11, including junctions whose targets are unavailable.
    """
    if os.name != "nt":
        return False
    try:
        info = path.lstat()
    except OSError:
        return False
    return getattr(info, "st_reparse_tag", 0) == stat.IO_REPARSE_TAG_MOUNT_POINT


def has_linked_component(path: Path) -> bool:
    """Identify links in the absolute path or any parent without dereferencing them."""
    absolute_path = path.absolute()
    components = (absolute_path, *absolute_path.parents)
    return any(component.is_symlink() or is_junction(component) for component in components)


def hash_file(
    path: Path,
    *,
    chunk_size: int = 8 * 1024 * 1024,
    on_chunk: Callable[[int], None] | None = None,
) -> bytes:
    """Return binary SHA-256, optionally reporting bytes read in the same pass."""
    if chunk_size < 1:
        message = "chunk_size must be positive"
        raise ValueError(message)
    digest = hashlib.sha256()
    with path.absolute().open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
            if on_chunk is not None:
                on_chunk(len(chunk))
    return digest.digest()
