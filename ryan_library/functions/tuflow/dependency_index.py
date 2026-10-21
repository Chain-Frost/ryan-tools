"""Deduplicated, network-friendly existence checks for TUFLOW input dependencies.

Prefer one bounded parent-directory listing when many unique requested files
share that directory. On sparse or exceptionally large directories use direct
path checks instead. Never traverse a model tree or the entire network drive.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from collections import defaultdict
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

FileStatus = Literal["present", "missing", "unresolved"]


@dataclass(frozen=True, slots=True)
class DependencyResult:
    """Result of checking one unique file or one GeoPackage layer."""

    status: FileStatus
    detail: str = ""


@dataclass(frozen=True, slots=True)
class FileInventory:
    """Cached path and layer statuses; scanned folders count is diagnostic."""

    files: Mapping[Path, DependencyResult]
    layers: Mapping[tuple[Path, str], DependencyResult]
    directories_scanned: int
    individually_checked: int


def _individual_file(path: Path) -> DependencyResult:
    """Classify a file without treating I/O errors as missing files."""
    try:
        mode = path.stat().st_mode
    except FileNotFoundError:
        return DependencyResult("missing", "Input file does not exist")
    except OSError as exc:
        return DependencyResult("unresolved", f"Cannot inspect input file: {exc}")
    if not stat.S_ISREG(mode):
        return DependencyResult("missing", "Expected a file, but the path is a directory or another type")
    return DependencyResult("present")


def _directory_results(paths: Collection[Path], *, maximum_entries: int) -> dict[Path, DependencyResult] | None:
    """Enumerate a parent directory once; None means use targeted checks.

    A large directory may cost substantially more than targeted metadata calls.
    If enumeration exceeds the bound, return None and fall back to individual
    checks. Permission and network errors remain unresolved, never 'missing'.
    """
    if not paths:
        return {}
    parent = next(iter(paths)).parent
    requested = {path.name.casefold() for path in paths}
    matches: dict[str, bool] = {}
    try:
        with os.scandir(parent) as iterator:
            for index, entry in enumerate(iterator):
                if index >= maximum_entries:
                    return None
                name = entry.name.casefold()
                if name in requested:
                    try:
                        matches[name] = entry.is_file(follow_symlinks=True)
                    except OSError as exc:
                        return {
                            path: DependencyResult("unresolved", f"Cannot inspect directory entry: {exc}")
                            for path in paths
                        }
    except FileNotFoundError:
        return {path: DependencyResult("missing", "Parent input directory does not exist") for path in paths}
    except OSError as exc:
        return {path: DependencyResult("unresolved", f"Cannot list input directory: {exc}") for path in paths}
    return {
        path: DependencyResult("present")
        if matches.get(path.name.casefold())
        else DependencyResult("missing", "Input file does not exist")
        for path in paths
    }


def _geopackage_layers(path: Path, layers: Collection[str]) -> dict[str, DependencyResult]:
    """Check all required GeoPackage layers in one read-only SQLite connection."""
    try:
        with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as connection:
            names = {str(name).casefold() for (name,) in connection.execute("SELECT table_name FROM gpkg_contents")}
    except (OSError, sqlite3.DatabaseError) as exc:
        return {layer: DependencyResult("unresolved", f"Cannot inspect GeoPackage layers: {exc}") for layer in layers}
    return {
        layer: DependencyResult("present")
        if layer.casefold() in names
        else DependencyResult("missing", f"GeoPackage layer does not exist: {layer}")
        for layer in layers
    }


def check_unique_dependencies(  # noqa: C901 - inventory grouping, directory fallback and layer checks
    requirements: Mapping[Path, Collection[str]],
    *,
    scan_threshold: int = 8,
    maximum_directory_entries: int = 50000,
) -> FileInventory:
    """Check each distinct input file once and each GeoPackage once for its layers.

    Call this only *after* collecting the required files of all proposed runs.
    A directory with at least scan_threshold distinct inputs is listed once.
    Directory trees are never recursively enumerated.
    """
    if scan_threshold < 1 or maximum_directory_entries < 1:
        message = "scan_threshold and maximum_directory_entries must be positive"
        raise ValueError(message)

    by_parent: dict[Path, list[Path]] = defaultdict(list)
    for path in requirements:
        by_parent[path.parent].append(path)

    files: dict[Path, DependencyResult] = {}
    directory_scans = 0
    direct_checks = 0
    for paths in by_parent.values():
        directory_result = None
        if len(paths) >= scan_threshold:
            directory_result = _directory_results(paths, maximum_entries=maximum_directory_entries)
            if directory_result is not None:
                directory_scans += 1
        if directory_result is not None:
            files.update(directory_result)
        else:
            for path in paths:
                files[path] = _individual_file(path)
                direct_checks += 1

    layers: dict[tuple[Path, str], DependencyResult] = {}
    for path, requested_layers in requirements.items():
        if files[path].status != "present" or path.suffix.casefold() != ".gpkg":
            continue
        for layer, result in _geopackage_layers(path, requested_layers).items() if requested_layers else ():
            layers[(path, layer)] = result

    return FileInventory(files, layers, directory_scans, direct_checks)
