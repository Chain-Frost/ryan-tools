"""Deduplicated, network-friendly existence checks for TUFLOW input dependencies.

Prefer one bounded parent-directory listing when many unique requested files
share that directory. On sparse or exceptionally large directories use direct
path checks instead. Never traverse a model tree or the entire network drive.
"""

from __future__ import annotations

import fnmatch
import os
import sqlite3
import stat
from collections import defaultdict
from collections.abc import Collection, Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
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


def expand_file_patterns(  # noqa: C901 - grouped wildcard expansion and bounded scan error paths
    patterns: Collection[Path], *, maximum_directory_entries: int = 50000
) -> tuple[dict[Path, tuple[Path, ...]], dict[Path, DependencyResult], int]:
    """Expand nonrecursive wildcard filenames with one bounded scan per directory.

    A wildcard in the parent directory, an over-limit listing or any I/O error
    remains unresolved. No unbounded walk or recursive glob is attempted.
    """
    if maximum_directory_entries < 1:
        message = "maximum_directory_entries must be positive"
        raise ValueError(message)
    by_parent: dict[Path, list[Path]] = defaultdict(list)
    expanded: dict[Path, tuple[Path, ...]] = {}
    failures: dict[Path, DependencyResult] = {}
    scans = 0
    for pattern in set(patterns):
        if any(char in str(pattern.parent) for char in "*?["):
            failures[pattern] = DependencyResult("unresolved", "Wildcard parent directories are not supported")
        else:
            by_parent[pattern.parent].append(pattern)

    for parent, siblings in by_parent.items():
        found: dict[Path, list[Path]] = {pattern: [] for pattern in siblings}
        try:
            with os.scandir(parent) as entries:
                scans += 1
                for index, entry in enumerate(entries):
                    if index >= maximum_directory_entries:
                        for pattern in siblings:
                            failures[pattern] = DependencyResult(
                                "unresolved", "Wildcard directory exceeds the bounded scan limit"
                            )
                        break
                    matching = [
                        pattern
                        for pattern in siblings
                        if fnmatch.fnmatchcase(entry.name.casefold(), pattern.name.casefold())
                    ]
                    if matching:
                        try:
                            is_file = entry.is_file(follow_symlinks=True)
                        except OSError as exc:
                            for pattern in siblings:
                                failures[pattern] = DependencyResult(
                                    "unresolved", f"Cannot inspect wildcard match: {exc}"
                                )
                            break
                        if is_file:
                            for pattern in matching:
                                found[pattern].append(parent / entry.name)
        except FileNotFoundError:
            for pattern in siblings:
                failures[pattern] = DependencyResult("missing", "Wildcard parent directory does not exist")
        except OSError as exc:
            for pattern in siblings:
                failures[pattern] = DependencyResult("unresolved", f"Cannot scan wildcard directory: {exc}")
        for pattern in siblings:
            if pattern in failures:
                continue
            matches = tuple(sorted(found[pattern]))
            if matches:
                expanded[pattern] = matches
            else:
                failures[pattern] = DependencyResult("missing", "No input files match wildcard pattern")
    return expanded, failures, scans


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
        with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as connection:
            names = {str(name).casefold() for (name,) in connection.execute("SELECT table_name FROM gpkg_contents")}
    except (OSError, sqlite3.DatabaseError) as exc:
        return {layer: DependencyResult("unresolved", f"Cannot inspect GeoPackage layers: {exc}") for layer in layers}
    return {
        layer: DependencyResult("present")
        if (any(char in layer for char in "*?[") and any(fnmatch.fnmatchcase(name, layer.casefold()) for name in names))
        or (layer.casefold() == "all" and bool(names))
        or layer.casefold() in names
        else DependencyResult("missing", f"GeoPackage layer does not exist: {layer}")
        for layer in layers
    }


def check_unique_dependencies(  # noqa: C901 - inventory grouping, directory fallback and layer checks
    requirements: Mapping[Path, Collection[str]],
    *,
    scan_threshold: int = 8,
    io_workers: int = 1,
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

    if not 1 <= io_workers <= 32:
        message = "io_workers must be between 1 and 32"
        raise ValueError(message)
    # Freeze caller-owned collections before sharing them with I/O workers.
    requirements = {path: tuple(layers) for path, layers in requirements.items()}
    by_parent: dict[Path, list[Path]] = defaultdict(list)
    for path in requirements:
        by_parent[path.parent].append(path)

    def inspect_group(paths: list[Path]) -> FileInventory:
        files: dict[Path, DependencyResult] = {}
        directory_result = None
        scans = direct = 0
        if len(paths) >= scan_threshold:
            directory_result = _directory_results(paths, maximum_entries=maximum_directory_entries)
            if directory_result is not None:
                scans = 1
        if directory_result is not None:
            files.update(directory_result)
        else:
            for path in paths:
                files[path] = _individual_file(path)
                direct += 1
        layers: dict[tuple[Path, str], DependencyResult] = {}
        for path in paths:
            if files[path].status != "present" or path.suffix.casefold() != ".gpkg" or not requirements[path]:
                continue
            # Each connection is opened, used and closed in its owning worker.
            for layer, result in _geopackage_layers(path, requirements[path]).items():
                layers[(path, layer)] = result
        return FileInventory(files, layers, scans, direct)

    # Dense directories remain one listing job. Sparse files can overlap even
    # when they share a parent. No duplicate stat or SQLite jobs per simulation.
    groups = [
        group
        for paths in by_parent.values()
        for group in ([paths] if len(paths) >= scan_threshold else [[path] for path in paths])
    ]
    inventories: list[FileInventory] = []
    if io_workers == 1:
        inventories = [inspect_group(group) for group in groups]
    else:
        with ThreadPoolExecutor(max_workers=io_workers, thread_name_prefix="tuflow-inventory") as executor:
            # Refill on completion so one slow network request cannot hold up
            # the other workers. Retain submission order for report stability.
            remaining = iter(enumerate(groups))
            pending = {
                executor.submit(inspect_group, group): index
                for index, group in (next(remaining) for _ in range(min(io_workers, len(groups))))
            }
            completed: dict[int, FileInventory] = {}
            while pending:
                finished, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in finished:
                    completed[pending.pop(future)] = future.result()
                    following = next(remaining, None)
                    if following is not None:
                        index, group = following
                        pending[executor.submit(inspect_group, group)] = index
            inventories = [completed[index] for index in range(len(groups))]
    files = {path: result for inventory in inventories for path, result in inventory.files.items()}
    layers = {key: result for inventory in inventories for key, result in inventory.layers.items()}
    return FileInventory(
        files,
        layers,
        sum(item.directories_scanned for item in inventories),
        sum(item.individually_checked for item in inventories),
    )
