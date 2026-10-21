"""Read-only, deduplicated discovery of TUFLOW GIS attribute-linked input files.

Only the declared file-valued field positions for recognised TUFLOW layer types
are inspected. GeoPackages are opened once per distinct file, not per run.
Unsupported sources/schemas remain unresolved rather than silently passing.
"""

from __future__ import annotations

import fnmatch
import os
import re
import sqlite3
from collections import defaultdict
from collections.abc import Collection
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True, slots=True)
class AttributeInspection:
    """Secondary file references or a reason the layer cannot be certified."""

    status: Literal["present", "unresolved"]
    sources: tuple[Path, ...] = ()
    detail: str = ""


def _quote_identifier(value: str) -> str:
    """Quote only identifiers returned by the opened GeoPackage itself."""
    return '"' + value.replace('"', '""') + '"'


def _file_field_index(layer: str) -> int | None:
    """Mirror PyTUFLOW's file-valued field conventions for recognised layers."""
    name = layer.casefold()
    if re.match(r"^1d_nwk[eb]?_", name):
        return 10
    if re.match(r"^1d_pit_", name):
        return 3
    if re.match(r"^1d_(xs|tab|xz|bg|lc|cs|hw)_", name) or re.match(r"^1d_na_", name):
        return 0
    return None


def _attribute_file_paths(value: str, *, network_m: bool, parent: Path) -> tuple[Path, ...]:
    """Convert candidate TUFLOW attribute values into file references."""
    raw = value.strip()
    if not raw:
        return ()
    if "<<" in raw or ">>" in raw:
        message = "Unexpanded variable or ambiguous GIS attribute reference"
        raise ValueError(message)
    if "|" in raw:
        operation, reference = (part.strip() for part in raw.split("|", maxsplit=1))
        values = (operation, reference) if network_m else (reference,)
    else:
        values = (raw,)
    paths: list[Path] = []
    for item in values:
        if not item:
            message = "Empty GIS attribute file reference"
            raise ValueError(message)
        # TUFLOW interprets an initial slash as relative to the source
        # layer's directory; retain absolute Windows drives/UNC shares.
        windows_absolute = bool(re.match(r"^[A-Za-z]:[\\/]", item) or item.startswith("\\\\"))
        normal = item.replace("\\", "/")
        if normal.startswith("/") and not windows_absolute:
            normal = "." + normal
        candidate = Path(item if windows_absolute else normal)
        if not candidate.is_absolute() and not windows_absolute:
            candidate = parent / candidate
        if windows_absolute and os.name != "nt":
            message = "Windows absolute attribute reference requires Windows inspection"
            raise ValueError(message)
        paths.append(candidate.absolute())
    return tuple(paths)


def _scan_layer(  # noqa: C901 - feature schema, channel and field validation
    connection: sqlite3.Connection, layer: str, geometry_type: str, path: Path
) -> tuple[Path, ...]:
    """Read only relevant attribute fields; do not fetch geometry blobs."""
    index = _file_field_index(layer)
    if index is None:
        message = f"Unsupported GIS attribute-linked layer type: {layer}"
        raise ValueError(message)
    network = bool(re.match(r"^1d_nwk[eb]?_", layer, flags=re.IGNORECASE))
    if network and geometry_type.casefold() not in {"linestring", "multilinestring"}:
        if geometry_type.casefold() != "geometry":
            return ()  # A known non-line geometry cannot be an M/Q network channel.
        message = "Mixed or unknown network geometry requires feature-level inspection"
        raise ValueError(message)
    quoted = _quote_identifier(layer)
    columns = [(name, pk) for _, name, _, _, _, pk in connection.execute(f"PRAGMA table_info({quoted})")]
    geom_names = {
        str(name).casefold()
        for (name,) in connection.execute(
            "SELECT column_name FROM gpkg_geometry_columns WHERE lower(table_name) = lower(?)",
            (layer,),
        )
    }
    if not geom_names:
        message = f"GeoPackage layer geometry column is unavailable: {layer}"
        raise ValueError(message)
    # OGR attributes exclude the GeoPackage feature ID and geometry column.
    attrs = [name for name, pk in columns if not pk and str(name).casefold() not in geom_names]
    if len(attrs) <= index or (network and len(attrs) <= 1):
        message = f"Required file-reference attribute index {index} absent from {layer}"
        raise ValueError(message)
    field = _quote_identifier(str(attrs[index]))
    second = _quote_identifier(str(attrs[1])) if network else ""
    # Each SQL identifier came from SQLite metadata and was double-quoted by
    # _quote_identifier; values are never interpolated into SQL statements.
    sql = (
        f"SELECT {second}, {field} FROM {quoted}"  # noqa: S608 - identifiers safely double-quoted
        if network
        else f"SELECT {field} FROM {quoted}"  # noqa: S608 - identifiers safely double-quoted
    )
    sources: set[Path] = set()
    for row in connection.execute(sql):
        candidate_value = row[-1]
        if candidate_value is None or candidate_value == "":
            continue
        if not isinstance(candidate_value, str):
            message = f"Non-text GIS file-reference attribute in {layer}"
            raise ValueError(message)
        is_m = False
        if network:
            channel = row[0]
            if channel is None:
                continue
            if not isinstance(channel, str):
                message = f"Invalid network channel type in {layer}"
                raise ValueError(message)
            if not channel.casefold().startswith(("m", "q")):
                continue
            is_m = channel.casefold().startswith("m")
        sources.update(_attribute_file_paths(candidate_value, network_m=is_m, parent=path.parent))
    return tuple(sorted(sources))


def inspect_gis_attribute_sources(  # noqa: C901 - deduped per-database inspection and error routing
    requests: Collection[tuple[Path, str]],
) -> tuple[dict[tuple[Path, str], AttributeInspection], int]:
    """Scan distinct GeoPackage/layer patterns, one read-only open per GeoPackage.

    A wildcard or 'all' layer selector is expanded from gpkg_contents and all
    matched layers are inspected. Any unknown layer or schema fails closed.
    """
    grouped: dict[Path, set[str]] = defaultdict(set)
    for path, layer in requests:
        grouped[path].add(layer)

    results: dict[tuple[Path, str], AttributeInspection] = {}
    connections = 0
    for path, selectors in grouped.items():
        if path.suffix.casefold() != ".gpkg":
            for selector in selectors:
                results[(path, selector)] = AttributeInspection(
                    "unresolved", detail="GIS attribute scan currently supports GeoPackage sources only"
                )
            continue
        try:
            with closing(sqlite3.connect(f"{path.absolute().as_uri()}?mode=ro", uri=True)) as connection:
                connections += 1
                available = {
                    str(name): str(geometry)
                    for name, geometry in connection.execute(
                        "SELECT table_name, geometry_type_name FROM gpkg_geometry_columns"
                    )
                }
                if not available:
                    message = "No vector layer metadata in GeoPackage"
                    raise ValueError(message)
                scanned: dict[str, tuple[Path, ...]] = {}
                for selector in selectors:
                    key = (path, selector)
                    matches = [
                        name
                        for name in available
                        if selector.casefold() == "all" or fnmatch.fnmatchcase(name.casefold(), selector.casefold())
                    ]
                    if not matches:
                        results[key] = AttributeInspection(
                            "unresolved", detail=f"No GIS attribute layers matched {selector!r}"
                        )
                        continue
                    try:
                        for name in matches:
                            if name not in scanned:
                                scanned[name] = _scan_layer(connection, name, available[name], path)
                        sources = tuple(sorted({source for name in matches for source in scanned[name]}))
                        results[key] = AttributeInspection("present", sources)
                    except (sqlite3.DatabaseError, ValueError, OSError) as exc:
                        results[key] = AttributeInspection("unresolved", detail=f"Cannot inspect GIS attributes: {exc}")
        except (sqlite3.DatabaseError, OSError, ValueError) as exc:
            for selector in selectors:
                results[(path, selector)] = AttributeInspection(
                    "unresolved", detail=f"Cannot open GIS attributes: {exc}"
                )
    return results, connections
