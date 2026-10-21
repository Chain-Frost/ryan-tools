"""Read-only, deduplicated discovery of TUFLOW GIS attribute-linked input files.

Only the declared file-valued field positions for recognised TUFLOW layer types
are inspected. GeoPackages are opened once per distinct file, not per run.
Unsupported sources/schemas remain unresolved rather than silently passing.
"""

from __future__ import annotations

import csv
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
    identifiers: tuple[str, ...] = ()
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


def _scan_layer(  # noqa: C901 - safety-critical channel classification
    connection: sqlite3.Connection, layer: str, geometry_type: str, path: Path
) -> tuple[tuple[Path, ...], tuple[str, ...]]:
    """Read only file-valued attributes; return database identifiers separately.

    TUFLOW 1d_nwk Q and P channels and 1d_pit Q/VPI pits use an inlet
    identifier from the ECF Pit Inlet / Depth Discharge Database. M and QO
    channel matrices instead name files directly. Ignored network rows
    must never introduce input dependencies.
    """
    index = _file_field_index(layer)
    if index is None:
        message = f"Unsupported GIS attribute-linked layer type: {layer}"
        raise ValueError(message)
    network = bool(re.match(r"^1d_nwk[eb]?_", layer, flags=re.IGNORECASE))
    pit = bool(re.match(r"^1d_pit_", layer, flags=re.IGNORECASE))
    if network and geometry_type.casefold() not in {"linestring", "multilinestring"}:
        if geometry_type.casefold() != "geometry":
            return (), ()
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
    attrs = [name for name, pk in columns if not pk and str(name).casefold() not in geom_names]
    if len(attrs) <= index or ((network or pit) and len(attrs) <= 2):
        message = f"Required file-reference attribute index {index} absent from {layer}"
        raise ValueError(message)
    field = _quote_identifier(str(attrs[index]))
    if network:
        ignored = _quote_identifier(str(attrs[2]))
        channel_type = _quote_identifier(str(attrs[1]))
        # All identifiers are sourced from PRAGMA metadata and safely quoted.
        sql = f"SELECT {channel_type}, {ignored}, {field} FROM {quoted}"  # noqa: S608
    elif pit:
        channel_type = _quote_identifier(str(attrs[1]))
        sql = f"SELECT {channel_type}, {field} FROM {quoted}"  # noqa: S608
    else:
        sql = f"SELECT {field} FROM {quoted}"  # noqa: S608

    sources: set[Path] = set()
    database_identifiers: set[str] = set()
    for row in connection.execute(sql):
        value = row[-1]
        if network and isinstance(row[1], str) and row[1].strip().casefold() in {"y", "t"}:
            continue
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            message = f"Non-text GIS file-reference attribute in {layer}"
            raise ValueError(message)
        kind = row[0].strip().upper() if (network or pit) and isinstance(row[0], str) else ""
        if network:
            if kind in {"Q", "P"}:
                database_identifiers.add(value.strip())
                continue
            if kind == "QO":
                if "|" not in value:
                    message = f"QO channel lacks control ID and matrix file delimiter in {layer}"
                    raise ValueError(message)
                sources.update(_attribute_file_paths(value, network_m=False, parent=path.parent))
                continue
            if kind != "M":
                # Other channels use Inlet_Type for control names or unused
                # values. They are not direct file paths.
                continue
        elif pit:
            if kind in {"Q", "VPI", "I"}:
                database_identifiers.add(value.strip())
            # Other pit types do not use Inlet_Type to reference a file.
            continue
        sources.update(_attribute_file_paths(value, network_m=network, parent=path.parent))
    return tuple(sorted(sources)), tuple(sorted(database_identifiers))


def read_pit_inlet_database(path: Path) -> dict[str, Path]:
    """Map depth-discharge curve names to Source CSVs using the TUFLOW schema.

    Table 5.27: first column is Name, second is Source. Database CSVs are
    read only once by the caller and are never modified. Reject ambiguous
    or unsupported tables rather than guessing at curve file dependencies.
    """
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames or []
        if len(fields) < 2 or fields[0].strip().casefold() != "name" or fields[1].strip().casefold() != "source":
            message = f"Pit inlet database needs Name and Source columns: {path}"
            raise ValueError(message)
        mapping: dict[str, Path] = {}
        for row in reader:
            name = str(row.get(fields[0]) or "").strip()
            source = str(row.get(fields[1]) or "").strip()
            if not name or not source or "<<" in source or ">>" in source:
                message = f"Invalid pit inlet database entry in {path}: {name!r}"
                raise ValueError(message)
            key = name.casefold()
            if key in mapping:
                message = f"Duplicate pit inlet identifier in {path}: {name}"
                raise ValueError(message)
            paths = _attribute_file_paths(source, network_m=False, parent=path.parent)
            if len(paths) != 1:
                message = f"Invalid pit inlet Source reference in {path}: {name}"
                raise ValueError(message)
            mapping[key] = paths[0]
        return mapping


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
                scanned: dict[str, tuple[tuple[Path, ...], tuple[str, ...]]] = {}
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
                        sources = tuple(sorted({source for name in matches for source in scanned[name][0]}))
                        identifiers = tuple(sorted({identifier for name in matches for identifier in scanned[name][1]}))
                        results[key] = AttributeInspection("present", sources, identifiers)
                    except (sqlite3.DatabaseError, ValueError, OSError) as exc:
                        results[key] = AttributeInspection("unresolved", detail=f"Cannot inspect GIS attributes: {exc}")
        except (sqlite3.DatabaseError, OSError, ValueError) as exc:
            for selector in selectors:
                results[(path, selector)] = AttributeInspection(
                    "unresolved", detail=f"Cannot open GIS attributes: {exc}"
                )
    return results, connections
