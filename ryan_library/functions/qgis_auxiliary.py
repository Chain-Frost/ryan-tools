"""Inspect QGIS auxiliary .qgd SQLite data without QGIS or opening data layers.

The database may be beside a QGS, embedded in a QGZ, or supplied directly.
Inspection is read-only; row values are omitted unless explicitly requested.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile, ZipInfo

from .qgis_project import ensure_qgd_without_journals, project_layer_sources

_MAX_EMBEDDED_QGD_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class AuxiliaryTable:
    """One SQLite table, including the corresponding project layer if known."""

    name: str
    layer_name: str | None
    columns: tuple[str, ...]
    row_count: int
    sample_rows: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class AuxiliarySummary:
    """Summary of auxiliary content not stored in project XML."""

    location: str
    size_bytes: int
    tables: tuple[AuxiliaryTable, ...]


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _display_value(value: object) -> str:
    if isinstance(value, bytes):
        return f"<binary data: {len(value)} bytes>"
    text = str(value)
    return text if len(text) <= 120 else f"{text[:117]}..."


def _inspect_database(
    file_path: Path,
    *,
    location: str,
    layer_names: dict[str, str],
    sample_rows: int,
) -> AuxiliarySummary:
    ensure_qgd_without_journals(file_path)
    if file_path.stat().st_size == 0:
        return AuxiliarySummary(location=location, size_bytes=0, tables=())

    tables: list[AuxiliaryTable] = []
    # Immutable read-only mode avoids creating SQLite journal and shared-memory
    # files when auditing source auxiliary databases outside QGIS.
    with closing(sqlite3.connect(f"{file_path.absolute().as_uri()}?mode=ro&immutable=1", uri=True)) as db:
        db.execute("PRAGMA query_only = ON")
        db.execute("PRAGMA trusted_schema = OFF")
        names = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for (name,) in names:
            identifier: str = _quoted_identifier(str(name))
            columns: tuple[str, ...] = tuple(str(row[1]) for row in db.execute(f"PRAGMA table_info({identifier})"))
            result = db.execute(f"SELECT count(*) FROM {identifier}").fetchone()  # noqa: S608 - quoted SQLite name
            if result is None:
                message = f"Could not count rows in SQLite table {name}"
                raise ValueError(message)
            count = int(result[0])
            rows: tuple[tuple[str, ...], ...] = ()
            if sample_rows:
                values = db.execute(f"SELECT * FROM {identifier} LIMIT ?", (sample_rows,)).fetchall()  # noqa: S608
                rows = tuple(tuple(_display_value(value) for value in row) for row in values)
            tables.append(
                AuxiliaryTable(
                    name=str(name),
                    layer_name=layer_names.get(str(name)),
                    columns=columns,
                    row_count=count,
                    sample_rows=rows,
                )
            )
    ensure_qgd_without_journals(file_path)
    return AuxiliarySummary(location=location, size_bytes=file_path.stat().st_size, tables=tuple(tables))


def _inspect_embedded_qgd(project_path: Path, layer_names: dict[str, str], sample_rows: int) -> AuxiliarySummary | None:
    with ZipFile(project_path) as archive:
        members: list[ZipInfo] = [info for info in archive.infolist() if info.filename.lower().endswith(".qgd")]
        if not members:
            return None
        if len(members) != 1:
            message: str = f"Expected at most one QGD in {project_path}, found {len(members)}"
            raise ValueError(message)
        member: ZipInfo = members[0]
        if member.file_size > _MAX_EMBEDDED_QGD_BYTES:
            message = f"Embedded QGD is over the inspection limit: {member.file_size} bytes"
            raise ValueError(message)
        with TemporaryDirectory(prefix="qgis_qgd_inspect_") as temporary:
            copy: Path = Path(temporary) / "auxiliary.qgd"
            with archive.open(member) as src, copy.open("xb") as dest:
                copied = 0
                while chunk := src.read(min(1024 * 1024, _MAX_EMBEDDED_QGD_BYTES - copied + 1)):
                    copied += len(chunk)
                    if copied > _MAX_EMBEDDED_QGD_BYTES:
                        message = "Embedded QGD exceeds the inspection limit"
                        raise ValueError(message)
                    dest.write(chunk)
            return _inspect_database(
                file_path=copy,
                location=f"{project_path}!{member.filename}",
                layer_names=layer_names,
                sample_rows=sample_rows,
            )


def inspect_auxiliary_storage(project_path: Path, *, sample_rows: int = 0) -> AuxiliarySummary | None:
    """Summarise a QGD beside a QGS, embedded in a QGZ, or supplied directly.

    Table names matching project layer IDs identify the corresponding QGIS
    layer. Unrecognised tables are reported without guessing. Samples are
    explicitly opt-in and bounded to five records per table.
    """
    if not 0 <= sample_rows <= 5:
        message = "sample_rows must be between 0 and 5"
        raise ValueError(message)

    suffix = project_path.suffix.lower()
    layer_names: dict[str, str] = {}
    if suffix in {".qgs", ".qgz"}:
        layer_names = {layer.layer_id: layer.name for layer in project_layer_sources(project_path) if layer.layer_id}

    if suffix == ".qgd":
        if not project_path.is_file():
            raise FileNotFoundError(project_path)
        return _inspect_database(
            project_path, location=str(project_path), layer_names=layer_names, sample_rows=sample_rows
        )

    if suffix == ".qgs":
        companion = project_path.with_suffix(".qgd")
        if not companion.is_file():
            return None
        return _inspect_database(companion, location=str(companion), layer_names=layer_names, sample_rows=sample_rows)

    if suffix == ".qgz":
        return _inspect_embedded_qgd(project_path, layer_names, sample_rows)
    message = f"Not a QGS, QGZ or QGD file: {project_path}"
    raise ValueError(message)
