"""Lightweight QGS/QGZ inspection and explicit, non-destructive source-path repair.

No QGIS installation or geospatial driver is required. Only standard QGIS file
sources are rewritten; unknown providers and complex URI formats are left alone.
"""

from __future__ import annotations

import ntpath
import os
import shutil
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import SplitResult, unquote, urlsplit
from xml.etree import ElementTree as ET
from zipfile import ZipFile

type QgisAuditMode = Literal["existence", "cold-storage-ping"]
type QgisDriveMappings = tuple[tuple[str, str], ...]

_MAX_PROJECT_XML_BYTES = 64 * 1024 * 1024
_REMOTE_PREFIXES = (
    "memory:",
    "memory?",
    "crs=",
    "dbname=",
    "url=",
    "http://",
    "https://",
    "postgres://",
    "postgresql://",
    "wms:",
    "wfs:",
    "service=wms",
    "service=wfs",
    "type=xyz",
    "/vsicurl/",
    "/vsizip/",
    "/vsis3/",
    "pg:",
)
_REMOTE_PROVIDERS = frozenset(
    {
        "wms",
        "wfs",
        "postgres",
        "postgresraster",
        "memory",
        "virtual",
        "vectortile",
        "xyz",
        "arcgismapserver",
        "arcgisfeatureserver",
    }
)


@dataclass(frozen=True, slots=True)
class QgisLayerSource:
    """A project layer and its exact, unmodified data source URI."""

    index: int
    layer_id: str
    name: str
    provider: str
    uri: str


@dataclass(frozen=True, slots=True)
class QgisSourceChange:
    """One proposed source change, identified by the layer's XML position."""

    index: int
    layer_id: str
    name: str
    old_uri: str
    new_uri: str


def _qgs_member(archive: ZipFile) -> str:
    members: list[str] = [info.filename for info in archive.infolist() if info.filename.lower().endswith(".qgs")]
    if len(members) != 1:
        message = f"Expected exactly one QGS document in archive, found {len(members)}"
        raise ValueError(message)
    return members[0]


def read_project_xml(project_path: Path) -> bytes:
    """Read only the project XML; do not open layer data or auxiliary databases."""
    if project_path.suffix.lower() == ".qgz":
        with ZipFile(project_path) as archive:
            name = _qgs_member(archive)
            with archive.open(name) as member:
                document = member.read(_MAX_PROJECT_XML_BYTES + 1)
    elif project_path.suffix.lower() == ".qgs":
        with project_path.open("rb") as project:
            document = project.read(_MAX_PROJECT_XML_BYTES + 1)
    else:
        message = f"Not a QGS/QGZ project: {project_path}"
        raise ValueError(message)

    if len(document) > _MAX_PROJECT_XML_BYTES:
        message = f"QGIS project XML exceeds {_MAX_PROJECT_XML_BYTES} bytes: {project_path}"
        raise ValueError(message)
    return document


def _xml_root(project_path: Path) -> ET.Element:
    # Retain processing instructions and comments when reserialising a repaired project.
    parser: ET.XMLParser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True, insert_pis=True))  # noqa: S314
    return ET.fromstring(read_project_xml(project_path), parser=parser)  # noqa: S314


def project_layer_sources(project_path: Path) -> list[QgisLayerSource]:
    """List layers without deduplicating sources or losing layer ID and provider."""
    root: ET.Element = _xml_root(project_path)
    return [
        QgisLayerSource(
            index=index,
            layer_id=layer.findtext("id") or "",
            name=layer.findtext("layername") or "",
            provider=layer.findtext("provider") or "",
            uri=layer.findtext("datasource") or "",
        )
        for index, layer in enumerate(root.findall("./projectlayers/maplayer"))
    ]


def _path_parts(uri: str) -> tuple[str, str]:
    path, separator, options = uri.partition("|")
    return path.strip(), f"|{options}" if separator else ""


def validate_drive_mappings(drive_mappings: QgisDriveMappings) -> None:
    """Validate explicit mapped-drive to UNC roots without filesystem access."""
    seen: dict[str, str] = {}
    for drive, unc in drive_mappings:
        key = drive.strip().rstrip("\\/").upper()
        if len(key) != 2 or not ("A" <= key[0] <= "Z") or key[1] != ":":
            message = f"Drive mapping must use a drive root such as Q:, got {drive!r}"
            raise ValueError(message)
        root = ntpath.normpath(unc)
        share = ntpath.splitdrive(root)[0]
        if not share.startswith("\\\\") or len(share[2:].split("\\")) != 2 or not ntpath.isabs(root):
            message = f"Drive mapping must use an absolute UNC share such as \\\\server\\share, got {unc!r}"
            raise ValueError(message)
        previous = seen.get(key)
        if previous is not None and previous.casefold() != root.casefold():
            message = f"Conflicting UNC mappings for drive {key}: {previous!r} and {root!r}"
            raise ValueError(message)
        seen[key] = root


def _normalize_windows_network_path(path: str, drive_mappings: QgisDriveMappings) -> str:
    """Normalize Windows components and map absolute drive paths to UNC."""
    validate_drive_mappings(drive_mappings)
    normalized = ntpath.normpath(path)
    drive, tail = ntpath.splitdrive(normalized)
    if not tail.startswith(("\\", "/")):
        return normalized
    for mapped_drive, unc_root in drive_mappings:
        if drive.upper() == mapped_drive.strip().rstrip("\\/").upper():
            return ntpath.normpath(ntpath.join(unc_root, tail.lstrip("\\/")))
    return normalized


def _normalize_local_path(path: Path, drive_mappings: QgisDriveMappings = ()) -> Path:
    """Collapse parent components lexically without dereferencing junctions."""
    normalized = Path(os.path.normpath(path))
    if os.name == "nt" and drive_mappings:
        return Path(_normalize_windows_network_path(str(normalized), drive_mappings))
    return normalized


def ensure_qgd_without_journals(file_path: Path) -> None:
    """Refuse SQLite inspection/copying when a transaction journal is present."""
    for suffix, label in (("-wal", "WAL"), ("-journal", "rollback journal")):
        sidecar = Path(f"{file_path}{suffix}")
        if sidecar.exists():
            message = f"Cannot safely inspect or copy SQLite auxiliary storage while {label} exists: {sidecar}"
            raise ValueError(message)


def local_source_path(
    project_path: Path,
    uri: str,
    *,
    provider: str = "",
    original_project_directory: Path | None = None,
    drive_mappings: QgisDriveMappings = (),
) -> Path | None:
    """Resolve only recognised filesystem sources, without touching the filesystem.

    Windows drive paths cannot be probed using a POSIX Python interpreter.
    Provider-specific database strings are intentionally not guessed.
    """
    raw, _ = _path_parts(uri)
    lowered: str = raw.lower()
    if (
        not raw
        or provider.lower() in _REMOTE_PROVIDERS
        or lowered.startswith(_REMOTE_PREFIXES)
        or ("://" in raw and not lowered.startswith("file:"))
    ):
        return None

    if lowered.startswith("file:"):
        parsed: SplitResult = urlsplit(raw)
        if parsed.scheme.lower() != "file":
            return None
        decoded: str = unquote(parsed.path)
        if parsed.netloc and parsed.netloc.lower() != "localhost":
            candidate = Path(f"//{parsed.netloc}{decoded}")
        else:
            if len(decoded) >= 3 and decoded[0] == "/" and decoded[2] == ":":
                decoded = decoded[1:]
            candidate = Path(decoded)
    else:
        # Unknown URI schemes and provider-specific query syntax are not local paths.
        if "?" in raw or (":" in raw and not (len(raw) >= 2 and raw[1] == ":")):
            return None
        candidate = Path(raw.replace("\\", "/"))

    if os.name != "nt" and len(str(candidate)) >= 2 and str(candidate)[1] == ":":
        return None
    if not candidate.is_absolute():
        base: Path = original_project_directory if original_project_directory is not None else project_path.parent
        candidate: Path = base / candidate
    return _normalize_local_path(candidate.absolute(), drive_mappings)


def probe_local_file(path: Path, *, mode: QgisAuditMode = "existence") -> tuple[bool, str]:
    """Check metadata or explicitly read one byte to request cold-storage retrieval.

    The default existence mode must not open or read source-file contents.
    A ping sends a single-byte read request, but actual rehydration depends on
    the storage implementation. Empty files cannot supply a byte.
    """
    try:
        if mode == "existence":
            if path.is_file():
                return True, "file exists (no content read)"
            return False, "not a regular file or does not exist"
        if mode == "cold-storage-ping":
            with path.open("rb", buffering=0) as stream:
                data = stream.read(1)
            if data:
                return True, "read 1 byte"
            return False, "file is empty; no byte could be read"
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"

    message = f"Unknown QGIS audit mode: {mode}"
    raise ValueError(message)


def _map_root(path: Path, replace_root: tuple[Path, Path] | None, drive_mappings: QgisDriveMappings) -> Path | None:
    if replace_root is None:
        return path
    old_root, new_root = replace_root
    old_base = _normalize_local_path(old_root.absolute(), drive_mappings)
    try:
        relative: Path = path.relative_to(old_base)
    except ValueError:
        return None
    return _normalize_local_path(new_root.absolute(), drive_mappings) / relative


def _render_source_uri(
    destination_project_path: Path,
    original_uri: str,
    path: Path,
    *,
    relative: bool,
    drive_mappings: QgisDriveMappings = (),
) -> str:
    """Render a local source relative to the project that will contain it."""
    original, options = _path_parts(original_uri)
    if original.lower().startswith("file:"):
        # QGIS file URLs may include delimited-text query parameters.
        parsed: SplitResult = urlsplit(original)
        url: str = path.as_uri()
        if parsed.query:
            url += f"?{parsed.query}"
        if parsed.fragment:
            url += f"#{parsed.fragment}"
        return url + options

    if not relative:
        return path.as_posix() + options
    try:
        destination_folder = _normalize_local_path(destination_project_path.parent.absolute(), drive_mappings)
        text = os.path.relpath(path, destination_folder).replace("\\", "/")
    except ValueError:
        # Different Windows drives cannot use relative paths; retain an
        # absolute source rather than corrupting it with a "./" prefix.
        return path.as_posix() + options
    if not text.startswith((".", "/")):
        text = f"./{text}"
    return text + options


def plan_source_path_changes(
    project_path: Path,
    *,
    original_project_directory: Path | None = None,
    replace_root: tuple[Path, Path] | None = None,
    relative: bool = True,
    output_project_path: Path | None = None,
    drive_mappings: QgisDriveMappings = (),
) -> list[QgisSourceChange]:
    """Plan source rewrites without opening any layer files.

    Resolve original sources against the input project (or its previous
    directory), but encode relative URIs against the intended output project.
    Pass output_project_path for previews as well as writes.
    """
    validate_drive_mappings(drive_mappings)
    destination: Path = output_project_path.absolute() if output_project_path is not None else project_path
    changes: list[QgisSourceChange] = []
    for layer in project_layer_sources(project_path):
        source_path: Path | None = local_source_path(
            project_path,
            uri=layer.uri,
            provider=layer.provider,
            original_project_directory=original_project_directory,
            drive_mappings=drive_mappings,
        )
        if source_path is None:
            continue
        target: Path | None = _map_root(source_path, replace_root, drive_mappings)
        if target is None:
            continue
        rewritten: str = _render_source_uri(
            destination_project_path=destination,
            original_uri=layer.uri,
            path=target,
            relative=relative,
            drive_mappings=drive_mappings,
        )
        if rewritten != layer.uri:
            changes.append(
                QgisSourceChange(
                    index=layer.index,
                    layer_id=layer.layer_id,
                    name=layer.name,
                    old_uri=layer.uri,
                    new_uri=rewritten,
                )
            )
    return changes


def _edited_project_xml(project_path: Path, changes: list[QgisSourceChange]) -> bytes:
    root: ET.Element = _xml_root(project_path)
    layers: list[ET.Element] = root.findall("./projectlayers/maplayer")
    for change in changes:
        if not 0 <= change.index < len(layers):
            message = f"Layer position no longer present: {change.index}"
            raise ValueError(message)
        elem: ET.Element | None = layers[change.index].find("datasource")
        if elem is None or (elem.text or "") != change.old_uri:
            message = f"Layer source changed since preview: {change.name}"
            raise ValueError(message)
        elem.text = change.new_uri
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _copy_qgz_with_xml(project_path: Path, output: ZipFile, edited_xml: bytes) -> None:
    with ZipFile(project_path) as original:
        xml_member: str = _qgs_member(original)
        for info in original.infolist():
            if info.filename == xml_member:
                output.writestr(info, edited_xml)
            elif info.is_dir():
                output.writestr(info, b"")
            else:
                with original.open(info) as src, output.open(info, "w") as dest:
                    shutil.copyfileobj(src, dest)


def _remove_if_our_file(path: Path, created_identity: tuple[int, int] | None) -> None:
    if created_identity is None:
        return
    with suppress(FileNotFoundError):
        st: os.stat_result = path.lstat()
        if (st.st_dev, st.st_ino) == created_identity and not path.is_symlink():
            path.unlink()


def write_project_path_changes(
    project_path: Path,
    output_path: Path,
    changes: list[QgisSourceChange],
) -> None:
    """Create a repaired copy, keeping all QGZ members or companion QGS/QGD data.

    Never overwrite a pre-existing file. An unsuccessful partial output is
    removed only when it still has the identity created by this operation.
    """
    if not changes:
        message = "No source changes to write"
        raise ValueError(message)
    if project_path.suffix.lower() != output_path.suffix.lower():
        message = "Output must use the same QGS/QGZ format as the input"
        raise ValueError(message)
    if project_path.absolute() == output_path.absolute() or output_path.exists() or output_path.is_symlink():
        message = f"Output already exists or would replace the input: {output_path}"
        raise FileExistsError(message)

    old_qgd: Path | None = project_path.with_suffix(".qgd") if project_path.suffix.lower() == ".qgs" else None
    new_qgd: Path = output_path.with_suffix(".qgd")
    if old_qgd is not None and (new_qgd.exists() or new_qgd.is_symlink()):
        message: str = f"Auxiliary output already exists: {new_qgd}"
        raise FileExistsError(message)

    if old_qgd is not None and old_qgd.exists():
        # Both WAL and rollback-journal modes can contain uncommitted edits.
        ensure_qgd_without_journals(old_qgd)

    edited_xml: bytes = _edited_project_xml(project_path, changes)
    created_project: tuple[int, int] | None = None
    created_aux: tuple[int, int] | None = None
    completed = False
    try:
        with output_path.open("xb") as stream:
            stat: os.stat_result = os.fstat(stream.fileno())
            created_project = (stat.st_dev, stat.st_ino)
            if project_path.suffix.lower() == ".qgz":
                with ZipFile(stream, "w") as output:
                    _copy_qgz_with_xml(project_path, output, edited_xml)
            else:
                stream.write(edited_xml)
        if old_qgd is not None and old_qgd.is_file():
            with old_qgd.open("rb") as source, new_qgd.open("xb") as dest:
                stat = os.fstat(dest.fileno())
                created_aux = (stat.st_dev, stat.st_ino)
                shutil.copyfileobj(source, dest)
            # Refuse a transaction that became visible while copying.
            ensure_qgd_without_journals(old_qgd)
        completed = True
    finally:
        if not completed:
            _remove_if_our_file(path=output_path, created_identity=created_project)
            _remove_if_our_file(path=new_qgd, created_identity=created_aux)
