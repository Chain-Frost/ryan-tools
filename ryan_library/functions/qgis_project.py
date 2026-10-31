"""Lightweight QGS/QGZ inspection and explicit, non-destructive source-path repair.

No QGIS installation or geospatial driver is required. Only standard QGIS file
sources are rewritten; unknown providers and complex URI formats are left alone.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET
from zipfile import ZipFile

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
    {"wms", "wfs", "postgres", "postgresraster", "memory", "virtual", "vectortile", "xyz",
     "arcgismapserver", "arcgisfeatureserver"}
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
    members = [info.filename for info in archive.infolist() if info.filename.lower().endswith(".qgs")]
    if len(members) != 1:
        raise ValueError(f"Expected exactly one QGS document in archive, found {len(members)}")
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
        raise ValueError(f"Not a QGS/QGZ project: {project_path}")

    if len(document) > _MAX_PROJECT_XML_BYTES:
        raise ValueError(f"QGIS project XML exceeds {_MAX_PROJECT_XML_BYTES} bytes: {project_path}")
    return document


def _xml_root(project_path: Path) -> ET.Element:
    # Retain processing instructions and comments when reserialising a repaired project.
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True, insert_pis=True))
    return ET.fromstring(read_project_xml(project_path), parser=parser)  # noqa: S314


def project_layer_sources(project_path: Path) -> list[QgisLayerSource]:
    """List layers without deduplicating sources or losing layer ID and provider."""
    root = _xml_root(project_path)
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


def local_source_path(
    project_path: Path,
    uri: str,
    *,
    provider: str = "",
    original_project_directory: Path | None = None,
) -> Path | None:
    """Resolve only recognised filesystem sources, without touching the filesystem.

    Windows drive paths cannot be probed using a POSIX Python interpreter.
    Provider-specific database strings are intentionally not guessed.
    """
    raw, _ = _path_parts(uri)
    lowered = raw.lower()
    if (
        not raw
        or provider.lower() in _REMOTE_PROVIDERS
        or lowered.startswith(_REMOTE_PREFIXES)
        or ("://" in raw and not lowered.startswith("file:"))
    ):
        return None

    if lowered.startswith("file:"):
        parsed = urlsplit(raw)
        if parsed.scheme.lower() != "file":
            return None
        decoded = unquote(parsed.path)
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
        base = original_project_directory if original_project_directory is not None else project_path.parent
        candidate = base / candidate
    return candidate.absolute()


def probe_local_file(path: Path, *, read_one_byte: bool = True) -> tuple[bool, str]:
    """Return success and diagnostic; a one-byte open requests cold-storage hydration."""
    try:
        if read_one_byte:
            with path.open("rb") as stream:
                data = stream.read(1)
            return True, "read 1 byte" if data else "opened empty file"
        if path.is_file():
            return True, "file exists"
        return False, "not a regular file or does not exist"
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"


def plan_source_path_changes(
    project_path: Path,
    *,
    original_project_directory: Path | None = None,
    replace_root: tuple[Path, Path] | None = None,
    relative: bool = True,
) -> list[QgisSourceChange]:
    """Plan safe local-file changes. No files are opened or written.

    original_project_directory is where the project originally lived before it
    was moved without its relative sources being updated.
    replace_root maps a path subtree to a new subtree, respecting path boundaries.
    """
    changes: list[QgisSourceChange] = []
    for layer in project_layer_sources(project_path):
        path = local_source_path(
            project_path, layer.uri, provider=layer.provider,
            original_project_directory=original_project_directory,
        )
        if path is None:
            continue

        if replace_root is not None:
            old_root, new_root = replace_root
            try:
                suffix = path.relative_to(old_root.absolute())
            except ValueError:
                continue
            path = new_root.absolute() / suffix

        original, options = _path_parts(layer.uri)
        if original.lower().startswith("file:"):
            # Keep file URLs valid and their query string intact (e.g. delimited text).
            parsed = urlsplit(original)
            try:
                rewritten = path.as_uri()
            except ValueError:
                continue
            if parsed.query:
                rewritten += f"?{parsed.query}"
            if parsed.fragment:
                rewritten += f"#{parsed.fragment}"
        elif relative:
            try:
                rewritten = os.path.relpath(path, project_path.parent).replace("\\", "/")
            except ValueError:
                # Relative paths cannot cross Windows drive boundaries.
                rewritten = path.as_posix()
            if not rewritten.startswith((".", "/")):
                rewritten = f"./{rewritten}"
        else:
            rewritten = path.as_posix()

        updated_uri = rewritten + options
        if updated_uri != layer.uri:
            changes.append(
                QgisSourceChange(
                    index=layer.index,
                    layer_id=layer.layer_id,
                    name=layer.name,
                    old_uri=layer.uri,
                    new_uri=updated_uri,
                )
            )
    return changes


def write_project_path_changes(
    project_path: Path,
    output_path: Path,
    changes: list[QgisSourceChange],
) -> None:
    """Save an edited COPY without overwriting any original or existing output.

    Other QGZ archive members (including QGD bytes) are preserved. For QGS, a
    companion QGD is copied alongside the new project under the new stem.
    """
    if not changes:
        raise ValueError("No source changes to write")
    if project_path.suffix.lower() != output_path.suffix.lower():
        raise ValueError("Output must use the same QGS/QGZ format as the input")
    if project_path.absolute() == output_path.absolute() or output_path.exists() or output_path.is_symlink():
        raise FileExistsError(f"Output already exists or would replace the input: {output_path}")

    original_qgd = project_path.with_suffix(".qgd") if project_path.suffix.lower() == ".qgs" else None
    target_qgd = output_path.with_suffix(".qgd")
    if original_qgd is not None and original_qgd.exists() and (target_qgd.exists() or target_qgd.is_symlink()):
        raise FileExistsError(f"Auxiliary output already exists: {target_qgd}")

    root = _xml_root(project_path)
    layers = root.findall("./projectlayers/maplayer")
    for change in changes:
        if not 0 <= change.index < len(layers):
            raise ValueError(f"Layer position no longer present: {change.index}")
        elem = layers[change.index].find("datasource")
        if elem is None or (elem.text or "") != change.old_uri:
            raise ValueError(f"Layer source changed since preview: {change.name}")
        elem.text = change.new_uri

    edited_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    try:
        if project_path.suffix.lower() == ".qgz":
            with ZipFile(project_path) as original, ZipFile(output_path, "x") as output:
                xml_member = _qgs_member(original)
                for info in original.infolist():
                    if info.filename == xml_member:
                        output.writestr(info, edited_xml)
                    else:
                        with original.open(info) as src, output.open(info, "w") as dest:
                            shutil.copyfileobj(src, dest)
        else:
            with output_path.open("xb") as output:
                output.write(edited_xml)
            if original_qgd is not None and original_qgd.exists():
                with original_qgd.open("rb") as src, target_qgd.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
    except Exception:
        # Outputs are exclusive new names; never remove files that existed before.
        # Recheck before removing to avoid following symlink substitutions.
        if output_path.exists() and not output_path.is_symlink():
            output_path.unlink()
        if original_qgd is not None and target_qgd.exists() and not target_qgd.is_symlink():
            target_qgd.unlink()
        raise
