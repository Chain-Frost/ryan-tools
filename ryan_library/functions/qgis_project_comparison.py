"""Read-only structural comparison of QGIS project layers and coordinate systems.

This compares QGS XML (including the QGS member of a QGZ), not data-source
contents or QGIS-rendered state. File-capable providers are not assumed to
represent accessible local files; unknown providers remain explicitly unknown.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from xml.etree import ElementTree as ET

from .qgis_project import read_project_xml

type QgisProviderCategory = Literal["file-capable", "service", "non-file", "unknown"]
type QgisLayerChangeKind = Literal["added", "removed", "modified"]

_FILE_CAPABLE_PROVIDERS = frozenset({"gdal", "ogr"})
_SERVICE_PROVIDERS = frozenset(
    {"wms", "wfs", "postgres", "postgresraster", "vectortile", "xyz", "arcgismapserver", "arcgisfeatureserver"}
)
_NON_FILE_PROVIDERS = frozenset({"memory", "virtual"})


@dataclass(frozen=True, slots=True)
class QgisLayerStructure:
    """Unmodified layer metadata. The provider category is not a URI parser."""

    layer_id: str
    name: str
    provider: str
    provider_category: QgisProviderCategory
    datasource: str
    crs: str


@dataclass(frozen=True, slots=True)
class QgisLayerStructuralChange:
    """One layer addition, removal or modification, matched by stable layer ID."""

    layer_id: str
    kind: QgisLayerChangeKind
    changed_fields: tuple[str, ...]
    before: QgisLayerStructure | None
    after: QgisLayerStructure | None


@dataclass(frozen=True, slots=True)
class QgisProjectComparison:
    """Machine-readable comparison of two project XML documents."""

    original_project: str
    updated_project: str
    original_crs: str
    updated_crs: str
    layer_changes: tuple[QgisLayerStructuralChange, ...]

    @property
    def project_crs_changed(self) -> bool:
        return self.original_crs != self.updated_crs

    @property
    def has_changes(self) -> bool:
        return self.project_crs_changed or bool(self.layer_changes)


def _provider_category(provider: str) -> QgisProviderCategory:
    key = provider.casefold()
    if key in _FILE_CAPABLE_PROVIDERS:
        return "file-capable"
    if key in _SERVICE_PROVIDERS:
        return "service"
    if key in _NON_FILE_PROVIDERS:
        return "non-file"
    return "unknown"


def _project_structure(path: Path) -> tuple[str, dict[str, QgisLayerStructure]]:
    root = ET.fromstring(read_project_xml(path))  # noqa: S314
    project_crs = root.findtext("./projectCrs/spatialrefsys/authid") or ""
    layers: dict[str, QgisLayerStructure] = {}
    for index, element in enumerate(root.findall("./projectlayers/maplayer"), start=1):
        layer_id = element.findtext("id") or ""
        if not layer_id.strip():
            message = f"QGIS layer {index} in {path} has no ID; comparison by ID is unsafe"
            raise ValueError(message)
        if layer_id in layers:
            message = f"Duplicate QGIS layer ID {layer_id!r} in {path}; comparison by ID is unsafe"
            raise ValueError(message)
        provider = element.findtext("provider") or ""
        layers[layer_id] = QgisLayerStructure(
            layer_id=layer_id,
            name=element.findtext("layername") or "",
            provider=provider,
            provider_category=_provider_category(provider),
            datasource=element.findtext("datasource") or "",
            crs=element.findtext("./srs/spatialrefsys/authid") or "",
        )
    return project_crs, layers


def compare_qgis_projects(original_project: Path, updated_project: Path) -> QgisProjectComparison:
    """Compare two QGS/QGZ files by layer ID without opening sources or writing files.

    Changes cover layer presence, names, provider identifiers, verbatim source
    URIs, layer CRS authids and project CRS authid. Layer ordering, visibility,
    groups, relations, styling and provider interpretation are out of scope.
    Missing/duplicate layer IDs raise rather than invent a matching heuristic.
    """
    original_crs, original_layers = _project_structure(original_project)
    updated_crs, updated_layers = _project_structure(updated_project)
    changes: list[QgisLayerStructuralChange] = []

    for layer_id in sorted(original_layers.keys() | updated_layers.keys()):
        before = original_layers.get(layer_id)
        after = updated_layers.get(layer_id)
        if before is None:
            changes.append(QgisLayerStructuralChange(layer_id, "added", (), None, after))
            continue
        if after is None:
            changes.append(QgisLayerStructuralChange(layer_id, "removed", (), before, None))
            continue
        fields: list[str] = []
        if before.name != after.name:
            fields.append("name")
        if before.provider != after.provider:
            fields.append("provider")
        if before.datasource != after.datasource:
            fields.append("datasource")
        if before.crs != after.crs:
            fields.append("crs")
        if fields:
            changes.append(QgisLayerStructuralChange(layer_id, "modified", tuple(fields), before, after))

    return QgisProjectComparison(
        original_project=str(original_project),
        updated_project=str(updated_project),
        original_crs=original_crs,
        updated_crs=updated_crs,
        layer_changes=tuple(changes),
    )
