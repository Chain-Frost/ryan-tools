"""Read-only QGIS project structural comparison, including unknown providers."""

from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from ryan_library.functions.qgis_project_comparison import compare_qgis_projects


def _project_xml(*layers: tuple[str, str, str, str, str], project_crs: str = "EPSG:7850") -> bytes:
    root = ET.Element("qgis", version="3.34")
    crs = ET.SubElement(ET.SubElement(root, "projectCrs"), "spatialrefsys")
    ET.SubElement(crs, "authid").text = project_crs
    project_layers = ET.SubElement(root, "projectlayers")
    for identifier, name, provider, source, layer_crs in layers:
        layer = ET.SubElement(project_layers, "maplayer")
        for tag, value in (("id", identifier), ("layername", name), ("provider", provider), ("datasource", source)):
            ET.SubElement(layer, tag).text = value
        layer_srs = ET.SubElement(ET.SubElement(layer, "srs"), "spatialrefsys")
        ET.SubElement(layer_srs, "authid").text = layer_crs
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _qgz(path: Path, xml: bytes) -> None:
    with ZipFile(path, "w") as archive:
        archive.writestr(f"{path.stem}.qgs", xml)
        archive.writestr(f"{path.stem}.qgd", b"auxiliary SQLite bytes must stay opaque")
        archive.writestr("unknown/keep.dat", b"custom metadata should remain byte-identical")


def test_qgs_qgz_diff_preserves_sources_archives_and_opaque_providers(tmp_path: Path) -> None:
    before = tmp_path / "old.qgs"
    after = tmp_path / "new.qgz"
    source = tmp_path / "roads.gpkg"
    source.write_bytes(b"real data sentinel")
    before.write_bytes(
        _project_xml(
            ("roads", "Roads", "ogr", "./roads.gpkg|layername=roads", "EPSG:7850"),
            ("removed", "Remote", "wms", "url=https://example.invalid/tiles", "EPSG:4326"),
        )
    )
    _qgz(
        after,
        _project_xml(
            ("roads", "New Roads", "customProvider", "vendor:?path=x", "EPSG:7851"),
            ("new-layer", "New", "gdal", "Z:/data/missing.tif", "EPSG:7851"),
            project_crs="EPSG:7851",
        ),
    )
    input_bytes = {path: path.read_bytes() for path in (before, after, source)}
    original_files = set(tmp_path.iterdir())

    result = compare_qgis_projects(before, after)

    assert result.original_crs == "EPSG:7850"
    assert result.updated_crs == "EPSG:7851"
    assert result.project_crs_changed is True
    assert result.has_changes is True
    assert [(change.layer_id, change.kind) for change in result.layer_changes] == [
        ("new-layer", "added"),
        ("removed", "removed"),
        ("roads", "modified"),
    ]
    renamed = result.layer_changes[2]
    assert renamed.changed_fields == ("name", "provider", "datasource", "crs")
    old_roads = renamed.before
    new_roads = renamed.after
    assert old_roads is not None
    assert old_roads.provider_category == "file-capable"
    assert old_roads.datasource == "./roads.gpkg|layername=roads"
    assert new_roads is not None
    assert new_roads.provider == "customProvider"
    assert new_roads.provider_category == "unknown"
    assert new_roads.datasource == "vendor:?path=x"
    removed_layer = result.layer_changes[1].before
    assert removed_layer is not None
    assert removed_layer.provider_category == "service"
    assert set(tmp_path.iterdir()) == original_files
    assert {path: path.read_bytes() for path in input_bytes} == input_bytes
    assert not (tmp_path / "data").exists()


def test_layer_reordering_does_not_create_false_changes(tmp_path: Path) -> None:
    a = ("A", "First", "ogr", "./data.gpkg|layername=a", "EPSG:7850")
    b = ("B", "Second", "memory", "memory:", "")
    original = tmp_path / "a.qgs"
    updated = tmp_path / "b.qgs"
    original.write_bytes(_project_xml(a, b))
    updated.write_bytes(_project_xml(b, a))
    diff = compare_qgis_projects(original, updated)
    assert diff.layer_changes == ()
    assert diff.has_changes is False
    assert diff.project_crs_changed is False


@pytest.mark.parametrize("identifiers", [("",), ("duplicated", "duplicated")])
def test_ambiguous_layer_ids_fail_instead_of_pairing_by_position(tmp_path: Path, identifiers: tuple[str, ...]) -> None:
    original = tmp_path / "a.qgs"
    updated = tmp_path / "b.qgs"
    layers = tuple((identifier, "Layer", "ogr", "./data.gpkg", "") for identifier in identifiers)
    original.write_bytes(_project_xml(*layers))
    updated.write_bytes(_project_xml(("valid", "Layer", "ogr", "./data.gpkg", "")))
    bytes_before = original.read_bytes()
    with pytest.raises(ValueError, match="no ID|Duplicate QGIS layer ID"):
        compare_qgis_projects(original, updated)
    assert original.read_bytes() == bytes_before


def test_no_hidden_project_mutation_and_reject_invalid_qgz(tmp_path: Path) -> None:
    qgs = tmp_path / "old.qgs"
    qgs.write_bytes(_project_xml(("x", "Layer", "gdal", "./missing.tif", "")))
    qgz = tmp_path / "invalid.qgz"
    with ZipFile(qgz, "w") as archive:
        archive.writestr("aux.qgd", b"qgd")
    snapshot = qgz.read_bytes()
    with pytest.raises(ValueError, match="exactly one QGS"):
        compare_qgis_projects(qgs, qgz)
    assert qgz.read_bytes() == snapshot
