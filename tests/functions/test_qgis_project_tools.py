"""Synthetic, read-only QGS/QGZ cold-storage, repair and auxiliary SQLite checks."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from ryan_library.functions.qgis_auxiliary import inspect_auxiliary_storage
from ryan_library.functions.qgis_project import (
    local_source_path,
    plan_source_path_changes,
    probe_local_file,
    project_layer_sources,
    read_project_xml,
    write_project_path_changes,
)


def _xml(*layers: tuple[str, str, str, str]) -> bytes:
    root = ET.Element("qgis", version="3.34")
    parent = ET.SubElement(root, "projectlayers")
    for identifier, name, provider, uri in layers:
        layer = ET.SubElement(parent, "maplayer")
        for tag, value in (("id", identifier), ("layername", name), ("provider", provider), ("datasource", uri)):
            ET.SubElement(layer, tag).text = value
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _qgz(path: Path, xml: bytes, *, qgd: bytes | None = None) -> None:
    with ZipFile(path, "w") as zipfile:
        zipfile.writestr(f"{path.stem}.qgs", xml)
        zipfile.writestr("custom/", b"")
        zipfile.writestr("custom/keep.txt", b"keep this unknown project member")
        if qgd is not None:
            zipfile.writestr(f"{path.stem}.qgd", qgd)


def _qgd(path: Path, layer_id: str) -> bytes:
    with sqlite3.connect(path) as db:
        db.execute(f'CREATE TABLE "{layer_id}" (ASPK TEXT, labeling_positionx REAL, labeling_positiony REAL)')  # noqa: S608
        db.execute(f'INSERT INTO "{layer_id}" VALUES (?, ?, ?)', ("feature-a", 123.4, 567.8))  # noqa: S608
        db.commit()
    return path.read_bytes()


def test_layer_sources_are_not_deduplicated_and_do_not_read_linked_files(tmp_path: Path) -> None:
    project = tmp_path / "project.qgs"
    project.write_bytes(
        _xml(
            ("id-one", "One", "ogr", "missing.gpkg|layername=one"),
            ("id-two", "Two", "ogr", "missing.gpkg|layername=two"),
            ("id-three", "XYZ", "wms", "type=xyz&url=https://example.com/tiles"),
        )
    )
    before = project.read_bytes()
    layers = project_layer_sources(project)
    assert [(layer.layer_id, layer.name) for layer in layers] == [
        ("id-one", "One"), ("id-two", "Two"), ("id-three", "XYZ")
    ]
    assert local_source_path(project, layers[0].uri, provider=layers[0].provider) == tmp_path / "missing.gpkg"
    assert local_source_path(project, layers[2].uri, provider=layers[2].provider) is None
    assert read_project_xml(project) == before


def test_one_byte_probe_and_exists_only_are_non_mutating(tmp_path: Path) -> None:
    source = tmp_path / "cold data.tif"
    source.write_bytes(b"0123456789")
    before = source.read_bytes()
    assert probe_local_file(source) == (True, "read 1 byte")
    assert probe_local_file(source, read_one_byte=False) == (True, "file exists")
    assert probe_local_file(tmp_path / "missing.tif")[0] is False
    assert source.read_bytes() == before


def test_rebase_moved_project_without_touching_source_data(tmp_path: Path) -> None:
    old = tmp_path / "Original"
    new = tmp_path / "Relocated"
    (old / "GIS").mkdir(parents=True)
    (old / "Data").mkdir()
    (new / "GIS").mkdir(parents=True)
    data = old / "Data" / "terrain.tif"
    data.write_bytes(b"raster")
    project = new / "GIS" / "moved.qgs"
    project.write_bytes(_xml(("terrain-id", "Terrain", "gdal", "../Data/terrain.tif")))
    previous_bytes = project.read_bytes()

    changes = plan_source_path_changes(project, original_project_directory=old / "GIS")
    assert len(changes) == 1
    assert changes[0].old_uri == "../Data/terrain.tif"
    assert changes[0].new_uri == "../../Original/Data/terrain.tif"
    assert project.read_bytes() == previous_bytes

    target = new / "GIS" / "fixed.qgs"
    write_project_path_changes(project, target, changes)
    assert project.read_bytes() == previous_bytes
    assert data.read_bytes() == b"raster"
    layer = project_layer_sources(target)[0]
    assert local_source_path(target, layer.uri, provider=layer.provider) == data


def test_root_mapping_respects_component_boundaries(tmp_path: Path) -> None:
    old = tmp_path / "Old"
    new = tmp_path / "New"
    project = tmp_path / "model.qgs"
    project.write_bytes(
        _xml(
            ("one", "A", "ogr", f"{old / 'roads.gpkg'}|layername=roads"),
            ("two", "B", "ogr", f"{tmp_path / 'Older' / 'other.gpkg'}|layername=roads"),
            ("three", "Remote", "postgres", "dbname='production' table=roads"),
        )
    )
    changes = plan_source_path_changes(project, replace_root=(old, new), relative=False)
    assert len(changes) == 1
    assert changes[0].new_uri == f"{(new / 'roads.gpkg').as_posix()}|layername=roads"
    assert not (new / "roads.gpkg").exists()


def test_file_url_and_provider_options_preserved_on_rewrite(tmp_path: Path) -> None:
    old = tmp_path / "Old files"
    new = tmp_path / "New files"
    original = old / "roads & drains.gpkg"
    project = tmp_path / "project.qgs"
    project.write_bytes(_xml(("roads", "Roads", "ogr", f"{original.as_uri()}|layername=roads")))
    changes = plan_source_path_changes(project, replace_root=(old, new))
    assert len(changes) == 1
    assert changes[0].new_uri == f"{(new / 'roads & drains.gpkg').as_uri()}|layername=roads"


def test_qgz_repair_preserves_qgd_and_unknown_members(tmp_path: Path) -> None:
    database = tmp_path / "aux.qgd"
    auxiliary_bytes = _qgd(database, "road-layer-id")
    project = tmp_path / "model.qgz"
    _qgz(
        project,
        _xml(("road-layer-id", "Roads", "ogr", "./old/roads.gpkg|layername=roads")),
        qgd=auxiliary_bytes,
    )
    before = project.read_bytes()
    changes = plan_source_path_changes(
        project,
        replace_root=(tmp_path / "old", tmp_path / "new"),
    )
    output = tmp_path / "fixed.qgz"
    write_project_path_changes(project, output, changes)
    assert project.read_bytes() == before
    with ZipFile(project) as old_zip, ZipFile(output) as new_zip:
        assert old_zip.read("model.qgd") == new_zip.read("model.qgd") == auxiliary_bytes
        assert old_zip.read("custom/keep.txt") == new_zip.read("custom/keep.txt")
        assert "custom/" in new_zip.namelist()

    summary = inspect_auxiliary_storage(output)
    assert summary is not None
    assert len(summary.tables) == 1
    assert summary.tables[0].name == "road-layer-id"
    assert summary.tables[0].layer_name == "Roads"
    assert summary.tables[0].columns == ("ASPK", "labeling_positionx", "labeling_positiony")
    assert summary.tables[0].row_count == 1
    assert summary.tables[0].sample_rows == ()


def test_auxiliary_samples_are_opt_in_and_limited(tmp_path: Path) -> None:
    database = tmp_path / "data.qgd"
    _qgd(database, "layer-1")
    before = database.read_bytes()
    basic = inspect_auxiliary_storage(database)
    samples = inspect_auxiliary_storage(database, sample_rows=1)
    assert basic is not None and samples is not None
    assert basic.tables[0].sample_rows == ()
    assert samples.tables[0].sample_rows == (("feature-a", "123.4", "567.8"),)
    assert database.read_bytes() == before
    with pytest.raises(ValueError, match="between 0 and 5"):
        inspect_auxiliary_storage(database, sample_rows=100)


def test_qgs_repair_copies_companion_qgd(tmp_path: Path) -> None:
    project = tmp_path / "model.qgs"
    project.write_bytes(_xml(("labelled", "Labelled", "ogr", "./old.gpkg")))
    companion = project.with_suffix(".qgd")
    database_bytes = _qgd(companion, "labelled")
    changes = plan_source_path_changes(project, relative=False)
    output = tmp_path / "fixed.qgs"
    write_project_path_changes(project, output, changes)
    assert output.with_suffix(".qgd").read_bytes() == database_bytes
    assert companion.read_bytes() == database_bytes
    summary = inspect_auxiliary_storage(output, sample_rows=1)
    assert summary is not None and summary.tables[0].layer_name == "Labelled"


def test_no_overwrite_of_any_output_or_companion(tmp_path: Path) -> None:
    project = tmp_path / "project.qgs"
    project.write_bytes(_xml(("id", "Layer", "ogr", "./data.gpkg")))
    changes = plan_source_path_changes(project, relative=False)
    with pytest.raises(FileExistsError):
        write_project_path_changes(project, project, changes)
    output = tmp_path / "fixed.qgs"
    output.write_text("protected")
    with pytest.raises(FileExistsError):
        write_project_path_changes(project, output, changes)
    assert output.read_text() == "protected"

    output.unlink()
    _qgd(project.with_suffix(".qgd"), "id")
    protected_companion = output.with_suffix(".qgd")
    protected_companion.write_bytes(b"protected")
    with pytest.raises(FileExistsError):
        write_project_path_changes(project, output, changes)
    assert protected_companion.read_bytes() == b"protected"
    assert not output.exists()


def test_invalid_or_missing_qgd_does_not_claim_data(tmp_path: Path) -> None:
    project = tmp_path / "noaux.qgz"
    _qgz(project, _xml(("id", "Layer", "ogr", "./data.gpkg")))
    assert inspect_auxiliary_storage(project) is None
    broken = tmp_path / "invalid.qgd"
    broken.write_bytes(b"not SQLite")
    with pytest.raises(sqlite3.DatabaseError):
        inspect_auxiliary_storage(broken)



def test_qgs_repair_refuses_to_copy_auxiliary_with_uncheckpointed_wal(tmp_path: Path) -> None:
    project = tmp_path / "model.qgs"
    project.write_bytes(_xml(("id", "Labeled", "ogr", "./old.gpkg")))
    auxiliary = project.with_suffix(".qgd")
    _qgd(auxiliary, "id")
    wal = Path(f"{auxiliary}-wal")
    wal.write_bytes(b"simulated active SQLite WAL")
    output = tmp_path / "fixed.qgs"
    changes = plan_source_path_changes(project, relative=False)
    with pytest.raises(ValueError, match="WAL exists"):
        write_project_path_changes(project, output, changes)
    assert not output.exists()
    assert auxiliary.exists()
    assert wal.exists()



def test_auxiliary_inspection_is_immutable_and_rejects_wal_state(tmp_path: Path) -> None:
    auxiliary = tmp_path / "labels.qgd"
    before = _qgd(auxiliary, "label-id")
    summary = inspect_auxiliary_storage(auxiliary)
    assert summary is not None and summary.tables[0].row_count == 1
    assert auxiliary.read_bytes() == before
    assert not Path(f"{auxiliary}-shm").exists()
    assert not Path(f"{auxiliary}-wal").exists()

    wal = Path(f"{auxiliary}-wal")
    wal.write_bytes(b"simulated uncheckpointed edits")
    with pytest.raises(ValueError, match="WAL"):
        inspect_auxiliary_storage(auxiliary)
    assert wal.read_bytes() == b"simulated uncheckpointed edits"
