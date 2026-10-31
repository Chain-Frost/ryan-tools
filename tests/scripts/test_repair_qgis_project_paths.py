"""Synthetic regressions for safe, new-copy-by-default QGIS path repairs."""

from __future__ import annotations

import argparse
import runpy
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from ryan_library.functions.qgis_project import local_source_path, project_layer_sources

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "other" / "repair_qgis_project_paths.py"


class RepairMain(Protocol):
    def __call__(
        self,
        *,
        working_directory: Path | None = None,
        project_path: Path | None = None,
        old_project_directory: Path | None = None,
        replace_root: tuple[Path, Path] | None = None,
        absolute_paths: bool = False,
        output_path: Path | None = None,
        write: bool = True,
    ) -> int: ...


def _functions() -> tuple[RepairMain, Callable[[list[str] | None], argparse.Namespace]]:
    namespace = runpy.run_path(str(SCRIPT))
    return (
        cast(RepairMain, namespace["main"]),
        cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"]),
    )


def _write_project(path: Path, uri: str) -> None:
    root = ET.Element("qgis")
    layers = ET.SubElement(root, "projectlayers")
    layer = ET.SubElement(layers, "maplayer")
    ET.SubElement(layer, "id").text = "roads-id"
    ET.SubElement(layer, "layername").text = "Roads"
    ET.SubElement(layer, "provider").text = "ogr"
    ET.SubElement(layer, "datasource").text = uri
    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    if path.suffix == ".qgz":
        with ZipFile(path, "w") as archive:
            archive.writestr(f"{path.stem}.qgs", xml)
            archive.writestr("custom/keep.txt", b"unchanged")
    else:
        path.write_bytes(xml)


def _load_copied_repair(tmp_path: Path, replacements: dict[str, str]) -> dict[str, object]:
    source = SCRIPT.read_text(encoding="utf-8")
    for old, new in replacements.items():
        assert source.count(old) == 1
        source = source.replace(old, new)
    copied = tmp_path / "copied_repair_qgis.py"
    copied.write_text(source, encoding="utf-8")
    return runpy.run_path(str(copied))


def test_repair_cli_exposes_only_repair_options() -> None:
    _, parse_args = _functions()
    args = parse_args([])
    assert args.project is None
    assert args.old_project_directory is None
    assert args.replace_root is None
    assert args.absolute_paths is None
    assert args.output is None
    assert args.write is None
    assert args.console_log_level is None
    assert args.no_pause is None
    explicit = parse_args(
        ["--project", "model.qgs", "--replace-root", "old", "new", "--output", "fixed.qgs", "--write"]
    )
    assert explicit.project == Path("model.qgs")
    assert explicit.replace_root == [Path("old"), Path("new")]
    assert explicit.output == Path("fixed.qgs")
    assert explicit.write is True
    disabled = parse_args(["--no-write", "--no-absolute-paths", "--pause"])
    assert disabled.write is False
    assert disabled.absolute_paths is False
    assert disabled.no_pause is False
    for option in ("--mode", "--auxiliary-report", "--sample-rows"):
        with pytest.raises(SystemExit) as invalid:
            parse_args([option])
        assert invalid.value.code == 2


def test_repair_cli_version_and_help(capsys: pytest.CaptureFixture[str]) -> None:
    _, parse_args = _functions()
    for option in ("--help", "--version"):
        with pytest.raises(SystemExit) as result:
            parse_args([option])
        assert result.value.code == 0
    text = capsys.readouterr().out
    assert "usage:" in text
    assert "--replace-root" in text
    assert "--write" in text
    assert "--no-write" in text
    assert "INPUT_repaired" in text
    assert "2026-10-31.2" in text


def test_repair_rejects_missing_project(tmp_path: Path) -> None:
    main, _ = _functions()
    assert main(working_directory=tmp_path) == 1
    assert main(working_directory=tmp_path, project_path=Path("model.qgs")) == 1
    assert main(working_directory=tmp_path / "missing", project_path=Path("model.qgs")) == 1
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize("suffix", [".qgs", ".qgz"])
def test_preview_then_copy_to_new_directory_preserves_data(
    tmp_path: Path, suffix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    old_dir = tmp_path / "project"
    new_dir = tmp_path / "output"
    data_dir = tmp_path / "new_data"
    for folder in (old_dir, new_dir, data_dir):
        folder.mkdir()
    data = data_dir / "roads.gpkg"
    data.write_bytes(b"source-data-not-to-be-opened-or-modified")
    project = old_dir / f"model{suffix}"
    output = new_dir / f"fixed{suffix}"
    _write_project(project, "./old/roads.gpkg|layername=roads")
    before = project.read_bytes()
    main, _ = _functions()

    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(old_dir / "old", data_dir),
            output_path=output,
            write=False,
        )
        == 0
    )
    assert not output.exists()
    assert project.read_bytes() == before

    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(old_dir / "old", data_dir),
            output_path=output,
            write=True,
        )
        == 0
    )
    assert project.read_bytes() == before
    assert data.read_bytes() == b"source-data-not-to-be-opened-or-modified"
    layer = project_layer_sources(output)[0]
    assert layer.uri == "../new_data/roads.gpkg|layername=roads"
    source = local_source_path(output, layer.uri, provider=layer.provider)
    assert source is not None
    assert source.samefile(data)
    if suffix == ".qgz":
        with ZipFile(output) as archive:
            assert archive.read("custom/keep.txt") == b"unchanged"


def test_preview_does_not_overwrite_existing_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "model.qgs"
    output = tmp_path / "fixed.qgs"
    _write_project(project, "roads.gpkg|layername=roads")
    original = project.read_bytes()
    output.write_bytes(b"existing-output-must-survive")
    main, _ = _functions()
    assert main(working_directory=tmp_path, project_path=project, output_path=output, write=False) == 0
    assert main(working_directory=tmp_path, project_path=project, output_path=output, write=True) == 1
    assert project.read_bytes() == original
    assert output.read_bytes() == b"existing-output-must-survive"


def test_copied_repair_wrapper_defaults_and_no_write_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "model.qgs"
    output = tmp_path / "fixed.qgs"
    _write_project(project, "roads.gpkg|layername=roads")
    original = project.read_bytes()
    namespace = _load_copied_repair(
        tmp_path,
        {
            "DEFAULT_PROJECT: Path | None = None": 'DEFAULT_PROJECT: Path | None = Path("model.qgs")',
            "DEFAULT_OUTPUT: Path | None = None": 'DEFAULT_OUTPUT: Path | None = Path("fixed.qgs")',
        },
    )
    assert namespace["DEFAULT_WRITE"] is True
    main = cast(RepairMain, namespace["main"])
    parse_args = cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"])
    monkeypatch.chdir(tmp_path)

    assert parse_args(["--no-write"]).write is False
    assert main(working_directory=tmp_path, write=False) == 0
    assert not output.exists()
    assert main(working_directory=tmp_path) == 0
    assert output.exists()
    assert project.read_bytes() == original

@pytest.mark.parametrize("suffix", [".qgs", ".qgz"])
def test_default_repair_writes_new_named_copy_and_refuses_repeat(
    tmp_path: Path, suffix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    project = tmp_path / f"model{suffix}"
    _write_project(project, "./old/roads.gpkg|layername=roads")
    source_bytes = project.read_bytes()
    output = tmp_path / f"model_repaired{suffix}"
    main, _ = _functions()
    assert not output.exists()

    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(tmp_path / "old", tmp_path / "new"),
        )
        == 0
    )
    assert output.is_file()
    first_output_bytes = output.read_bytes()
    assert project.read_bytes() == source_bytes
    assert project_layer_sources(output)[0].uri == "./new/roads.gpkg|layername=roads"

    # Repeated runs never overwrite the already-created copy.
    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(tmp_path / "old", tmp_path / "new"),
        )
        == 1
    )
    assert output.read_bytes() == first_output_bytes
    assert project.read_bytes() == source_bytes


def test_default_no_output_option_rebases_in_input_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    source_dir = tmp_path / "models"
    source_dir.mkdir()
    project = source_dir / "model.qgs"
    _write_project(project, "./old/roads.gpkg|layername=roads")
    main, _ = _functions()

    assert (
        main(
            working_directory=tmp_path,
            project_path=Path("models/model.qgs"),
            replace_root=(source_dir / "old", source_dir / "new"),
        )
        == 0
    )
    output = source_dir / "model_repaired.qgs"
    assert output.exists()
    assert not (tmp_path / "model_repaired.qgs").exists()


def test_default_write_can_be_overridden_to_preview_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "model.qgs"
    _write_project(project, "./old/roads.gpkg|layername=roads")
    original = project.read_bytes()
    main, parse_args = _functions()
    assert parse_args([]).write is None
    assert parse_args(["--no-write"]).write is False

    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(tmp_path / "old", tmp_path / "new"),
            write=False,
        )
        == 0
    )
    assert not (tmp_path / "model_repaired.qgs").exists()
    assert project.read_bytes() == original


def test_default_copy_keeps_qgs_qgd_sidecar_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "model.qgs"
    _write_project(project, "./old/roads.gpkg")
    sidecar = tmp_path / "model.qgd"
    sidecar.write_bytes(b"sample SQLite companion bytes")
    original_bytes = project.read_bytes()
    main, _ = _functions()

    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(tmp_path / "old", tmp_path / "new"),
        )
        == 0
    )
    assert (tmp_path / "model_repaired.qgd").read_bytes() == sidecar.read_bytes()
    assert project.read_bytes() == original_bytes
    assert sidecar.read_bytes() == b"sample SQLite companion bytes"


def test_default_output_qgd_collision_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "model.qgs"
    _write_project(project, "./old/roads.gpkg")
    original = project.read_bytes()
    project.with_suffix(".qgd").write_bytes(b"existing auxiliary")
    reserved = tmp_path / "model_repaired.qgd"
    reserved.write_bytes(b"do not replace")
    main, _ = _functions()
    assert (
        main(
            working_directory=tmp_path,
            project_path=project,
            replace_root=(tmp_path / "old", tmp_path / "new"),
        )
        == 1
    )
    assert reserved.read_bytes() == b"do not replace"
    assert not (tmp_path / "model_repaired.qgs").exists()
    assert project.read_bytes() == original
