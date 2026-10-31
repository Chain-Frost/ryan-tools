"""Disposable QGIS project path/provider regressions for the read-only audit wrapper."""

from __future__ import annotations

import argparse
import json
import runpy
from collections.abc import Callable
from pathlib import Path
from types import FunctionType
from typing import Protocol, cast
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from ryan_library.functions.qgis_project import QgisAuditMode

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "other" / "audit_qgis_projects.py"


class AuditMain(Protocol):
    def __call__(
        self,
        *,
        working_directory: Path | None = None,
        mode: QgisAuditMode = "existence",
        project_path: Path | None = None,
        old_project_directory: Path | None = None,
        replace_root: tuple[Path, Path] | None = None,
        output_path: Path | None = None,
        write: bool = False,
    ) -> int: ...


def _functions() -> tuple[AuditMain, Callable[[Path], list[str]], Callable[[Path, str], Path | None]]:
    namespace = runpy.run_path(str(SCRIPT))
    return (
        cast(AuditMain, namespace["main"]),
        cast("Callable[[Path], list[str]]", namespace["get_data_sources"]),
        cast("Callable[[Path, str], Path | None]", namespace["resolve_file_source"]),
    )


def _project_xml(*sources: str) -> bytes:
    root = ET.Element("qgis")
    project_layers = ET.SubElement(root, "projectlayers")
    for source in sources:
        maplayer = ET.SubElement(project_layers, "maplayer")
        ET.SubElement(maplayer, "datasource").text = source
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _write_qgs(path: Path, *sources: str) -> None:
    path.write_bytes(_project_xml(*sources))


def _write_qgz(path: Path, *sources: str) -> None:
    with ZipFile(path, "w") as project:
        project.writestr(f"{path.stem}.qgs", _project_xml(*sources))


def test_qgz_relative_gpkg_and_qgs_absolute_sources_are_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gpkg = tmp_path / "relative data.gpkg"
    gpkg.write_bytes(b"source sentinel")
    qgz = tmp_path / "model.qgz"
    qgs = tmp_path / "drawing.qgs"
    _write_qgz(qgz, "./relative data.gpkg|layername=roads")
    _write_qgs(qgs, f"{gpkg}|layername=roads")
    before = {path: path.read_bytes() for path in (gpkg, qgz, qgs)}

    main, _, _ = _functions()
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=tmp_path) == 0
    assert {path: path.read_bytes() for path in before} == before


def test_missing_local_layer_returns_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_qgz(tmp_path / "model.qgz", "./missing.gpkg|layername=roads")
    main, _, _ = _functions()
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=tmp_path) == 1
    assert not (tmp_path / "missing.gpkg").exists()


def test_invalid_qgz_returns_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    invalid = tmp_path / "broken.qgz"
    invalid.write_bytes(b"not a QGIS archive")
    main, _, _ = _functions()
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=tmp_path) == 1
    assert invalid.read_bytes() == b"not a QGIS archive"


def test_file_uri_to_local_path_with_percent_escaping(tmp_path: Path) -> None:
    project = tmp_path / "model.qgz"
    gpkg = tmp_path / "roads & culverts.gpkg"
    gpkg.write_bytes(b"source")
    _, _, resolve = _functions()
    resolved = resolve(project, f"{gpkg.as_uri()}|layername=roads")
    assert resolved == gpkg.absolute()


def test_file_uri_localhost_preserves_local_path(tmp_path: Path) -> None:
    project = tmp_path / "model.qgz"
    gpkg = tmp_path / "roads & culverts.gpkg"
    _, _, resolve = _functions()
    uri = gpkg.as_uri().replace("file:///", "file://localhost/", 1)
    assert resolve(project, uri) == gpkg.absolute()


def test_file_uri_unc_preserves_server_and_share(tmp_path: Path) -> None:
    project = tmp_path / "model.qgz"
    _, _, resolve = _functions()
    result = resolve(project, "file://fileserver/engineering/roads%20data.gpkg|layername=roads")
    assert result is not None
    assert str(result).replace("\\", "/").lower().startswith("//fileserver/engineering/")
    assert result.name == "roads data.gpkg"


@pytest.mark.parametrize(
    "provider",
    [
        "memory?geometry=Point&crs=EPSG:28350",
        "type=xyz&url=https%3A%2F%2Fexample.com%2Ftiles",
        "service=WMS&url=https://example.com/wms",
        "service=WFS&url=https://example.com/wfs",
        "dbname='qgis' table=\"roads\"",
        "postgresql://server/database",
        "https://example.com/wms",
        "/vsicurl/https://example.com/roads.gpkg",
        "",
    ],
)
def test_remote_or_empty_provider_is_not_a_missing_file(tmp_path: Path, provider: str) -> None:
    _, _, resolve = _functions()
    assert resolve(tmp_path / "model.qgz", provider) is None


def test_qgz_source_list_is_deterministically_deduplicated(tmp_path: Path) -> None:
    project = tmp_path / "model.qgz"
    _write_qgz(project, "./b.gpkg|layername=2", "./a.gpkg|layername=1", "./b.gpkg|layername=2")
    _, get_sources, _ = _functions()
    assert get_sources(project) == ["./a.gpkg|layername=1", "./b.gpkg|layername=2"]


def test_cli_audit_modes_are_explicit_and_existence_is_default() -> None:
    namespace = runpy.run_path(str(SCRIPT))
    parse_args = cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"])
    assert namespace["DEFAULT_AUDIT_MODE"] == "existence"
    assert parse_args([]).mode is None  # The safe default is applied at the CLI process boundary.
    assert parse_args(["--mode", "existence", "--no-pause"]).mode == "existence"
    assert parse_args(["--mode", "cold-storage-ping", "--no-pause"]).mode == "cold-storage-ping"
    with pytest.raises(SystemExit) as invalid:
        parse_args(["--mode", "unknown"])
    assert invalid.value.code == 2


def test_cli_version_prints_embedded_wrapper_revision(capsys: pytest.CaptureFixture[str]) -> None:
    namespace = runpy.run_path(str(SCRIPT))
    parse_args = cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"])
    with pytest.raises(SystemExit) as result:
        parse_args(["--version"])
    assert result.value.code == 0
    assert namespace["WRAPPER_VERSION"] in capsys.readouterr().out


def test_cli_overrides_are_distinct_from_editable_defaults() -> None:
    namespace = runpy.run_path(str(SCRIPT))
    parse_args = cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"])
    defaults = parse_args([])
    assert defaults.project is None
    assert defaults.auxiliary_report is None
    assert defaults.sample_rows is None
    assert defaults.absolute_paths is None
    assert defaults.old_project_directory is None
    assert defaults.replace_root is None
    assert defaults.output is None
    assert defaults.write is None
    assert defaults.no_pause is None
    assert defaults.console_log_level is None
    assert namespace["DEFAULT_WRITE"] is False
    assert namespace["DEFAULT_NO_PAUSE"] is False
    assert namespace["DEFAULT_CONSOLE_LOG_LEVEL"] is None

    selected = parse_args(["--project", "model.qgz", "--auxiliary-report", "--sample-rows", "0", "--absolute-paths"])
    assert selected.project == Path("model.qgz")
    assert selected.auxiliary_report is True
    assert selected.sample_rows == 0
    assert selected.absolute_paths is True
    enabled = parse_args(["--write", "--no-pause", "--console-log-level", "DEBUG"])
    assert enabled.write is True
    assert enabled.no_pause is True
    assert enabled.console_log_level == "DEBUG"
    disabled = parse_args(["--no-write", "--pause", "--no-auxiliary-report", "--no-absolute-paths"])
    assert disabled.write is False
    assert disabled.no_pause is False
    assert disabled.auxiliary_report is False
    assert disabled.absolute_paths is False


def _load_copied_wrapper(tmp_path: Path, replacements: dict[str, str]) -> dict[str, object]:
    """Change only the user-editable constants of a copied script."""
    source = SCRIPT.read_text(encoding="utf-8")
    for original, replacement in replacements.items():
        assert source.count(original) == 1
        source = source.replace(original, replacement)
    copy = tmp_path / "editable_audit_qgis_projects.py"
    copy.write_text(source, encoding="utf-8")
    return runpy.run_path(str(copy))


def test_editing_default_audit_mode_enables_cold_ping_without_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "roads.gpkg"
    source.write_bytes(b"roads")
    _write_qgs(tmp_path / "model.qgs", "./roads.gpkg|layername=roads")
    namespace = _load_copied_wrapper(
        tmp_path,
        {
            'DEFAULT_AUDIT_MODE: Literal["existence", "cold-storage-ping"] = "existence"': 'DEFAULT_AUDIT_MODE: Literal["existence", "cold-storage-ping"] = "cold-storage-ping"'
        },
    )
    main = cast(FunctionType, namespace["main"])
    checked: list[tuple[Path, str]] = []

    def record_probe(path: Path, *, mode: str = "existence") -> tuple[bool, str]:
        checked.append((path, mode))
        return True, "checked"

    monkeypatch.setitem(main.__globals__, "probe_local_file", record_probe)
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=tmp_path) == 0
    assert checked == [(source, "cold-storage-ping")]

    checked.clear()
    assert main(working_directory=tmp_path, mode="existence") == 0
    assert checked == [(source, "existence")]


def test_editing_default_write_enables_new_copy_without_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "model.qgs"
    _write_qgs(project, "roads.gpkg|layername=roads")
    original = project.read_bytes()
    namespace = _load_copied_wrapper(
        tmp_path,
        {
            "DEFAULT_PROJECT: Path | None = None": 'DEFAULT_PROJECT: Path | None = Path("model.qgs")',
            "DEFAULT_OUTPUT: Path | None = None": 'DEFAULT_OUTPUT: Path | None = Path("fixed.qgs")',
            "DEFAULT_WRITE = False": "DEFAULT_WRITE = True",
        },
    )
    main = cast(FunctionType, namespace["main"])
    parse_args = cast(Callable[[list[str] | None], argparse.Namespace], namespace["_parse_cli_arguments"])
    monkeypatch.chdir(tmp_path)

    # An explicitly supplied --no-write must override an edited True default.
    args = parse_args(["--no-write"])
    assert args.write is False
    assert main(working_directory=tmp_path, write=args.write) == 0
    assert not (tmp_path / "fixed.qgs").exists()

    assert main(working_directory=tmp_path) == 0
    assert (tmp_path / "fixed.qgs").is_file()
    assert project.read_bytes() == original


def test_automated_qgis_catalogue_forces_safe_audit_options() -> None:
    registry = SCRIPT.parents[2] / "ryan_library" / "resources" / "mcp" / "workflows.json"
    data = json.loads(registry.read_text(encoding="utf-8"))
    workflow = next(w for w in data["workflows"] if w["id"] == "audit_qgis_projects")
    args = workflow["headless_arguments"]
    parse_args = cast(
        Callable[[list[str] | None], argparse.Namespace], runpy.run_path(str(SCRIPT))["_parse_cli_arguments"]
    )
    selected = parse_args(args)
    assert selected.mode == "existence"
    assert selected.write is False
    assert selected.auxiliary_report is False
    assert selected.sample_rows == 0
    assert selected.no_pause is True


def test_missing_working_directory_returns_failure_without_audit(tmp_path: Path) -> None:
    main, _, _ = _functions()
    assert main(working_directory=tmp_path / "missing_dir") == 1
    assert not (tmp_path / "missing_dir").exists()


def test_main_uses_selected_mode_and_deduplicates_sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = tmp_path / "shared.tif"
    data.write_bytes(b"source")
    _write_qgs(
        tmp_path / "project.qgs",
        "./shared.tif|layername=one",
        "./shared.tif|layername=two",
        "https://example.com/remote",
    )
    namespace = runpy.run_path(str(SCRIPT))
    main = cast(AuditMain, namespace["main"])
    audit = cast(FunctionType, namespace["_audit_projects"])
    checked: list[tuple[Path, QgisAuditMode]] = []

    def checked_probe(path: Path, *, mode: QgisAuditMode = "existence") -> tuple[bool, str]:
        checked.append((path, mode))
        return True, "checked"

    monkeypatch.setitem(audit.__globals__, "probe_local_file", checked_probe)
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=tmp_path) == 0
    assert checked == [(data, "existence")]
    checked.clear()
    assert main(working_directory=tmp_path, mode="cold-storage-ping") == 0
    assert checked == [(data, "cold-storage-ping")]


def test_cold_storage_ping_cannot_be_combined_with_path_repair(tmp_path: Path) -> None:
    main, _, _ = _functions()
    assert (
        main(
            working_directory=tmp_path,
            project_path=tmp_path / "project.qgs",
            old_project_directory=tmp_path,
            mode="cold-storage-ping",
        )
        == 1
    )


def test_cli_repair_to_different_directory_rebases_sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_dir = tmp_path / "model"
    data_dir = tmp_path / "data"
    target_dir = tmp_path / "export"
    for folder in (model_dir, data_dir, target_dir):
        folder.mkdir()

    target_data = data_dir / "roads.gpkg"
    target_data.write_bytes(b"roads")
    model = model_dir / "original.qgs"
    _write_qgs(model, "./old/roads.gpkg|layername=roads")
    original_bytes = model.read_bytes()
    output = target_dir / "repaired.qgs"

    main, get_sources, resolve = _functions()
    monkeypatch.chdir(tmp_path)
    assert (
        main(
            working_directory=tmp_path,
            project_path=model,
            replace_root=(model_dir / "old", data_dir),
            output_path=output,
            write=False,
        )
        == 0
    )
    assert not output.exists()

    assert (
        main(
            working_directory=tmp_path,
            project_path=model,
            replace_root=(model_dir / "old", data_dir),
            output_path=output,
            write=True,
        )
        == 0
    )
    assert model.read_bytes() == original_bytes
    assert get_sources(output) == ["../data/roads.gpkg|layername=roads"]
    result = resolve(output, get_sources(output)[0])
    assert result is not None
    assert result.samefile(target_data)
