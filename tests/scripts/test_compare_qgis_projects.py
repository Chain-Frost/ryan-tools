"""Maintained comparison wrapper exit codes and JSON fidelity."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from typing import Literal, Protocol, cast
from xml.etree import ElementTree as ET

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "other" / "compare_qgis_projects.py"


class ComparisonMain(Protocol):
    def __call__(
        self,
        *,
        working_directory: Path | None = None,
        original_project: Path | None = None,
        updated_project: Path | None = None,
        report_format: Literal["text", "json"] = "text",
    ) -> int: ...


def _main() -> ComparisonMain:
    namespace = runpy.run_path(str(SCRIPT))
    return cast(ComparisonMain, namespace["main"])


def _project_xml(name: str, *, provider: str = "ogr") -> bytes:
    root = ET.Element("qgis")
    layers = ET.SubElement(root, "projectlayers")
    layer = ET.SubElement(layers, "maplayer")
    for tag, value in (("id", "layer-1"), ("layername", name), ("provider", provider), ("datasource", "./roads.gpkg")):
        ET.SubElement(layer, tag).text = value
    return ET.tostring(root, encoding="utf-8")


def test_json_report_is_parseable_read_only_and_reports_actual_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    before = tmp_path / "original.qgs"
    after = tmp_path / "updated.qgs"
    before.write_bytes(_project_xml("Original"))
    after.write_bytes(_project_xml("Updated"))
    originals = (before.read_bytes(), after.read_bytes())
    status = _main()(
        working_directory=tmp_path,
        original_project=Path("original.qgs"),
        updated_project=Path("updated.qgs"),
        report_format="json",
    )
    output = json.loads(capsys.readouterr().out)
    assert status == 2
    assert output["layer_changes"][0]["changed_fields"] == ["name"]
    assert output["layer_changes"][0]["before"]["name"] == "Original"
    assert output["layer_changes"][0]["after"]["name"] == "Updated"
    assert (before.read_bytes(), after.read_bytes()) == originals
    assert set(tmp_path.iterdir()) == {before, after}


def test_identical_and_invalid_inputs_have_distinct_statuses(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    project = tmp_path / "existing.qgs"
    project.write_bytes(_project_xml("Same"))
    main = _main()
    assert main(working_directory=tmp_path, original_project=project, updated_project=project) == 0
    assert "No structural differences" in capsys.readouterr().out
    assert main(working_directory=tmp_path, original_project=project, updated_project=Path("missing.qgs")) == 1
    assert main(working_directory=tmp_path, original_project=None, updated_project=project) == 1
    assert not (tmp_path / "missing.qgs").exists()


def test_text_reports_provider_classification_without_guessing_uri(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    original = tmp_path / "original.qgs"
    updated = tmp_path / "updated.qgs"
    original.write_bytes(_project_xml("One", provider="ogr"))
    updated.write_bytes(_project_xml("One", provider="unrecognised_driver"))
    status = _main()(working_directory=tmp_path, original_project=original, updated_project=updated)
    assert status == 2
    report = capsys.readouterr().out
    assert "provider: 'ogr' -> 'unrecognised_driver'" in report
    assert "provider category: 'file-capable' -> 'unknown'" in report
