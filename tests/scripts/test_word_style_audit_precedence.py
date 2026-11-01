"""Bounded contract tests for the standalone DOCX style-audit CLI."""

from __future__ import annotations

import json
import runpy
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import FunctionType
from typing import cast

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "docx-python" / "word_style_audit.py"
NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


def _create_docx(path: Path, style_id: str) -> None:
    styles = (
        f'<w:styles xmlns:w="{NS}"><w:style w:type="paragraph" w:styleId="{style_id}">'
        f'<w:name w:val="{style_id}"/></w:style></w:styles>'
    )
    document = (
        f'<w:document xmlns:w="{NS}"><w:body><w:p><w:pPr>'
        f'<w:pStyle w:val="{style_id}"/></w:pPr><w:r><w:t>Text</w:t></w:r>'
        "</w:p></w:body></w:document>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("word/styles.xml", styles)
        archive.writestr("word/document.xml", document)


def _audit_with_default(default: Path | None) -> Callable[[list[str]], int]:
    namespace = runpy.run_path(str(SCRIPT))
    function = cast(FunctionType, namespace["main"])
    function.__globals__["HARDCODED_DOCX_PATH"] = default
    return cast("Callable[[list[str]], int]", function)


def test_explicit_docx_overrides_editable_default(tmp_path: Path) -> None:
    editable = tmp_path / "default.docx"
    explicit = tmp_path / "café styles.docx"
    _create_docx(editable, "FromDefault")
    _create_docx(explicit, "FromArgument")
    original_default = editable.read_bytes()
    original_explicit = explicit.read_bytes()
    output = tmp_path / "reports with spaces"
    assert _audit_with_default(editable)([str(explicit), "--out-dir", str(output)]) == 0

    defined = json.loads((output / "styles_defined.json").read_text(encoding="utf-8"))
    applied = json.loads((output / "styles_applied_summary.json").read_text(encoding="utf-8"))
    assert defined["docx"] == str(explicit)
    assert [style["styleId"] for style in defined["styles"]] == ["FromArgument"]
    assert applied["applied"]["paragraph"] == [{"styleId": "FromArgument", "count": 1}]
    assert "FromArgument" in (output / "styles_applied.csv").read_text(encoding="utf-8")
    assert editable.read_bytes() == original_default
    assert explicit.read_bytes() == original_explicit


def test_no_argument_preserves_editable_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    editable = tmp_path / "default.docx"
    _create_docx(editable, "FromDefault")
    monkeypatch.chdir(tmp_path)
    assert _audit_with_default(editable)([]) == 0
    output = tmp_path / "style_audit_report"
    defined = json.loads((output / "styles_defined.json").read_text(encoding="utf-8"))
    assert defined["docx"] == str(editable)
    assert defined["styles"][0]["styleId"] == "FromDefault"


def test_missing_explicit_docx_does_not_fall_back(tmp_path: Path) -> None:
    editable = tmp_path / "default.docx"
    _create_docx(editable, "FromDefault")
    missing = tmp_path / "missing.docx"
    output = tmp_path / "reports"
    assert _audit_with_default(editable)([str(missing), "--out-dir", str(output)]) == 2
    assert not output.exists()


def test_bad_docx_returns_error_without_creating_reports(tmp_path: Path) -> None:
    bad_docx = tmp_path / "invalid.docx"
    bad_docx.write_bytes(b"not a zip file")
    output = tmp_path / "reports"
    assert _audit_with_default(None)([str(bad_docx), "--out-dir", str(output)]) == 2
    assert not output.exists()
