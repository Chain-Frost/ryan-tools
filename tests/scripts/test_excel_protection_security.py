"""Disposable workbook security and publication regressions for issue #199."""

import importlib.util
import os
import shutil
import stat
import subprocess
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock
from xml.dom import minidom

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts/misc-python/remove_excel_protection.py"
NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


@pytest.fixture
def script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("office_security_199", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workbook(path: Path, *, extra: tuple[str | zipfile.ZipInfo, bytes] | None = None) -> bytes:
    with zipfile.ZipFile(path, "w") as archive:
        archive.comment = b"archive comment"
        archive.writestr(
            "xl/workbook.xml",
            f'<x:workbook xmlns:x="{NS}"><x:workbookProtection lockStructure="1"/><x:sheets/></x:workbook>',
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            f'<x:worksheet xmlns:x="{NS}" xmlns:foreign="urn:other"><!--keep--><x:sheetProtection sheet="1"/><foreign:sheetProtection keep="1"/><x:sheetData><x:row/></x:sheetData></x:worksheet>',
        )
        info = zipfile.ZipInfo("xl/vbaProject.bin", date_time=(2020, 1, 2, 3, 4, 6))
        info.comment = b"member comment"
        info.extra = b"\xfe\xca\x02\x00ok"
        info.external_attr = (stat.S_IFREG | 0o640) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, b"unchanged macro bytes\x00\xff")
        archive.writestr("xl/_rels/workbook.xml.rels", b"unchanged relationships")
        if extra is not None:
            archive.writestr(*extra)
    return path.read_bytes()


def test_preserves_source_members_metadata_and_foreign_elements(script: ModuleType, tmp_path: Path) -> None:
    source = tmp_path / "source.xlsm"
    original = workbook(source)
    output_dir = tmp_path / "out"
    assert script.process_excel_file(source, output_dir) is True
    assert source.read_bytes() == original
    with zipfile.ZipFile(source) as before, zipfile.ZipFile(output_dir / "source_unprotected.xlsm") as after:
        assert after.namelist() == before.namelist()
        assert after.comment == before.comment
        for name in ("xl/vbaProject.bin", "xl/_rels/workbook.xml.rels"):
            assert after.read(name) == before.read(name)
            for attr in ("date_time", "comment", "extra", "external_attr", "compress_type", "create_system"):
                assert getattr(after.getinfo(name), attr) == getattr(before.getinfo(name), attr)
        doc = minidom.parseString(after.read("xl/worksheets/sheet1.xml"))  # noqa: S318 - generated test fixture
        assert not doc.getElementsByTagNameNS(NS, "sheetProtection")
        assert len(doc.getElementsByTagNameNS("urn:other", "sheetProtection")) == 1
        assert len(doc.getElementsByTagNameNS(NS, "row")) == 1
        assert b"<!--keep-->" in after.read("xl/worksheets/sheet1.xml")
        doc.unlink()
    assert sorted(p.name for p in output_dir.iterdir()) == ["source_unprotected.xlsm"]


@pytest.mark.parametrize(
    "member",
    [
        "xl/worksheets/../../../victim.xml",
        "/absolute.xml",
        "C:/drive.xml",
        "xl\\worksheets\\sheet.xml",
        "xl/./sheet.xml",
        "XL/WORKBOOK.XML",
    ],
)
def test_rejects_unsafe_or_duplicate_members(script: ModuleType, tmp_path: Path, member: str) -> None:
    source = tmp_path / "source.xlsx"
    original = workbook(source, extra=(member, b"untrusted"))
    if "\\" in member:
        # ZipInfo normalizes separators when writing on Windows. Patch both
        # ZIP headers to reproduce an externally supplied raw backslash name.
        original = original.replace(member.replace("\\", "/").encode(), member.encode())
        source.write_bytes(original)
    sentinel = tmp_path / "victim.xml"
    sentinel.write_text("<root><sheetProtection/></root>")
    output_dir = tmp_path / "out"
    assert script.process_excel_file(source, output_dir) is False
    assert sentinel.read_text() == "<root><sheetProtection/></root>"
    assert source.read_bytes() == original
    assert not output_dir.exists()


def test_rejects_zip_symlink(script: ModuleType, tmp_path: Path) -> None:
    link = zipfile.ZipInfo("xl/worksheets/link.xml")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    source = tmp_path / "source.xlsx"
    workbook(source, extra=(link, b"../victim.xml"))
    assert script.process_excel_file(source, tmp_path / "out") is False


@pytest.mark.parametrize("xml", [b"<broken>", b'<!DOCTYPE workbook [<!ENTITY x "bad">]><workbook>&x;</workbook>'])
def test_invalid_xml_leaves_no_output_or_staging(script: ModuleType, tmp_path: Path, xml: bytes) -> None:
    source = tmp_path / "source.xlsx"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("xl/workbook.xml", xml)
    output = tmp_path / "out"
    assert script.process_excel_file(source, output) is False
    assert list(output.iterdir()) == []


def test_existing_output_and_unowned_temporary_file_survive(script: ModuleType, tmp_path: Path) -> None:
    source = tmp_path / "source.xlsx"
    workbook(source)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    output = output_dir / "source_unprotected.xlsx"
    legacy_temp = output.with_suffix(".xlsx.tmp")
    output.write_bytes(b"existing output")
    legacy_temp.write_bytes(b"another invocation")
    assert script.process_excel_file(source, output_dir) is False
    assert output.read_bytes() == b"existing output"
    assert legacy_temp.read_bytes() == b"another invocation"


def test_concurrent_destination_creator_wins(
    script: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.xlsx"
    workbook(source)
    output_dir = tmp_path / "out"
    real_link = os.link

    def concurrent_create(staged: Path, destination: Path) -> None:
        destination.write_bytes(b"concurrent creator")
        real_link(staged, destination)

    monkeypatch.setattr(script.os, "link", concurrent_create)
    assert script.process_excel_file(source, output_dir) is False
    assert (output_dir / "source_unprotected.xlsx").read_bytes() == b"concurrent creator"
    assert len(list(output_dir.iterdir())) == 1


def test_unsupported_publication_fails_closed(
    script: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.xlsx"
    workbook(source)
    monkeypatch.setattr(script.os, "link", Mock(side_effect=OSError("hard links unsupported")))
    output_dir = tmp_path / "out"
    assert script.process_excel_file(source, output_dir) is False
    assert list(output_dir.iterdir()) == []


def test_batch_collision_fails_before_creating_outputs(script: ModuleType, tmp_path: Path) -> None:
    roots = [tmp_path / "a", tmp_path / "b"]
    for root in roots:
        root.mkdir()
        workbook(root / "same.xlsx")
    output = tmp_path / "shared"
    assert script.main(input_directories=roots, output_directory=output, use_live_dashboard=False) == 1
    assert not output.exists()


def test_preview_is_nonmutating(script: ModuleType, tmp_path: Path) -> None:
    workbook(tmp_path / "source.xlsx")
    before = set(tmp_path.iterdir())
    assert script.main(input_directories=tmp_path, write=False) == 0
    assert set(tmp_path.iterdir()) == before


def test_partial_batch_reports_failure(script: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workbook(tmp_path / "good.xlsx")
    (tmp_path / "bad.xlsx").write_bytes(b"invalid ZIP")
    assert script.main(input_directories=tmp_path, use_live_dashboard=True) == 1
    output = capsys.readouterr().out
    assert "FAILED" in output
    assert "SUCCESS" not in output
    assert "\x1b[" not in output
    assert (tmp_path / "unprotected/good_unprotected.xlsx").is_file()


def test_copied_cli_preview_and_output_protection(tmp_path: Path) -> None:
    copied = tmp_path / SCRIPT.name
    shutil.copy2(SCRIPT, copied)
    workbook(tmp_path / "source.xlsx")
    command = [sys.executable, str(copied), "-i", str(tmp_path), "--no-pause", "--no-live-dashboard"]
    for args, expected in ((["--no-write"], 0), ([], 0), ([], 1)):
        result = subprocess.run(command + args, cwd=tmp_path, capture_output=True, text=True, check=False, timeout=30)
        assert result.returncode == expected, result.stdout + result.stderr
        if args:
            assert not (tmp_path / "unprotected").exists()


def test_interruption_cleans_owned_staging(script: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workbook(tmp_path / "source.xlsx")
    monkeypatch.setattr(script.os, "link", Mock(side_effect=KeyboardInterrupt))
    assert script.main(input_directories=tmp_path, use_live_dashboard=True) == 130
    assert list((tmp_path / "unprotected").iterdir()) == []


@pytest.mark.skipif(sys.platform != "win32", reason="Windows junction protection")
def test_rejects_output_junction(script: ModuleType, tmp_path: Path) -> None:
    source = tmp_path / "source.xlsx"
    workbook(source)
    actual = tmp_path / "actual"
    actual.mkdir()
    junction = tmp_path / "alias"
    cmd = shutil.which("cmd.exe")
    assert cmd is not None
    result = subprocess.run([cmd, "/c", "mklink", "/J", str(junction), str(actual)], capture_output=True, check=False)
    assert result.returncode == 0
    try:
        assert script.process_excel_file(source, junction) is False
        assert list(actual.iterdir()) == []
    finally:
        junction.rmdir()


def test_no_protection_keeps_xml_bytes(script: ModuleType, tmp_path: Path) -> None:
    source = tmp_path / "source.xlsx"
    payload = b'<?xml version="1.0"?><workbook><!--keep formatting--><sheets /></workbook>'
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("xl/workbook.xml", payload)
    assert script.process_excel_file(source, tmp_path / "out") is True
    with zipfile.ZipFile(tmp_path / "out/source_unprotected.xlsx") as archive:
        assert archive.read("xl/workbook.xml") == payload
