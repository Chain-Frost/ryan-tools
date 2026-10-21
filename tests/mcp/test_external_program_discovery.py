"""External executable path discovery: no process launches or proprietary dependencies."""

from __future__ import annotations

from pathlib import Path

import pytest

from ryan_library.mcp.engineering import locate_external_program


@pytest.mark.parametrize("program", ["gdalinfo", "ogrinfo", "qgis_process", "TUFLOWHPC", "HY8.exe"])
def test_executable_location_is_not_readiness(program: str, tmp_path: Path) -> None:
    fake = tmp_path / "tool 測試 with spaces.exe"
    fake.write_bytes(b"synthetic file, not a runnable application")
    seen: list[str] = []

    def find_on_path(requested: str) -> str | None:
        seen.append(requested)
        return str(fake)

    probe = locate_external_program(program, find_on_path=find_on_path)

    assert seen == [program]
    assert probe.status == "path_found_unverified"
    assert probe.source == "path_lookup"
    assert probe.path == fake.absolute()
    assert "not verified" in probe.note
    assert fake.read_bytes() == b"synthetic file, not a runnable application"


def test_missing_configured_executable_does_not_fall_back_to_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    not_installed = tmp_path / "Missing HY-8.exe"

    def do_not_search(_program: str) -> str | None:
        pytest.fail("Explicit missing configuration must not search PATH")

    probe = locate_external_program("HY-8", configured_path=not_installed, find_on_path=do_not_search)
    assert probe.status == "not_found"
    assert probe.path == not_installed.absolute()
    assert probe.source == "configured_path"


def test_missing_path_lookup_reports_failure_without_execution() -> None:
    probe = locate_external_program("missing-gdalinfo", find_on_path=lambda _program: None)
    assert probe.status == "not_found"
    assert probe.path is None
    assert probe.source == "path_lookup"


def test_explicit_executable_path_is_preserved(tmp_path: Path) -> None:
    program = tmp_path / "QGIS 4 測試" / "qgis_process.exe"
    program.parent.mkdir(parents=True)
    program.write_bytes(b"synthetic")
    probe = locate_external_program("qgis_process", configured_path=program)
    assert probe.status == "path_found_unverified"
    assert probe.path == program.absolute()


def test_empty_program_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        locate_external_program(" ")
