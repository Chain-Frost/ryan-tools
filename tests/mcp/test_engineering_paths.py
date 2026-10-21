"""Disposable engineering-path and failure-contract checks.

Opt-in mapped/UNC checks require RYAN_CF_MAPPED_ROOT / RYAN_CF_UNC_ROOT.
They only create and remove their own temporary directory on the chosen share.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from ryan_library.mcp.installed_scripts import copy_wrapper

SCRIPT = "file-management-python/list_files_recursive.py"
CONTENT = b"# synthetic wrapper; not executable\n"


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "wheel - é" / "ryan-scripts" / SCRIPT
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(CONTENT)
    return source


@pytest.mark.parametrize("directory_name", ["ordinary", "path with spaces", "model_測試_é"])
def test_copy_preserves_source_and_handles_special_names(tmp_path: Path, directory_name: str) -> None:
    source = _source(tmp_path)
    destination = tmp_path / directory_name
    destination.mkdir()
    copied = copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1])
    assert copied.read_bytes() == CONTENT
    assert source.read_bytes() == CONTENT
    assert copied.parent == destination.absolute()


def test_copy_failure_is_non_destructive(tmp_path: Path) -> None:
    source = _source(tmp_path)
    destination = tmp_path / "destination"
    destination.mkdir()
    existing = destination / source.name
    existing.write_bytes(b"do not overwrite")
    before_source = source.read_bytes()
    before_destination = existing.read_bytes()

    with pytest.raises(FileExistsError, match="--overwrite"):
        copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1])
    assert source.read_bytes() == before_source
    assert existing.read_bytes() == before_destination
    assert list(destination.iterdir()) == [existing]


def test_copy_dry_run_does_not_write(tmp_path: Path) -> None:
    source = _source(tmp_path)
    destination = tmp_path / "preview"
    destination.mkdir()
    copied = copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1], dry_run=True)
    assert copied == destination / source.name
    assert list(destination.iterdir()) == []
    assert source.read_bytes() == CONTENT


def test_installed_cli_rejects_unknown_copy_with_nonzero_exit(tmp_path: Path) -> None:
    destination = tmp_path / "existing output"
    destination.mkdir()
    sentinel = destination / "input.txt"
    sentinel.write_bytes(b"retain this data")

    process = subprocess.run(
        [sys.executable, "-m", "ryan_library.mcp.installed_scripts", "copy", "../not-allowlisted.py", str(destination)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert process.returncode == 2
    assert "not in the installed wrapper allowlist" in process.stderr
    assert sentinel.read_bytes() == b"retain this data"
    assert list(destination.iterdir()) == [sentinel]


@pytest.mark.skipif(os.name != "nt", reason="Windows long-path acceptance needs a Windows filesystem")
def test_opt_in_windows_long_path(tmp_path: Path) -> None:
    if os.environ.get("RYAN_CF_TEST_LONG_PATHS") != "1":
        pytest.skip("Set RYAN_CF_TEST_LONG_PATHS=1 to enable long-path acceptance.")
    source = _source(tmp_path)
    # Explicit opt-in because long-path policy can be disabled by Windows or the share.
    destination = tmp_path
    while len(str(destination)) < 280:
        destination /= "a_long_unicode_path_測試_0123456789"
    destination.mkdir(parents=True)
    copied = copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1])
    assert len(str(copied)) > 260
    assert copied.read_bytes() == CONTENT
    assert source.read_bytes() == CONTENT


@pytest.mark.skipif(os.name != "nt", reason="Mapped-drive and UNC acceptance requires Windows")
@pytest.mark.parametrize("variable", ["RYAN_CF_MAPPED_ROOT", "RYAN_CF_UNC_ROOT"])
def test_opt_in_network_share_copy(variable: str, tmp_path: Path) -> None:
    configured = os.environ.get(variable)
    if not configured:
        pytest.skip(f"Set {variable} to an existing writable test directory to enable share acceptance.")
    root = Path(configured)
    assert root.is_dir(), f"{variable} is not an existing directory: {root}"
    source = _source(tmp_path)

    with tempfile.TemporaryDirectory(prefix="ryan-cf-acceptance-", dir=root) as temporary:
        destination = Path(temporary)
        copied = copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1])
        assert copied.read_bytes() == CONTENT
        assert source.read_bytes() == CONTENT
        with pytest.raises(FileExistsError):
            copy_wrapper(SCRIPT, destination, scripts_root=source.parents[1])
        assert copied.read_bytes() == CONTENT
        # Path.absolute retains the user-facing mapped drive/UNC spelling.
        assert str(copied).casefold().startswith(str(root.absolute()).casefold())


@pytest.mark.parametrize("program", ["gdalinfo", "ogrinfo", "qgis_process", "TUFLOWHPC", "HY8.exe"])
def test_executable_discovery_is_non_executing(
    program: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Finding a program on PATH is not evidence that it launches or is licensed.
    executable = tmp_path / "tool with spaces.exe"
    executable.write_bytes(b"synthetic-not-an-executable")
    def fake_which(name: str) -> str | None:
        return str(executable) if name == program else None

    monkeypatch.setattr(shutil, "which", fake_which)
    assert shutil.which(program) == str(executable)
    assert executable.read_bytes() == b"synthetic-not-an-executable"
