"""Synthetic preflight checks for archive member traversal and output collisions."""

from __future__ import annotations

import io
import runpy
import stat
import tarfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import FunctionType
from typing import cast

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "file-management-python" / "archive_extract.py"


def _function(name: str) -> tuple[FunctionType, dict[str, object]]:
    namespace = runpy.run_path(str(SCRIPT))
    function = cast(FunctionType, namespace[name])
    return function, function.__globals__


@pytest.mark.parametrize(
    "name",
    [
        "../outside.txt",
        "dir/../../outside.txt",
        "/absolute.txt",
        "C:/Windows/target.txt",
        r"dir\..\outside.txt",
        "//server/share/file.txt",
        "file.txt:alternate-stream",
    ],
)
def test_zip_traversal_is_rejected_before_extraction(tmp_path: Path, name: str) -> None:
    archive = tmp_path / "model.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(name, "untrusted data")

    worker, _ = _function("_extract_archive")
    _, success, message = cast("Callable[[Path], tuple[Path, bool, str]]", worker)(archive)
    assert not success
    assert "Unsafe archive member path" in message
    assert not (tmp_path / "model").exists()
    assert not (tmp_path / "outside.txt").exists()
    assert not list(tmp_path.glob(".model.extracting-*"))


def test_zip_symlink_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "model.zip"
    link = zipfile.ZipInfo("symlink")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(link, "../outside.txt")

    worker, _ = _function("_extract_archive")
    _, success, message = cast("Callable[[Path], tuple[Path, bool, str]]", worker)(archive)
    assert not success
    assert "symbolic link" in message.lower()
    assert not (tmp_path / "model").exists()


def test_tar_symlink_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "model.tar"
    with tarfile.open(archive, "w") as output:
        entry = tarfile.TarInfo("symlink")
        entry.type = tarfile.SYMTYPE
        entry.linkname = "../outside.txt"
        output.addfile(entry)

    worker, _ = _function("_extract_archive")
    _, success, message = cast("Callable[[Path], tuple[Path, bool, str]]", worker)(archive)
    assert not success
    assert "unsupported link" in message
    assert not (tmp_path / "model").exists()


def test_tar_parent_traversal_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "model.tar"
    payload = b"unsafe"
    with tarfile.open(archive, "w") as output:
        entry = tarfile.TarInfo("../outside.txt")
        entry.size = len(payload)
        output.addfile(entry, io.BytesIO(payload))
    worker, _ = _function("_extract_archive")
    _, success, message = cast("Callable[[Path], tuple[Path, bool, str]]", worker)(archive)
    assert not success
    assert "Unsafe archive member path" in message
    assert not (tmp_path / "outside.txt").exists()


def test_valid_zip_extracts_with_python_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "model.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("subfolder/safe.txt", b"safe")
    worker, namespace = _function("_extract_archive")
    monkeypatch.setitem(namespace, "SEVEN_ZIP_EXE", None)
    _, success, message = cast("Callable[[Path], tuple[Path, bool, str]]", worker)(archive)
    assert success, message
    assert (tmp_path / "model" / "subfolder" / "safe.txt").read_bytes() == b"safe"


def test_same_stem_archives_refused_before_processing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "model.zip").write_bytes(b"not opened")
    (tmp_path / "model.tar").write_bytes(b"not opened")
    main, _ = _function("main")
    monkeypatch.chdir(tmp_path)
    result = cast("Callable[..., int]", main)(input_paths=tmp_path)
    assert result == 1
    assert not (tmp_path / "model").exists()
