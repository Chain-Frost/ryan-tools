"""Bounded synthetic acceptance for archive extraction staging and output protection."""

from __future__ import annotations

import runpy
from collections.abc import Callable
from pathlib import Path
from types import FunctionType
from typing import cast

import pytest

ROOT = Path(__file__).absolute().parents[2]
SCRIPT = ROOT / "ryan-scripts" / "file-management-python" / "archive_extract.py"


def _worker() -> tuple[Callable[[Path], tuple[Path, bool, str]], dict[str, object]]:
    namespace = runpy.run_path(str(SCRIPT))
    function = cast(FunctionType, namespace["_extract_archive"])
    return cast("Callable[[Path], tuple[Path, bool, str]]", function), function.__globals__


def test_stale_staging_directory_is_never_deleted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "model.zip"
    archive.write_bytes(b"synthetic archive input")
    stale = tmp_path / ".model.extracting"
    stale.mkdir()
    sentinel = stale / "user-data.txt"
    sentinel.write_bytes(b"retain this original content")

    def extract(*, archive_path: Path, output_directory: Path, executable: Path | None) -> None:
        assert archive_path == archive
        _ = executable
        assert output_directory != stale
        assert output_directory.name.startswith(".model.extracting-")
        (output_directory / "payload.txt").write_text("extracted", encoding="utf-8")

    worker, namespace = _worker()
    monkeypatch.setitem(namespace, "extract_archive", extract)
    returned, ok, message = worker(archive)
    assert (returned, ok, message) == (archive, True, "")
    assert (tmp_path / "model" / "payload.txt").read_text(encoding="utf-8") == "extracted"
    assert sentinel.read_bytes() == b"retain this original content"
    assert list(tmp_path.glob(".model.extracting-*")) == []


def test_failed_extraction_cleans_only_its_unique_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "model.zip"
    original = b"archive bytes"
    archive.write_bytes(original)
    stale = tmp_path / ".model.extracting"
    stale.mkdir()
    sentinel = stale / "user-data.txt"
    sentinel.write_bytes(b"keep")

    def extract(*, archive_path: Path, output_directory: Path, executable: Path | None) -> None:
        assert archive_path == archive
        _ = executable
        (output_directory / "partial.txt").write_bytes(b"partial")
        msg = "synthetic extraction failure"
        raise RuntimeError(msg)

    worker, namespace = _worker()
    monkeypatch.setitem(namespace, "extract_archive", extract)
    returned, ok, message = worker(archive)
    assert returned == archive
    assert not ok
    assert "synthetic extraction failure" in message
    assert archive.read_bytes() == original
    assert sentinel.read_bytes() == b"keep"
    assert not (tmp_path / "model").exists()
    assert list(tmp_path.glob(".model.extracting-*")) == []


def test_existing_destination_never_invokes_extraction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "model.zip"
    archive.write_bytes(b"original")
    existing = tmp_path / "model"
    existing.mkdir()
    sentinel = existing / "original.txt"
    sentinel.write_bytes(b"untouched")

    def forbidden(*, archive_path: Path, output_directory: Path, executable: Path | None) -> None:
        _ = (archive_path, output_directory, executable)
        msg = "existing outputs must not be overwritten"
        raise AssertionError(msg)

    worker, namespace = _worker()
    monkeypatch.setitem(namespace, "extract_archive", forbidden)
    _, ok, message = worker(archive)
    assert ok
    assert "already exists" in message
    assert sentinel.read_bytes() == b"untouched"


def test_symlinked_archive_input_is_rejected(tmp_path: Path) -> None:
    original = tmp_path / "input.zip"
    original.write_bytes(b"original")
    link = tmp_path / "model.zip"
    try:
        link.symlink_to(original)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    worker, _ = _worker()
    returned, ok, message = worker(link)
    assert returned == link
    assert not ok
    assert "Symlinked" in message
    assert original.read_bytes() == b"original"
    assert not (tmp_path / "model").exists()
