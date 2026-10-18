"""Contract tests for shared filesystem verification primitives."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ryan_library.functions.file_verification import (
    FileSnapshot,
    has_linked_component,
    hash_file,
    snapshot_file,
    snapshot_is_unchanged,
)


def test_snapshots_retain_lexical_path_and_detect_file_changes(tmp_path: Path) -> None:
    path = tmp_path / "sample.bin"
    path.write_bytes(b"before")
    before = snapshot_file(path)

    assert before == FileSnapshot(path, 6, path.stat().st_mtime_ns)
    assert snapshot_is_unchanged(before)
    path.write_bytes(b"after-but-longer")
    assert not snapshot_is_unchanged(before)
    path.unlink()
    assert not snapshot_is_unchanged(before)


def test_sha256_streaming_preserves_digest_hex_and_byte_progress(tmp_path: Path) -> None:
    payload = b"abcdefghijk"
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)
    chunks: list[int] = []

    digest = hash_file(path, chunk_size=4, on_chunk=chunks.append)

    assert digest == hashlib.sha256(payload).digest()
    assert digest.hex() == hashlib.sha256(payload).hexdigest()
    assert chunks == [4, 4, 3]
    assert sum(chunks) == len(payload)


def test_zero_byte_hash_has_no_progress_updates(tmp_path: Path) -> None:
    path = tmp_path / "empty.bin"
    path.touch()
    counts: list[int] = []
    assert hash_file(path, on_chunk=counts.append) == hashlib.sha256(b"").digest()
    assert counts == []


def test_invalid_hash_chunk_size_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "empty.bin"
    path.touch()
    with pytest.raises(ValueError, match="chunk_size"):
        hash_file(path, chunk_size=0)


def test_symlink_components_are_detected_and_not_resolved_for_snapshots(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    target = real / "data.bin"
    target.write_bytes(b"data")
    alias = tmp_path / "linked"
    try:
        alias.symlink_to(real, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlink creation unavailable: {exc}")

    linked_file = alias / "data.bin"
    assert not linked_file.is_symlink()
    assert has_linked_component(linked_file)
    assert not has_linked_component(target)
    assert snapshot_file(linked_file).path == linked_file
    assert snapshot_file(linked_file).path == linked_file.absolute()
    assert snapshot_file(linked_file).path != target


def test_relative_snapshot_is_absolute_without_following_links(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = tmp_path / "real"
    real.mkdir()
    file = real / "source.bin"
    file.write_bytes(b"sample")
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(real, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlinks unavailable: {exc}")
    monkeypatch.chdir(tmp_path)
    relative = Path("alias") / "source.bin"

    record = snapshot_file(relative)

    assert record.path == relative.absolute()
    assert record.path != file
    assert has_linked_component(relative)
    assert snapshot_is_unchanged(record)
