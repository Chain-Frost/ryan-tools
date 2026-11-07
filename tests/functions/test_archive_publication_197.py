"""Owned archive staging and publication regressions for issue #197."""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from ryan_library.functions import archive_utils


def _staged_archive(command: list[str]) -> Path:
    return next(
        Path(argument)
        for argument in command
        if ".staging-" in argument and argument.endswith(".7z")
    )


def _assert_no_private_artifacts(archive: Path) -> None:
    assert not list(archive.parent.glob(f".{archive.name}.staging-*"))
    assert not archive.with_name(f".{archive.name}.writing").exists()


def test_dotted_folder_names_remain_distinct(tmp_path: Path) -> None:
    first, second = tmp_path / "model.v1", tmp_path / "model.v2"
    first.mkdir()
    second.mkdir()
    assert archive_utils.archive_path_for_folder(first) == tmp_path / "model.v1.7z"
    assert archive_utils.archive_path_for_folder(second) == tmp_path / "model.v2.7z"
    assert archive_utils.archive_path_for_folder(first) != archive_utils.archive_path_for_folder(second)


def test_staged_archive_publishes_bytes_without_clobber(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "output.7z"

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        staged = _staged_archive(command)
        assert staged.parent != archive.parent
        staged.write_bytes(b"complete archive")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(archive_utils.subprocess, "run", fake_run)
    published = archive_utils.create_7zip_archive(executable=Path("7z"), output_archive=archive, inputs=[])
    assert published == (archive,)
    assert archive.read_bytes() == b"complete archive"
    _assert_no_private_artifacts(archive)


def test_existing_case_alias_or_volume_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "result[1].7z"
    existing = tmp_path / "RESULT[1].7Z.001"
    existing.write_bytes(b"untouched")

    def unexpected_run(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        pytest.fail("7-Zip must not run against existing archive output")

    monkeypatch.setattr(archive_utils.subprocess, "run", unexpected_run)
    with pytest.raises(FileExistsError, match="already exists"):
        archive_utils.create_7zip_archive(executable=Path("7z"), output_archive=archive, inputs=[])
    assert existing.read_bytes() == b"untouched"
    _assert_no_private_artifacts(archive)


def test_creator_after_preflight_cannot_be_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "target.7z"

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        _staged_archive(command).write_bytes(b"our archive")
        archive.write_bytes(b"concurrent creator")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(archive_utils.subprocess, "run", fake_run)
    with pytest.raises(FileExistsError, match="already exists"):
        archive_utils.create_7zip_archive(executable=Path("7z"), output_archive=archive, inputs=[])
    assert archive.read_bytes() == b"concurrent creator"
    _assert_no_private_artifacts(archive)


def test_failure_cleans_owned_staging_but_not_source_or_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"source")
    archive = tmp_path / "result.7z"
    unrelated = tmp_path / ".unrelated.staging-keep"
    unrelated.write_bytes(b"other owner")

    def failed_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        _staged_archive(command).write_bytes(b"partial")
        return subprocess.CompletedProcess(command, 2, stdout="", stderr="failed")

    monkeypatch.setattr(archive_utils.subprocess, "run", failed_run)
    with pytest.raises(RuntimeError, match="7z exit code 2"):
        archive_utils.create_7zip_archive(
            executable=Path("7z"),
            output_archive=archive,
            inputs=[source],
        )

    assert source.read_bytes() == b"source"
    assert unrelated.read_bytes() == b"other owner"
    assert not archive.exists()
    _assert_no_private_artifacts(archive)


def test_keyboard_interrupt_cleans_only_owned_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "interrupted.7z"
    sentinel = tmp_path / "unrelated.7z"
    sentinel.write_bytes(b"safe")

    def interrupted_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        _staged_archive(command).write_bytes(b"incomplete")
        raise KeyboardInterrupt

    monkeypatch.setattr(archive_utils.subprocess, "run", interrupted_run)
    with pytest.raises(KeyboardInterrupt):
        archive_utils.create_7zip_archive(executable=Path("7z"), output_archive=archive, inputs=[])

    assert sentinel.read_bytes() == b"safe"
    assert not archive.exists()
    _assert_no_private_artifacts(archive)


def test_concurrent_creators_use_exclusive_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "shared.7z"
    started, release = Event(), Event()

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        started.set()
        assert release.wait(10)
        _staged_archive(command).write_bytes(b"winner")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(archive_utils.subprocess, "run", fake_run)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(archive_utils.create_7zip_archive, executable=Path("7z"), output_archive=archive, inputs=[])
        assert started.wait(10)
        with pytest.raises(FileExistsError, match="publication already in progress"):
            archive_utils.create_7zip_archive(executable=Path("7z"), output_archive=archive, inputs=[])
        release.set()
        assert first.result(timeout=10) == (archive,)

    assert archive.read_bytes() == b"winner"
    _assert_no_private_artifacts(archive)


def test_split_volume_failure_rolls_back_only_owned_parts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "split.7z"
    first = tmp_path / "split.7z.001"
    second = tmp_path / "split.7z.002"

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        staged = _staged_archive(command)
        (staged.parent / f"{staged.name}.001").write_bytes(b"ours")
        (staged.parent / f"{staged.name}.002").write_bytes(b"ours")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    native_publish = archive_utils._publish_archive_part

    def competing_second(staged: Path, destination: Path) -> None:
        if destination.name.endswith(".002"):
            destination.write_bytes(b"foreign")
        native_publish(staged, destination)

    monkeypatch.setattr(archive_utils.subprocess, "run", fake_run)
    monkeypatch.setattr(archive_utils, "_publish_archive_part", competing_second)
    with pytest.raises(FileExistsError, match="already exists"):
        archive_utils.create_7zip_archive(
            executable=Path("7z"),
            output_archive=archive,
            inputs=[],
            max_part_size_gib=1,
        )

    assert not first.exists()
    assert second.read_bytes() == b"foreign"
    _assert_no_private_artifacts(archive)


def test_split_volume_sequence_must_start_at_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "split.7z"

    def missing_first(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        staged = _staged_archive(command)
        (staged.parent / f"{staged.name}.002").write_bytes(b"orphan")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(archive_utils.subprocess, "run", missing_first)
    with pytest.raises(RuntimeError, match="non-sequential"):
        archive_utils.create_7zip_archive(
            executable=Path("7z"),
            output_archive=archive,
            inputs=[],
            max_part_size_gib=1,
        )

    assert not list(tmp_path.glob("split.7z.*"))
    _assert_no_private_artifacts(archive)
