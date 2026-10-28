"""Safety and functional regressions for the standalone multi-folder ZIP script."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast

import pytest

SCRIPT = (
    Path(__file__).absolute().parents[2]
    / "ryan-scripts"
    / "file-management-python"
    / "archive_compress_multiple_folders.py"
)


class ArchiveModule(Protocol):
    def _publish_archive(self, temporary_archive: Path, output_archive: Path) -> None: ...
    def _validated_paths(self, input_paths: Sequence[Path], output_path: Path) -> tuple[list[Path], Path]: ...
    def create_zip_from_folders(
        self,
        *,
        folders: Sequence[Path],
        output_archive: Path,
        executable: Path,
        excluded_folders: Sequence[str] = (),
        excluded_extensions: Sequence[str] = (),
    ) -> None: ...
    def main(
        self,
        *,
        input_paths: Sequence[Path],
        output_path: Path,
        seven_zip_exe: Path | None = None,
        excluded_folders: Sequence[str] = (),
        excluded_extensions: Sequence[str] = (),
        dry_run: bool = False,
    ) -> int: ...


@pytest.fixture
def module() -> ArchiveModule:
    spec = importlib.util.spec_from_file_location("archive_multiple_tested", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    return cast("ArchiveModule", loaded)


def test_combines_distinct_parent_folders_with_exclusions(
    module: ArchiveModule, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    left, right = tmp_path / "left" / "Run_A", tmp_path / "right" / "Run_B"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "A.csv").write_text("alpha", encoding="utf-8")
    (right / "B.csv").write_text("beta", encoding="utf-8")
    (left / "noise.tmp").write_text("ignore", encoding="utf-8")
    (right / "skip").mkdir()
    (right / "skip" / "hidden.csv").write_text("ignore", encoding="utf-8")
    output = tmp_path / "complete.zip"
    executable = tmp_path / "7z.exe"
    executable.touch()
    observed: list[tuple[Path, list[str]]] = []

    def mock_7z(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command[0] == str(executable)
        assert command[1:5] == ["a", "-tzip", "-mx=5", "-mmt=on"]
        assert "-xr!skip" in command
        assert "-xr!*.tmp" in command
        base = cast("Path", kwargs["cwd"])
        observed.append((base, command))
        folder = base / command[6]
        archive = Path(command[5])
        with zipfile.ZipFile(archive, "a") as handle:
            for file in folder.rglob("*"):
                if file.is_file() and file.suffix != ".tmp" and "skip" not in file.parts:
                    handle.write(file, arcname=(Path(folder.name) / file.relative_to(folder)).as_posix())
        return subprocess.CompletedProcess(command, 0, stdout="Everything is Ok", stderr="")

    monkeypatch.setattr(subprocess, "run", mock_7z)
    module.create_zip_from_folders(
        folders=[left, right],
        output_archive=output,
        executable=executable,
        excluded_folders=("skip",),
        excluded_extensions=("tmp",),
    )
    assert [path for path, _ in observed] == [left.parent, right.parent]
    with zipfile.ZipFile(output) as handle:
        assert sorted(handle.namelist()) == ["Run_A/A.csv", "Run_B/B.csv"]
        assert handle.read("Run_A/A.csv") == b"alpha"
        assert handle.read("Run_B/B.csv") == b"beta"
    assert not list(tmp_path.glob(".*.partial.zip"))


def test_output_cannot_overwrite_an_existing_archive(module: ArchiveModule, tmp_path: Path) -> None:
    staged, output = tmp_path / "staged.zip", tmp_path / "original.zip"
    staged.write_bytes(b"complete staged file")
    output.write_bytes(b"existing archive")
    with pytest.raises((FileExistsError, OSError)):
        module._publish_archive(staged, output)  # pyright: ignore[reportPrivateUsage] - Regression exercises a guarded helper.
    assert output.read_bytes() == b"existing archive"
    assert staged.read_bytes() == b"complete staged file"


def test_unsupported_atomic_publishing_fails_without_creating_partial_output(
    module: ArchiveModule, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staged, output = tmp_path / "staged.zip", tmp_path / "target.zip"
    staged.write_bytes(b"complete staged file")

    def unavailable(*_args: object, **_kwargs: object) -> None:
        message = "simulated network filesystem unsupported operation"
        raise OSError(message)

    with monkeypatch.context() as patch:
        if os.name == "nt":
            patch.setattr(Path, "rename", unavailable)
        else:
            patch.setattr(os, "link", unavailable)
        with pytest.raises(OSError, match=r"simulated|atomically publish"):
            module._publish_archive(staged, output)  # pyright: ignore[reportPrivateUsage] - Regression exercises a guarded helper.
    assert not output.exists()
    assert staged.read_bytes() == b"complete staged file"


def test_no_archive_from_7zip_is_a_failure_not_a_success(
    module: ArchiveModule, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "Run_A"
    folder.mkdir()
    executable = tmp_path / "7z.exe"
    executable.touch()
    output = tmp_path / "missing.zip"

    def fake_success(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="OK", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_success)
    with pytest.raises(RuntimeError, match="non-empty archive"):
        module.create_zip_from_folders(folders=[folder], output_archive=output, executable=executable)
    assert not output.exists()
    assert not list(tmp_path.glob(".*.partial.zip"))


def test_dry_run_and_duplicate_archive_root_names(
    module: ArchiveModule, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = tmp_path / "north" / "same", tmp_path / "south" / "same"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    output = tmp_path / "selected.zip"
    executable = tmp_path / "7z.exe"
    executable.touch()
    with pytest.raises(ValueError, match="unique top-level"):
        module._validated_paths([first, second], output)  # pyright: ignore[reportPrivateUsage] - Regression exercises a guarded helper.

    def fake_info(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="7-Zip 25.00", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_info)
    assert module.main(input_paths=[first], output_path=output, seven_zip_exe=executable, dry_run=True) == 0
    assert not output.exists()


def test_rejects_broken_output_symlink(module: ArchiveModule, tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "broken.zip"
    try:
        output.symlink_to(tmp_path / "missing.zip")
    except (OSError, NotImplementedError):
        pytest.skip("Creating symbolic links is not permitted on this machine")
    with pytest.raises(FileExistsError, match="already exists"):
        module._validated_paths([source], output)  # pyright: ignore[reportPrivateUsage] - Regression exercises a guarded helper.
