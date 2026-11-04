"""Focused tests for the maintained standalone multi-folder ZIP script."""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
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
    DEFAULT_7ZIP_EXE: Path | None
    DEFAULT_EXCLUDED_FOLDERS: tuple[str, ...]
    DEFAULT_EXCLUDED_EXTENSIONS: tuple[str, ...]
    DEFAULT_DRY_RUN: bool
    DEFAULT_SHOW_PROGRESS: bool

    def _parse_cli_arguments(self, argv: Sequence[str] | None = None) -> argparse.Namespace: ...
    def main(
        self,
        *,
        input_paths: Sequence[Path],
        output_path: Path,
        seven_zip_exe: Path | None = None,
        force_auto_discovery: bool = False,
        excluded_folders: Sequence[str] | None = None,
        excluded_extensions: Sequence[str] | None = None,
        dry_run: bool | None = None,
        show_progress: bool | None = None,
    ) -> int: ...
    def create_zip_from_folders(
        self,
        *,
        folders: Sequence[Path],
        output_archive: Path,
        executable: Path,
        excluded_folders: Sequence[str] = (),
        excluded_extensions: Sequence[str] = (),
        show_progress: bool = True,
    ) -> list[Path]: ...


@pytest.fixture
def script() -> ArchiveModule:
    spec = importlib.util.spec_from_file_location("archive_multiple_tested", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return cast("ArchiveModule", module)


def test_editable_defaults_and_bidirectional_cli(script: ArchiveModule) -> None:
    assert script.DEFAULT_7ZIP_EXE is None
    assert script.DEFAULT_EXCLUDED_FOLDERS == ()
    assert script.DEFAULT_EXCLUDED_EXTENSIONS == ()
    assert script.DEFAULT_DRY_RUN is False
    assert script.DEFAULT_SHOW_PROGRESS is True

    arguments = script._parse_cli_arguments(  # pyright: ignore[reportPrivateUsage] - exercise CLI default resolution
        ["--exclude-folders", "--exclude-extensions", "--no-progress", "--no-dry-run"]
    )
    assert arguments.exclude_folders == []
    assert arguments.exclude_extensions == []
    assert arguments.progress is False
    assert arguments.dry_run is False


def test_dry_run_does_not_require_7zip(
    script: ArchiveModule,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "output.zip"
    monkeypatch.setattr(script, "find_7zip", lambda: None)

    code = script.main(
        input_paths=[source],
        output_path=output,
        force_auto_discovery=True,
        dry_run=True,
        show_progress=False,
    )

    assert code == 0
    assert not output.exists()


@pytest.mark.parametrize("suffix", ["", "[1]"])
def test_progress_switch_controls_7zip_progress_stream(
    suffix: str,
    script: ArchiveModule,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    executable = tmp_path / "7z.exe"
    executable.touch()
    observed_commands: list[list[str]] = []

    def fake_7zip(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        observed_commands.append(command)
        volume_switch_index = next(index for index, argument in enumerate(command) if argument.startswith("-v"))
        temporary_archive = Path(command[volume_switch_index + 1])
        temporary_archive.with_name(f"{temporary_archive.name}.001").write_bytes(b"complete")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_7zip)
    without_progress = script.create_zip_from_folders(
        folders=[source],
        output_archive=tmp_path / f"quiet{suffix}.zip",
        executable=executable,
        show_progress=False,
    )
    with_progress = script.create_zip_from_folders(
        folders=[source],
        output_archive=tmp_path / f"verbose{suffix}.zip",
        executable=executable,
        show_progress=True,
    )

    assert without_progress == [tmp_path / f"quiet{suffix}.zip"]
    assert with_progress == [tmp_path / f"verbose{suffix}.zip"]
    assert "-bsp1" not in observed_commands[0]
    assert "-bsp1" in observed_commands[1]
