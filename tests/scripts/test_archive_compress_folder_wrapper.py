"""Focused archive-folder wrapper and shared 7-Zip helper tests."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest

from ryan_library.functions import archive_utils

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "file-management-python" / "archive_compress_folder.py"


@pytest.fixture
def wrapper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("archive_compress_folder_tested", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class InlinePool:
    """Keep multiprocessing's scheduling contract without starting external processes."""

    def __init__(self, processes: int) -> None:
        assert processes >= 1

    def __enter__(self) -> InlinePool:
        return self

    def __exit__(self, *_arguments: object) -> None:
        return None

    def imap_unordered(
        self, action: Callable[[archive_utils.FolderCompressionJob], archive_utils.FolderCompressionResult], jobs: list[archive_utils.FolderCompressionJob]
    ) -> list[archive_utils.FolderCompressionResult]:
        assert callable(action)
        return [action(job) for job in jobs]


def test_archive_helper_preserves_output_and_native_exclusions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "Folder.data"
    source.mkdir()
    executable = tmp_path / "7z.exe"
    executable.touch()
    observed: dict[str, object] = {}

    def fake_7zip(*, executable: Path, output_archive: Path, inputs: list[str], exclusions: list[str]) -> None:
        observed.update(exe=executable, output=output_archive, inputs=inputs, exclusions=exclusions)
        output_archive.write_text("compressed", encoding="utf-8")

    monkeypatch.setattr(archive_utils, "create_7zip_archive", fake_7zip)
    job = archive_utils.FolderCompressionJob(source, executable, ("xf",), (".dat", "xmdf"))
    result = archive_utils.compress_folder_7zip(job)
    assert result.status == "SUCCESS"
    assert observed == {
        "exe": executable,
        "output": source.with_suffix(".7z"),
        "inputs": [f"{source}\\*"],
        "exclusions": ["-xr!xf", "-xr!*.dat", "-xr!*.xmdf"],
    }
    second = archive_utils.compress_folder_7zip(job)
    assert second.status == "SKIPPED"


def test_wrapper_reports_each_folder_without_tty_progress(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    exe = tmp_path / "7z.exe"
    exe.touch()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(wrapper.multiprocessing, "Pool", InlinePool)

    def fake_7zip(*, executable: Path, output_archive: Path, inputs: list[str], exclusions: list[str]) -> None:
        assert executable.exists()
        assert inputs
        assert exclusions
        if output_archive.stem == "second":
            message = "Synthetic archive failure"
            raise RuntimeError(message)
        output_archive.write_text("compressed", encoding="utf-8")

    monkeypatch.setattr(archive_utils, "create_7zip_archive", fake_7zip)
    code = wrapper.main(
        input_paths=[first, second], working_directory=tmp_path, seven_zip_exe=exe, max_workers=2,
        show_progress=False,
    )
    output = capsys.readouterr().out
    assert code == 1
    assert "Folders: 2 | workers: 2" in output
    assert "[1/2] SUCCESS:" in output
    assert "[2/2] ERROR:" in output
    assert "Synthetic archive failure" in output
    assert "Archive: " in output


def test_wrapper_defaults_and_invalid_worker_count(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert wrapper.DEFAULT_MAX_WORKERS is None
    assert wrapper.DEFAULT_SHOW_PROGRESS is True
    assert wrapper.DEFAULT_CONSOLE_LOG_LEVEL == "INFO"
    assert wrapper._parse_cli_arguments(["--no-progress", "--workers", "3"]).progress is False
    assert wrapper.main(input_paths=[tmp_path], working_directory=tmp_path, max_workers=0) == 2
