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
        self,
        action: Callable[[archive_utils.FolderCompressionJob], archive_utils.FolderCompressionResult],
        jobs: list[archive_utils.FolderCompressionJob],
    ) -> list[archive_utils.FolderCompressionResult]:
        assert callable(action)
        return [action(job) for job in jobs]


def test_archive_helper_preserves_output_and_native_exclusions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "Folder.data"
    source.mkdir()
    executable = tmp_path / "7z.exe"
    executable.touch()
    observed: dict[str, object] = {}

    def fake_7zip(
        *,
        executable: Path,
        output_archive: Path,
        inputs: list[str],
        exclusions: list[str],
        max_part_size_gib: int | None,
    ) -> tuple[Path, ...]:
        observed.update(
            exe=executable,
            output=output_archive,
            inputs=inputs,
            exclusions=exclusions,
            max_part_size_gib=max_part_size_gib,
        )
        output_archive.write_text("compressed", encoding="utf-8")
        return (output_archive,)

    monkeypatch.setattr(archive_utils, "create_7zip_archive", fake_7zip)
    job = archive_utils.FolderCompressionJob(source, executable, ("xf",), (".dat", "xmdf"))
    result = archive_utils.compress_folder_7zip(job)
    assert result.status == "SUCCESS"
    assert observed == {
        "exe": executable,
        "output": source.with_name(f"{source.name}.7z"),
        "inputs": [f"{source}\\*"],
        "exclusions": ["-xr!xf", "-xr!*.dat", "-xr!*.xmdf"],
        "max_part_size_gib": None,
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

    def fake_7zip(
        *,
        executable: Path,
        output_archive: Path,
        inputs: list[str],
        exclusions: list[str],
        max_part_size_gib: int | None,
    ) -> tuple[Path, ...]:
        assert executable.exists()
        assert inputs
        assert exclusions
        assert max_part_size_gib is None
        if output_archive.stem == "second":
            message = "Synthetic archive failure"
            raise RuntimeError(message)
        output_archive.write_text("compressed", encoding="utf-8")
        return (output_archive,)

    monkeypatch.setattr(archive_utils, "create_7zip_archive", fake_7zip)
    code = wrapper.main(
        input_paths=[first, second],
        working_directory=tmp_path,
        seven_zip_exe=exe,
        max_workers=2,
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
    assert wrapper.DEFAULT_MAX_PART_SIZE_GIB is None
    assert wrapper.DEFAULT_DRY_RUN is False
    assert wrapper.DEFAULT_PAUSE_ON_FINISH is True
    assert wrapper.DEFAULT_SHOW_PROGRESS is True
    assert wrapper.DEFAULT_CONSOLE_LOG_LEVEL == "INFO"
    assert wrapper._parse_cli_arguments(["--no-progress", "--workers", "3"]).progress is False
    assert wrapper._parse_cli_arguments(["--max-part-size-gib", "5"]).max_part_size_gib == 5
    assert wrapper._parse_cli_arguments(["--no-pause"]).pause is False
    assert wrapper.main(input_paths=[tmp_path], working_directory=tmp_path, max_workers=0) == 1


def test_dry_run_does_not_require_7zip_or_create_an_archive(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(wrapper, "find_7zip", lambda: None)

    code = wrapper.main(
        input_paths=[source],
        working_directory=tmp_path,
        force_auto_discovery=True,
        dry_run=True,
        show_progress=False,
    )

    assert code == 0
    assert not source.with_suffix(".7z").exists()
    assert "Would create:" in capsys.readouterr().out


def test_dotted_folder_siblings_have_distinct_archive_plan(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    first, second = tmp_path / "model.v1", tmp_path / "model.v2"
    first.mkdir()
    second.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(wrapper, "find_7zip", lambda: None)

    result = wrapper.main(
        input_paths=[first, second],
        working_directory=tmp_path,
        force_auto_discovery=True,
        dry_run=True,
        show_progress=False,
    )
    output = capsys.readouterr().out
    assert result == 0
    assert "model.v1.7z" in output
    assert "model.v2.7z" in output
    assert not (tmp_path / "model.7z").exists()


def test_wrapper_rejects_physical_output_aliases_before_dispatch(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "models" / "A"
    folder.mkdir(parents=True)
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(folder.parent, target_is_directory=True)
    except OSError:
        pytest.skip("This runtime cannot create directory aliases.")
    executable = tmp_path / "7z.exe"
    executable.touch()
    monkeypatch.chdir(tmp_path)

    def unexpected_pool(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Archive workers must not start for duplicated destinations")

    monkeypatch.setattr(wrapper.multiprocessing, "Pool", unexpected_pool)
    assert (
        wrapper.main(
            input_paths=[folder, alias / folder.name],
            working_directory=tmp_path,
            seven_zip_exe=executable,
            max_workers=2,
            show_progress=False,
        )
        == 1
    )


def test_staged_output_size_includes_split_temporary_files(wrapper: ModuleType, tmp_path: Path) -> None:
    archive = tmp_path / "result.7z"
    staging = tmp_path / ".result.7z.staging-test"
    staging.mkdir()
    (staging / "result.7z.001").write_bytes(b"123")
    (staging / "result.7z.002.tmp").write_bytes(b"12345")
    (staging / "unrelated").write_bytes(b"ignored")
    assert wrapper._staged_output_size(archive, 0) == 8


def test_active_log_is_excluded_for_its_source_folder(
    wrapper: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    exe = tmp_path / "7z.exe"
    exe.touch()
    monkeypatch.setattr(wrapper.multiprocessing, "Pool", InlinePool)
    observed: list[str] = []

    def fake_create(**kwargs: object) -> tuple[Path, ...]:
        observed.extend(kwargs["exclusions"])  # type: ignore[arg-type]
        output = kwargs["output_archive"]
        assert isinstance(output, Path)
        output.write_bytes(b"archive")
        return (output,)

    monkeypatch.setattr(archive_utils, "create_7zip_archive", fake_create)
    assert wrapper.main(input_paths=[source], seven_zip_exe=exe, active_log_file=source / "run.log") == 0
    assert "-x!run.log" in observed
    assert "Active log excluded" in capsys.readouterr().out


def test_dashboard_renders_activity_without_completion_percentage(wrapper: ModuleType) -> None:
    from io import StringIO
    from rich.console import Console

    output = StringIO()
    console = Console(file=output, width=80)
    dashboard = wrapper.ArchiveDashboard(console=console, disable=True)
    dashboard.add_task("Compressing", total=1, folder="WeelMLTP", activity="2.46 GiB", rate="1.25 MiB/s")
    console.print(dashboard.get_renderable())
    text = output.getvalue()
    assert "Archive compression" in text
    assert "2.46 GiB" in text
    assert "1.25 MiB/s" in text
    assert "0 / 1" in text
    assert "0%" not in text
