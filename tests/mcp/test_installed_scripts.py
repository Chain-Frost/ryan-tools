"""Installed-script allowlist, catalogue coverage and safe copying regression tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from ryan_library.mcp import installed_scripts as installed_script_module
from ryan_library.mcp.installed_scripts import available_wrappers, copy_wrapper, installed_files, main

ROOT = Path(__file__).absolute().parents[2]


def test_manifest_is_an_explicit_selection_not_entire_source_tree() -> None:
    selected = installed_files()
    assert selected == tuple(sorted(set(selected)))
    assert "README.md" in selected
    assert "culvert.py" in selected
    assert "floodway.py" in selected
    assert "python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py" not in selected
    assert "unsorted-python/plot_water_level_profiles.py" not in selected
    assert "gdal-bat/gdal_merge.bat" not in selected
    assert "misc-python/remove_excel_protection.py" in selected  # Existing privileged MCP workflow.
    assert len(selected) < len([p for p in (ROOT / "ryan-scripts").rglob("*") if p.is_file()])
    for script in selected:
        assert ".." not in Path(script).parts
        assert (ROOT / "ryan-scripts" / script).is_file(), script


def test_manifest_covers_all_script_target_workflows() -> None:
    resources = ROOT / "ryan_library" / "resources" / "mcp"
    general = json.loads((resources / "workflows.json").read_text(encoding="utf-8"))
    gdal = json.loads((resources / "gdal_cli_tools.json").read_text(encoding="utf-8"))
    expected = {item["script"].removeprefix("ryan-scripts/") for item in general["workflows"] if "script" in item}
    expected.update("gdal-python/" + item["script"] for item in gdal["tools"] if "module" not in item)
    assert expected <= set(installed_files())


def test_copy_is_byte_identical_and_does_not_overwrite(tmp_path: Path) -> None:
    script = "file-management-python/archive_compress_folder.py"
    source = ROOT / "ryan-scripts" / script
    assert script in available_wrappers()
    target = tmp_path / Path(script).name
    assert copy_wrapper(script, tmp_path) == target
    assert target.read_bytes() == source.read_bytes()

    with pytest.raises(FileExistsError, match="--overwrite"):
        copy_wrapper(script, tmp_path)

    target.write_bytes(b"modified project copy")
    assert copy_wrapper(script, tmp_path, dry_run=True, overwrite=True) == target
    assert target.read_bytes() == b"modified project copy"
    copy_wrapper(script, tmp_path, overwrite=True)
    assert target.read_bytes() == source.read_bytes()


def test_copy_refuses_unlisted_scripts_and_nonexistent_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not in the installed"):
        copy_wrapper("python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py", tmp_path)
    with pytest.raises(NotADirectoryError):
        copy_wrapper("culvert.py", tmp_path / "missing")


def test_cli_list_and_copy_dry_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["list"]) == 0
    assert "culvert.py" in capsys.readouterr().out
    assert main(["copy", "culvert.py", str(tmp_path), "--dry-run"]) == 0
    assert not (tmp_path / "culvert.py").exists()
    assert "Would copy to:" in capsys.readouterr().out


def test_overwrite_replaces_swapped_symlink_without_writing_its_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression for the P2 symlink-swap review finding in PR #130."""
    victim = tmp_path / "outside.txt"
    victim.write_bytes(b"must not change")
    target = tmp_path / "culvert.py"
    target.write_bytes(b"previous contents")
    probe = tmp_path / "symlink-probe"
    try:
        probe.symlink_to(victim)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Host does not permit creating test symlinks: {exc}")
    probe.unlink()
    if os.name == "posix":
        real_replace = os.replace

        def replace_after_swap(
            source: str, destination: str, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None
        ) -> None:
            target.unlink()
            target.symlink_to(victim)
            real_replace(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

        monkeypatch.setattr(os, "replace", replace_after_swap)
    else:
        real_path_replace = Path.replace

        def path_replace_after_swap(source: Path, destination: str | os.PathLike[str]) -> Path:
            target.unlink()
            target.symlink_to(victim)
            return real_path_replace(source, destination)

        monkeypatch.setattr(Path, "replace", path_replace_after_swap)
    result = copy_wrapper("culvert.py", tmp_path, overwrite=True)

    assert result == target
    assert not target.is_symlink()
    assert target.read_bytes() == (ROOT / "ryan-scripts" / "culvert.py").read_bytes()
    assert victim.read_bytes() == b"must not change"
    assert not list(tmp_path.glob(".culvert.py.*.tmp"))


def test_overwrite_replaces_swapped_hardlink_without_modifying_other_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the atomic replacement invariant where symlink creation is restricted."""
    victim = tmp_path / "outside.txt"
    victim.write_bytes(b"must not change")
    target = tmp_path / "culvert.py"
    target.write_bytes(b"previous contents")
    if os.name == "posix":
        real_replace = os.replace

        def replace_after_swap(
            source: str, destination: str, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None
        ) -> None:
            target.unlink()
            os.link(victim, target)
            real_replace(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

        monkeypatch.setattr(os, "replace", replace_after_swap)
    else:
        real_path_replace = Path.replace

        def path_replace_after_swap(source: Path, destination: str | os.PathLike[str]) -> Path:
            target.unlink()
            os.link(victim, target)
            return real_path_replace(source, destination)

        monkeypatch.setattr(Path, "replace", path_replace_after_swap)
    copy_wrapper("culvert.py", tmp_path, overwrite=True)
    assert target.read_bytes() == (ROOT / "ryan-scripts" / "culvert.py").read_bytes()
    assert victim.read_bytes() == b"must not change"


def test_overwrite_copy_failure_preserves_original_and_cleans_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "culvert.py"
    target.write_bytes(b"retain original")

    def failed_copy(_source: object, _destination: object) -> None:
        msg = "copy failed"
        raise OSError(msg)

    monkeypatch.setattr(installed_script_module.shutil, "copyfileobj", failed_copy)
    with pytest.raises(OSError, match="copy failed"):
        copy_wrapper("culvert.py", tmp_path, overwrite=True)
    assert target.read_bytes() == b"retain original"
    assert not list(tmp_path.glob(".culvert.py.*.tmp"))
    assert not list(tmp_path.glob(".culvert.py.stage-*"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits")
@pytest.mark.parametrize("overwrite", [False, True])
def test_copy_preserves_source_mode_for_both_paths(tmp_path: Path, overwrite: bool) -> None:
    import stat

    source_root = tmp_path / "installed"
    source_root.mkdir()
    source = source_root / "culvert.py"
    source.write_bytes(b"print('example')\n")
    source.chmod(0o755)
    destination = tmp_path / "project"
    destination.mkdir()
    target = destination / source.name
    if overwrite:
        target.write_bytes(b"old")
        target.chmod(0o600)
    copy_wrapper("culvert.py", destination, scripts_root=source_root, overwrite=overwrite)
    assert stat.S_IMODE(target.stat().st_mode) == 0o755
    assert target.read_bytes() == source.read_bytes()


@pytest.mark.skipif(os.name != "posix", reason="POSIX private-directory permissions")
def test_staging_is_private_and_mode_set_before_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """P2: an untrusted peer cannot swap staged entries in a shared directory."""
    import stat

    source_root = tmp_path / "installed"
    source_root.mkdir()
    source = source_root / "culvert.py"
    source.write_bytes(b"example contents")
    source.chmod(0o754)
    destination = tmp_path / "shared"
    destination.mkdir()
    target = destination / "culvert.py"
    target.write_bytes(b"old copy")
    target.chmod(0o600)
    real_replace = os.replace

    def inspect_before_publish(
        source_name: str, replacement_name: str, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None
    ) -> None:
        assert src_dir_fd is not None
        assert dst_dir_fd is not None
        assert stat.S_IMODE(os.fstat(src_dir_fd).st_mode) == 0o700
        assert stat.S_IMODE(os.stat(source_name, dir_fd=src_dir_fd).st_mode) == 0o754
        assert os.fstat(src_dir_fd).st_uid == destination.stat().st_uid
        assert source_name == replacement_name == target.name
        real_replace(source_name, replacement_name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(os, "replace", inspect_before_publish)
    copy_wrapper("culvert.py", destination, overwrite=True, scripts_root=source_root)
    assert target.read_bytes() == b"example contents"
    assert stat.S_IMODE(target.stat().st_mode) == 0o754
    assert not list(destination.glob(".culvert.py.stage-*"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX dirfd-based atomic publication")
def test_staging_directory_rename_does_not_publish_substituted_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A renamed staging directory must not change the source of publication."""
    source_root = tmp_path / "installed"
    source_root.mkdir()
    source = source_root / "culvert.py"
    source.write_bytes(b"genuine script contents")
    destination = tmp_path / "shared"
    destination.mkdir()
    target = destination / "culvert.py"
    target.write_bytes(b"old contents")
    victim = tmp_path / "outside.txt"
    victim.write_bytes(b"unchanged victim")
    real_replace = os.replace

    def relocate_stage_before_publish(
        source_name: str, destination_name: str, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None
    ) -> None:
        assert src_dir_fd is not None
        assert dst_dir_fd is not None
        stage = next(destination.glob(".culvert.py.stage-*"))
        relocated = destination / "renamed-staging-directory"
        stage.rename(relocated)
        stage.symlink_to(victim)
        try:
            assert source_name == destination_name == "culvert.py"
            # Publication must use the open directory fd instead of 'stage'.
            real_replace(source_name, destination_name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
        finally:
            stage.unlink()
            relocated.rename(stage)

    monkeypatch.setattr(os, "replace", relocate_stage_before_publish)
    copy_wrapper("culvert.py", destination, overwrite=True, scripts_root=source_root)
    assert not target.is_symlink()
    assert target.read_bytes() == b"genuine script contents"
    assert victim.read_bytes() == b"unchanged victim"
    assert not list(destination.glob(".culvert.py.stage-*"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX fd-based staging cleanup")
def test_overwrite_cleanup_does_not_delete_substituted_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1 regression: do not recursively delete an unrelated replacement directory."""
    source_root = tmp_path / "installed"
    source_root.mkdir()
    source = source_root / "culvert.py"
    source.write_bytes(b"original staged wrapper")
    destination = tmp_path / "shared"
    destination.mkdir()
    target = destination / "culvert.py"
    target.write_bytes(b"previous contents")
    substituted = destination / "unrelated-directory"
    (substituted / "nested").mkdir(parents=True)
    protected = substituted / "nested" / "keep.txt"
    protected.write_bytes(b"must survive")
    real_replace = os.replace

    def replace_after_directory_substitution(
        source_name: str, destination_name: str, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None
    ) -> None:
        assert src_dir_fd is not None
        assert dst_dir_fd is not None
        staged_directory = next(destination.glob(".culvert.py.stage-*"))
        relocated = destination / "renamed-genuine-staging"
        staged_directory.rename(relocated)
        substituted.rename(staged_directory)
        real_replace(source_name, destination_name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(os, "replace", replace_after_directory_substitution)
    copy_wrapper("culvert.py", destination, overwrite=True, scripts_root=source_root)
    assert target.read_bytes() == b"original staged wrapper"
    assert protected.exists() is False  # The original location was renamed.
    substituted_stage = next(destination.glob(".culvert.py.stage-*"))
    assert (substituted_stage / "nested" / "keep.txt").read_bytes() == b"must survive"
    assert (destination / "renamed-genuine-staging").is_dir()


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory security")
def test_posix_rejects_nonsticky_peer_writable_overwrite_destination(tmp_path: Path) -> None:
    """Reject directories where another user can race the final rmdir."""
    destination = tmp_path / "unsafe"
    destination.mkdir()
    target = destination / "culvert.py"
    target.write_bytes(b"do not overwrite")
    destination.chmod(0o777)
    try:
        with pytest.raises(PermissionError, match="unsafe peer-writable"):
            copy_wrapper("culvert.py", destination, overwrite=True)
        assert target.read_bytes() == b"do not overwrite"
        assert not list(destination.glob(".culvert.py.stage-*"))
    finally:
        destination.chmod(0o700)


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory ownership security")
def test_posix_rejects_destination_owned_by_another_user_even_if_not_peer_writable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2: a foreign owner can change 0755 permissions and rename stage entries."""
    import stat

    destination = tmp_path / "foreign-owner"
    destination.mkdir()
    destination.chmod(0o755)
    target = destination / "culvert.py"
    target.write_bytes(b"original wrapper")
    original_stat = destination.stat()
    caller_uid = os.geteuid()
    foreign_uid = caller_uid + 1
    assert foreign_uid not in {0, caller_uid}
    actual_fstat = os.fstat

    def spoof_destination_owner(fd: int) -> os.stat_result:
        observed = actual_fstat(fd)
        if stat.S_ISDIR(observed.st_mode) and (
            observed.st_dev,
            observed.st_ino,
        ) == (original_stat.st_dev, original_stat.st_ino):
            values = list(observed)
            values[4] = foreign_uid  # POSIX stat tuple index for st_uid
            return os.stat_result(values)
        return observed

    monkeypatch.setattr(os, "fstat", spoof_destination_owner)
    with pytest.raises(PermissionError, match="untrusted-owner"):
        copy_wrapper("culvert.py", destination, overwrite=True)

    assert target.read_bytes() == b"original wrapper"
    assert not list(destination.glob(".culvert.py.stage-*"))


@pytest.mark.skipif(os.name != "nt", reason="Windows read-only attributes")
@pytest.mark.parametrize("overwrite", [False, True])
def test_windows_copy_preserves_read_only_attribute(tmp_path: Path, overwrite: bool) -> None:
    """P2: preserve the source read-only attribute for both Windows copy paths."""
    import stat

    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "culvert.py"
    source.write_bytes(b"readonly")
    destination = tmp_path / "project"
    destination.mkdir()
    target = destination / "culvert.py"
    if overwrite:
        target.write_bytes(b"previous writable copy")
    source.chmod(stat.S_IREAD)
    try:
        copy_wrapper("culvert.py", destination, overwrite=overwrite, scripts_root=source_root)
        assert not bool(target.stat().st_mode & stat.S_IWRITE)
        assert target.read_bytes() == source.read_bytes()
    finally:
        source.chmod(stat.S_IWRITE | stat.S_IREAD)
        if target.exists():
            target.chmod(stat.S_IWRITE | stat.S_IREAD)
