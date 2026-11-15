"""Focused error-path regressions for recursive file discovery."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from ryan_library.functions.file_utils import find_files_parallel


def _scan(root: Path, *, skip_inaccessible_subdirectories: bool = False) -> list[Path]:
    return find_files_parallel(
        root_dirs=[root],
        patterns="*.txt",
        report_level=None,
        print_found_folder=False,
        skip_inaccessible_subdirectories=skip_inaccessible_subdirectories,
    )


class TestFindFilesParallelErrors:
    def test_root_dir_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(OSError, match="Root directory does not exist"):
            _scan(tmp_path / "missing")

    def test_iterdir_permission_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        root = tmp_path / "root"
        root.mkdir()
        original_iterdir = Path.iterdir

        def unreadable(path: Path) -> Iterator[Path]:
            if path == root:
                message = "synthetic directory access denied"
                raise PermissionError(message)
            return original_iterdir(path)

        monkeypatch.setattr(Path, "iterdir", unreadable)
        with pytest.raises(OSError, match="Permission denied accessing directory"):
            _scan(root)

    def test_worker_unexpected_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        root = tmp_path / "root"
        root.mkdir()
        original_iterdir = Path.iterdir

        def unexpected(path: Path) -> Iterator[Path]:
            if path == root:
                message = "synthetic unexpected directory error"
                raise RuntimeError(message)
            return original_iterdir(path)

        monkeypatch.setattr(Path, "iterdir", unexpected)
        with pytest.raises(OSError, match="Error accessing directory"):
            _scan(root)

    def test_subdirectory_permission_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        root = tmp_path / "root"
        subdir = root / "sub"
        subdir.mkdir(parents=True)
        original_absolute = Path.absolute

        def forbidden(path: Path) -> Path:
            if path == subdir:
                message = "synthetic subdirectory access denied"
                raise PermissionError(message)
            return original_absolute(path)

        monkeypatch.setattr(Path, "absolute", forbidden)
        with pytest.raises(OSError, match="Permission denied accessing subdirectory"):
            _scan(root)

    def test_file_permission_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "test.txt"
        target.write_text("data", encoding="utf-8")
        original_absolute = Path.absolute

        def forbidden(path: Path) -> Path:
            if path == target:
                message = "synthetic file access denied"
                raise PermissionError(message)
            return original_absolute(path)

        monkeypatch.setattr(Path, "absolute", forbidden)
        with pytest.raises(OSError, match="Permission denied accessing file"):
            _scan(root)

    def test_file_not_found_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "test.txt"
        target.write_text("data", encoding="utf-8")
        original_exists = Path.exists

        def disappeared(path: Path, *, follow_symlinks: bool = True) -> bool:
            if path == target:
                return False
            return original_exists(path, follow_symlinks=follow_symlinks)

        monkeypatch.setattr(Path, "exists", disappeared)
        with pytest.raises(OSError, match="Matched file disappeared"):
            _scan(root)


def test_unreadable_child_directory_does_not_abort_opt_in_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    bad = root / "locked"
    good = root / "good"
    bad.mkdir(parents=True)
    good.mkdir()
    direct = root / "direct.txt"
    nested = good / "nested.txt"
    direct.write_text("direct", encoding="utf-8")
    nested.write_text("nested", encoding="utf-8")
    original_iterdir = Path.iterdir

    def unreadable(path: Path) -> Iterator[Path]:
        if path == bad:
            # Path.iterdir may raise during iteration rather than at creation.
            yield from ()
            raise OSError(59, "synthetic network directory error")
        yield from original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", unreadable)
    assert _scan(root, skip_inaccessible_subdirectories=True) == sorted(
        [direct.absolute(), nested.absolute()],
        key=lambda path: (str(path).casefold(), str(path)),
    )
    with pytest.raises(OSError, match="File discovery incomplete"):
        _scan(root)


def test_unreadable_root_remains_fatal_in_opt_in_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    original_iterdir = Path.iterdir

    def unreadable(path: Path) -> Iterator[Path]:
        if path == root:
            yield from ()
            message = "root denied"
            raise PermissionError(message)
        yield from original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", unreadable)
    with pytest.raises(OSError, match="File discovery incomplete"):
        _scan(root, skip_inaccessible_subdirectories=True)


def test_unreadable_child_entry_is_skipped_in_opt_in_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    bad = root / "locked"
    bad.mkdir(parents=True)
    good = root / "good.txt"
    good.write_text("good", encoding="utf-8")
    original_absolute = Path.absolute

    def unreadable(path: Path) -> Path:
        if path == bad:
            message = "synthetic subdirectory permission error"
            raise PermissionError(message)
        return original_absolute(path)

    monkeypatch.setattr(Path, "absolute", unreadable)
    assert _scan(root, skip_inaccessible_subdirectories=True) == [good.absolute()]
