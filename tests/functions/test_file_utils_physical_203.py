"""Physical directory discovery regressions for #203 (WP07)."""

import os
import subprocess
from pathlib import Path

import pytest

from ryan_library.functions.file_utils import find_files_parallel


def _search(roots: list[Path], *, recursive: bool = True) -> list[Path]:
    return find_files_parallel(
        root_dirs=roots,
        patterns="*.csv",
        recursive_search=recursive,
        report_level=None,
        print_found_folder=False,
    )


@pytest.mark.parametrize("loop_target", ["self", "ancestor"])
def test_directory_symlink_cycles_do_not_repeat_files(tmp_path: Path, loop_target: str) -> None:
    root = tmp_path / "model"
    deep = root / "first" / "second"
    deep.mkdir(parents=True)
    source = deep / "result.csv"
    source.write_text("data", encoding="utf-8")
    link = deep / "loop"
    destination = deep if loop_target == "self" else root
    try:
        link.symlink_to(destination, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlink privilege unavailable: {exc}")

    assert _search([root]) == [source.absolute()]


def test_alias_roots_preserve_first_users_path(tmp_path: Path) -> None:
    physical = tmp_path / "physical"
    physical.mkdir()
    (physical / "result.csv").write_text("value", encoding="utf-8")
    alias = tmp_path / "mapped"
    try:
        alias.symlink_to(physical, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlink privilege unavailable: {exc}")

    assert _search([alias, physical, alias]) == [(alias / "result.csv").absolute()]


def test_overlapping_roots_and_stable_sorting(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (parent / "z.csv").write_text("z", encoding="utf-8")
    (parent / "a.csv").write_text("a", encoding="utf-8")
    (child / "m.csv").write_text("m", encoding="utf-8")

    results = _search([parent, child])
    assert results == sorted(
        [(parent / "a.csv").absolute(), (child / "m.csv").absolute(), (parent / "z.csv").absolute()],
        key=lambda p: (str(p).casefold(), str(p)),
    )
    assert _search([parent], recursive=False) == [(parent / "a.csv").absolute(), (parent / "z.csv").absolute()]


def test_distinct_file_link_spellings_remain_distinct(tmp_path: Path) -> None:
    root = tmp_path / "files"
    root.mkdir()
    source = root / "real.csv"
    source.write_text("data", encoding="utf-8")
    alias = root / "alias.csv"
    try:
        alias.symlink_to(source)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"File symlink privilege unavailable: {exc}")

    assert _search([root]) == [alias.absolute(), source.absolute()]


def test_directory_read_error_rejects_partial_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    bad = root / "bad"
    bad.mkdir(parents=True)
    (root / "good.csv").write_text("good", encoding="utf-8")
    original_iterdir = Path.iterdir

    def fail_on_bad(path: Path):  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        if path == bad:
            raise PermissionError("synthetic denial")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_on_bad)
    with pytest.raises(OSError, match="File discovery incomplete.*bad"):
        _search([root])


def test_invalid_root_is_not_a_valid_empty_scan(tmp_path: Path) -> None:
    assert _search([tmp_path]) == []
    with pytest.raises(OSError, match="Root directory does not exist"):
        _search([tmp_path / "typo"])


@pytest.mark.skipif(os.name != "nt", reason="Windows junction requires cmd.exe")
def test_windows_junction_to_ancestor(tmp_path: Path) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "sample.csv").write_text("data", encoding="utf-8")
    junction = nested / "junction"
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Junction unavailable: {result.stderr}")
    try:
        assert _search([root]) == [(root / "sample.csv").absolute()]
    finally:
        junction.rmdir()
