from pathlib import Path


from ryan_library.functions.file_utils import (
    ensure_output_directory,
    find_files_parallel,
    is_non_zero_file,
)


def _write_file(path: Path, content: str = "data") -> None:
    path.write_text(content, encoding="utf-8")


def test_find_files_parallel_non_recursive_respects_excludes(tmp_path: Path) -> None:
    root = tmp_path / "root"
    sub = root / "nested"
    sub.mkdir(parents=True)
    keep = root / "keep.txt"
    skip = root / "skip.txt"
    nested = sub / "nested.txt"
    _write_file(keep)
    _write_file(skip)
    _write_file(nested)

    results = find_files_parallel(
        root_dirs=[root],
        patterns="*.txt",
        excludes=["skip.txt"],
        recursive_search=False,
        report_level=None,
        print_found_folder=False,
    )

    assert results == [keep.resolve()]


def test_find_files_parallel_deduplicates_overlapping_roots(tmp_path: Path) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    target = nested / "match.csv"
    _write_file(target)

    results = find_files_parallel(
        root_dirs=[root, nested],
        patterns="*.csv",
        recursive_search=True,
        report_level=None,
        print_found_folder=False,
    )

    assert results == [target.resolve()]


def test_is_non_zero_file_variants(tmp_path: Path) -> None:
    data_file = tmp_path / "data.txt"
    empty_file = tmp_path / "empty.txt"
    directory = tmp_path / "dir"
    directory.mkdir()
    _write_file(data_file)
    empty_file.touch()

    assert is_non_zero_file(data_file)
    assert is_non_zero_file(str(data_file))
    assert not is_non_zero_file(empty_file)
    assert not is_non_zero_file(directory)
    assert not is_non_zero_file(tmp_path / "missing.txt")


def test_ensure_output_directory_creates_missing(tmp_path: Path) -> None:
    out_dir = tmp_path / "exports"
    ensure_output_directory(out_dir)
    assert out_dir.exists() and out_dir.is_dir()

    # second invocation should be a no-op
    ensure_output_directory(out_dir)
