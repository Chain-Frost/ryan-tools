"""Host-correct file and wildcard matching across bulk/direct TUFLOW preflight."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from ryan_library.functions.tuflow.dependency_index import (
    check_unique_dependencies,
    expand_file_patterns,
)


@pytest.mark.parametrize("threshold,limit", [(1, 100), (8, 100), (1, 1)])
def test_bulk_direct_and_bounded_fallback_agree_on_case(
    tmp_path: Path, threshold: int, limit: int
) -> None:
    """The same filename must not change status when grouping changes."""
    (tmp_path / "Materials.csv").write_text("example", encoding="utf-8")
    requested = tmp_path / "materials.csv"
    other = [tmp_path / f"extra_{i}.csv" for i in range(7)]
    for file in other:
        file.touch()
    inventory = check_unique_dependencies(
        {requested: (), **dict.fromkeys(other, ())},
        scan_threshold=threshold,
        maximum_directory_entries=limit,
    )
    assert inventory.files[requested].status == (
        "present" if os.name == "nt" else "missing"
    )
    assert all(inventory.files[path].status == "present" for path in other)


@pytest.mark.skipif(os.name != "posix", reason="Requires a case-sensitive Linux filesystem")
def test_posix_distinct_case_only_files_do_not_alias(tmp_path: Path) -> None:
    upper, lower = tmp_path / "Mat.csv", tmp_path / "mat.csv"
    upper.write_text("upper", encoding="utf-8")
    lower.write_text("lower", encoding="utf-8")
    requirements = {upper: (), lower: ()}
    for threshold in (1, 8):
        inventory = check_unique_dependencies(requirements, scan_threshold=threshold)
        assert len(inventory.files) == 2
        assert all(result.status == "present" for result in inventory.files.values())
    upper.unlink()
    for threshold in (1, 8):
        inventory = check_unique_dependencies(requirements, scan_threshold=threshold)
        assert inventory.files[upper].status == "missing"
        assert inventory.files[lower].status == "present"


def test_wildcard_case_follows_host_filesystem(tmp_path: Path) -> None:
    (tmp_path / "Alpha.CSV").touch()
    (tmp_path / "Beta.csv").touch()
    requested = tmp_path / "*.CSV"
    matches, errors, scans = expand_file_patterns([requested])
    assert scans == 1 and not errors
    assert {match.name for match in matches[requested]} == (
        {"Alpha.CSV", "Beta.csv"} if os.name == "nt" else {"Alpha.CSV"}
    )


@pytest.mark.parametrize("threshold,limit", [(1, 100), (8, 100), (1, 1)])
def test_missing_gpkg_case_does_not_open_or_certify_layer(
    tmp_path: Path, threshold: int, limit: int
) -> None:
    """POSIX mismatch must not be masked by GeoPackage case-insensitive tables."""
    actual = tmp_path / "Terrain.GPKG"
    with sqlite3.connect(actual) as db:
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute("INSERT INTO gpkg_contents VALUES ('2d_code')")
    requested = tmp_path / "terrain.gpkg"
    inventory = check_unique_dependencies(
        {requested: ("2D_CODE",)},
        scan_threshold=threshold,
        maximum_directory_entries=limit,
    )
    if os.name == "nt":
        assert inventory.files[requested].status == "present"
        assert inventory.layers[(requested, "2D_CODE")].status == "present"
    else:
        assert inventory.files[requested].status == "missing"
        assert not inventory.layers


@pytest.mark.skipif(os.name != "posix", reason="Requires a case-sensitive Linux filesystem")
def test_posix_alias_and_original_spelling(tmp_path: Path) -> None:
    original = tmp_path / "Materials.csv"
    original.touch()
    alias = tmp_path / "Linked.csv"
    try:
        alias.symlink_to(original)
    except (NotImplementedError, OSError):
        pytest.skip("Symlinks unavailable")
    wrong = tmp_path / "linked.csv"
    for threshold in (1, 8):
        inventory = check_unique_dependencies(
            {original: (), alias: (), wrong: ()}, scan_threshold=threshold
        )
        assert inventory.files[original].status == "present"
        assert inventory.files[alias].status == "present"
        assert inventory.files[wrong].status == "missing"
