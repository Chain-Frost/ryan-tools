"""Source-tree behavior regressions; isolated wheel acceptance runs separately.

These tests copy source-tree wrappers into disposable directories to protect
their behavior during development. They do not validate wheel packaging.
The package CI job performs independent executable checks against an isolated
installed wheel in `repo-scripts/smoke_test_installed_wheel.py`.
"""

from __future__ import annotations

import csv
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).absolute().parents[2] / "ryan-scripts"


def _run_copied_wrapper(relative: str, work: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a source-tree wrapper on disposable data; do not claim wheel isolation."""
    source = ROOT / relative
    target = work / source.name
    shutil.copy2(source, target)
    return subprocess.run(
        [sys.executable, str(target), *args],
        cwd=work,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        stdin=subprocess.DEVNULL,
    )


def test_copied_rorb_wrapper_reports_missing_input_without_creating_output(tmp_path: Path) -> None:
    """A missing input is a clean nonzero result and must not create output data."""
    missing = tmp_path / "missing-input.parquet"
    output_dir = tmp_path / "unused-output"
    result = _run_copied_wrapper(
        "RORB-python/plot_ensemble_results.py",
        tmp_path,
        "--input",
        str(missing),
        "--output-dir",
        str(output_dir),
        "--no-pause",
    )
    assert result.returncode == 1, result.stderr[-2000:]
    assert not output_dir.exists()
    assert not missing.exists()


def test_copied_file_inventory_respects_cli_defaults_and_writes_expected_csv(tmp_path: Path) -> None:
    """End-to-end, nondestructive CSV export with an explicit input/output."""
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "ABC-001 Rev2.txt").write_text("first", encoding="utf-8")
    (inputs / "XYZ-002 Rev1.txt").write_text("second", encoding="utf-8")
    output = tmp_path / "reports"
    result = _run_copied_wrapper(
        "file-management-python/file_inventory.py",
        tmp_path,
        str(inputs),
        "--output-directory",
        str(output),
        "--basename",
        "inventory-smoke",
        "--no-excel",
        "--no-pause",
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    csv_file = output / "inventory-smoke.csv"
    assert csv_file.is_file()
    assert not (output / "inventory-smoke.xlsx").exists()
    with csv_file.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert all(row for row in rows)


def test_copied_file_list_wrapper_writes_only_disposable_output(tmp_path: Path) -> None:
    """The original wrapper retains its editable default output filename."""
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    first = inputs / "model-01.txt"
    first.write_text("one", encoding="utf-8")
    result = _run_copied_wrapper(
        "file-management-python/list_files_recursive.py",
        tmp_path,
        "--input_directories",
        str(inputs),
        "--no-pause",
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    listing = inputs / "filenames.txt"
    assert listing.is_file()
    assert str(first) in listing.read_text(encoding="utf-8")


def test_copied_geotiff_compressor_is_read_only_by_default(tmp_path: Path) -> None:
    """No replacement flag means auditing an empty folder creates no rasters."""
    work = tmp_path / "rasters"
    work.mkdir()
    result = _run_copied_wrapper("gdal-python/recompress_geotiffs.py", tmp_path, str(work), "--no-pause")
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    assert list(work.iterdir()) == []


def test_copied_file_inventory_rejects_output_traversal_before_creating_reports(tmp_path: Path) -> None:
    """Reject a report basename containing a parent path without touching input files."""
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    original = inputs / "ABC-001 Rev2.txt"
    original.write_bytes(b"original-content")
    reports = tmp_path / "reports"
    result = _run_copied_wrapper(
        "file-management-python/file_inventory.py",
        tmp_path,
        str(inputs),
        "--output-directory",
        str(reports),
        "--basename",
        "../outside",
        "--no-excel",
        "--no-pause",
    )
    assert result.returncode == 2, (result.stdout + result.stderr)[-2000:]
    assert original.read_bytes() == b"original-content"
    assert not reports.exists()
    assert not (tmp_path / "outside.csv").exists()


def test_copied_file_list_rejects_missing_input_without_writing_output(tmp_path: Path) -> None:
    """Invalid input directories must not produce a misleading successful file list."""
    missing = tmp_path / "missing-input"
    result = _run_copied_wrapper(
        "file-management-python/list_files_recursive.py",
        tmp_path,
        "--input_directories",
        str(missing),
        "--no-pause",
    )
    assert result.returncode == 1, (result.stdout + result.stderr)[-2000:]
    assert not missing.exists()
    assert not (missing / "filenames.txt").exists()


def test_copied_file_inventory_hashes_unicode_filename_without_modifying_input(tmp_path: Path) -> None:
    """SHA-256 and UTF-8 filenames survive the copied-wrapper CSV workflow."""
    inputs = tmp_path / "input files"
    inputs.mkdir()
    original = inputs / "ABC-001_Rev2_café.txt"
    payload = "terrain, résumé\n".encode()
    original.write_bytes(payload)
    (inputs / "not-an-input.bin").write_bytes(b"untouched")
    reports = tmp_path / "reports"

    result = _run_copied_wrapper(
        "file-management-python/file_inventory.py",
        tmp_path,
        str(inputs),
        "--pattern",
        "*.txt",
        "--output-directory",
        str(reports),
        "--basename",
        "sha-audit",
        "--sha256",
        "--no-excel",
        "--no-pause",
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    with (reports / "sha-audit.csv").open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1
    row = rows[0]
    assert row["filename"] == original.name
    assert row["document_number"] == "ABC-001"
    assert row["revision"] == "2"
    assert row["sha256"] == hashlib.sha256(payload).hexdigest()
    assert row["file_size_bytes"] == str(len(payload))
    assert row["error"] == ""
    assert original.read_bytes() == payload
    assert not (reports / "sha-audit.xlsx").exists()


def test_copied_file_inventory_nonrecursive_excludes_nested_files(tmp_path: Path) -> None:
    """The explicit nonrecursive flag must override the embedded recursive default."""
    inputs = tmp_path / "inputs"
    nested = inputs / "nested"
    nested.mkdir(parents=True)
    (inputs / "A-001.txt").write_bytes(b"root")
    (nested / "B-002.txt").write_bytes(b"nested")
    reports = tmp_path / "reports"

    result = _run_copied_wrapper(
        "file-management-python/file_inventory.py",
        tmp_path,
        str(inputs),
        "--no-recursive",
        "--output-directory",
        str(reports),
        "--no-excel",
        "--no-pause",
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    with (reports / "file_inventory.csv").open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert [row["filename"] for row in rows] == ["A-001.txt"]
    assert (nested / "B-002.txt").read_bytes() == b"nested"
