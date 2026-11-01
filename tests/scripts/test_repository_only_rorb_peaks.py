"""Disposable regression fixtures for the repository-only RORB peaks script."""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).absolute().parents[2]
SOURCE = ROOT / "ryan-scripts" / "RORB-python" / "rorb-peaks_v2.py"


def _copy_script(tmp_path: Path) -> Path:
    root = tmp_path / "RORB scripts"
    root.mkdir()
    copied = root / SOURCE.name
    shutil.copy2(SOURCE, copied)
    return copied


def _run(script: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script), *args],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_import_does_not_scan_change_cwd_or_overwrite_existing_report(tmp_path: Path) -> None:
    script = _copy_script(tmp_path)
    (script.parent / "model.out").write_text("incomplete RORB content", encoding="latin1")
    report = script.parent / "processed_output.csv"
    report.write_bytes(b"existing output should be unchanged")
    elsewhere = tmp_path / "unrelated directory"
    elsewhere.mkdir()
    code = (
        "import os, runpy, sys; "
        "original = os.getcwd(); "
        "runpy.run_path(sys.argv[1], run_name='rorb_import_only'); "
        "assert os.getcwd() == original"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(script)],
        cwd=elsewhere,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr
    assert report.read_bytes() == b"existing output should be unchanged"
    assert "Processing Summary" not in result.stdout


def test_main_with_no_out_files_preserves_original_error(tmp_path: Path) -> None:
    script = _copy_script(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    result = _run(script, elsewhere)
    assert result.returncode != 0
    assert "No .out files found in the directory and its subdirectories." in result.stderr
    assert not (script.parent / "processed_output.csv").exists()
    assert not (elsewhere / "processed_output.csv").exists()


def test_main_writes_upper_median_tpat_beside_script_without_chdir(tmp_path: Path) -> None:
    script = _copy_script(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    nested = script.parent / "nested outputs"
    nested.mkdir()
    fixture = nested / "River_A_001.out"
    payload = (
        "Parameters:  kc = 1.5      m = 0.7\n"
        "Initial loss (mm)    10.0    2.0\n"
        "Run        Duration             AEP        TPat        Peak_Flow\n"
        "run1        60m        01.0p        TP01        1.0\n"
        "run2        60m        01.0p        TP02        3.0\n"
        "run3        60m        01.0p        TP03        4.0\n"
        "run4        60m        01.0p        TP04        6.0\n"
    )
    fixture.write_bytes(payload.encode("latin1"))
    (nested / "no-median.out").write_bytes(b"no valid rows")

    result = _run(script, elsewhere)
    assert result.returncode == 0, result.stderr
    assert "Total files processed: 1" in result.stdout
    assert "Total files skipped: 1" in result.stdout
    report = script.parent / "processed_output.csv"
    assert report.is_file()
    with report.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1
    row = rows[0]
    assert row["Filename"] == fixture.name
    assert row["Relative_Path"] == "nested outputs"
    assert row["Duration"] == "60m"
    assert row["AEP"] == "01.0p"
    assert float(row["Median_Peak_Flow"]) == 4.0
    assert row["Median_TPat_Peak_Flow"] == "TP03"
    assert float(row["kc"]) == 1.5
    assert float(row["m"]) == 0.7
    assert float(row["Initial_loss_mm"]) == 10.0
    assert float(row["Cont_loss_mm_per_h"]) == 2.0
    assert row["r0"] == "River"
    assert row["r1"] == "A"
    assert fixture.read_bytes() == payload.encode("latin1")
    assert not (elsewhere / "processed_output.csv").exists()
