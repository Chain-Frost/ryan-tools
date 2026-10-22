"""Bounded subprocess tests for the wheel-selected standalone RORB hydrograph script.

A copied script processes only disposable fixture CSVs, never project inputs.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).absolute().parents[2]
SOURCE = ROOT / "ryan-scripts" / "RORB-python" / "hydrograph_extract_rorb_csv.py"


def _run_wrapper(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Copy the standalone script and run it outside the source checkout."""
    executable = tmp_path / "hydrograph_extract_rorb_csv.py"
    shutil.copy2(SOURCE, executable)
    return subprocess.run(
        [sys.executable, str(executable), *args],
        cwd=tmp_path,
        env={**os.environ, "MPLBACKEND": "Agg"},
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
        timeout=60,
    )


def test_hydrograph_cli_help_does_not_process_files(tmp_path: Path) -> None:
    result = _run_wrapper(tmp_path, "--help")
    assert result.returncode == 0, result.stderr
    assert "--no-pause" in result.stdout
    assert "--input-directory" in result.stdout
    assert not list(tmp_path.glob("*.xlsx"))
    assert not list(tmp_path.glob("*.png"))


def test_hydrograph_rejects_missing_input_directory(tmp_path: Path) -> None:
    missing = tmp_path / "missing hydrographs"
    result = _run_wrapper(tmp_path, "--input-directory", str(missing), "--no-pause")
    assert result.returncode == 2, (result.stdout + result.stderr)[-1500:]
    assert not missing.exists()
    assert not list(tmp_path.glob("*.xlsx"))
    assert not list(tmp_path.glob("*.png"))


def test_hydrograph_headless_export_preserves_input(tmp_path: Path) -> None:
    inputs = tmp_path / "input data"
    inputs.mkdir()
    fixture = inputs / "ExampleCreek_01_aep1_du12hourtp3.csv"
    original = (
        "RORB synthetic fixture\\n"
        "Hydrograph output\\n"
        "Inc,Time (hrs),Calculated hydrograph:  Outlet\\n"
        "1,0,0\\n"
        "2,1,2\\n"
        "3,2,1\\n"
    ).replace("\\n", "\n").encode()
    fixture.write_bytes(original)

    result = _run_wrapper(tmp_path, "--input-directory", str(inputs), "--no-pause")
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    assert "Press Enter" not in result.stdout
    assert len(list(inputs.glob("combined_hydrograph_data_ExampleCreek_*.xlsx"))) == 1
    assert len(list(inputs.glob("hydrograph_plot_ExampleCreek_*.png"))) == 1
    assert fixture.read_bytes() == original
    assert not list(tmp_path.glob("*.xlsx"))
    assert not list(tmp_path.glob("*.png"))
