"""Verify representative packaged wrapper CLIs in a fully provisioned environment."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).absolute().parents[2]


@pytest.mark.parametrize("relative", ["culvert.py", "floodway.py"])
def test_maintained_wrapper_help_runs_without_project_data(relative: str) -> None:
    script = ROOT / "ryan-scripts" / relative
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert result.returncode == 0, f"{relative} help failed: {result.stderr[-1000:]}"
    assert "usage:" in result.stdout.lower()
