"""Source-checkout import isolation, independent of installed-wheel smoke tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROBE = PROJECT_ROOT / "repo-scripts" / "check_compatibility_imports.py"


def test_source_checkout_imports_do_not_require_optional_modules(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PROJECT_ROOT)
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("RYAN_TOOLS_REPOSITORY_ROOT", None)

    result = subprocess.run(
        [sys.executable, "-B", str(PROBE), "--expected-root", str(PROJECT_ROOT)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        stdin=subprocess.DEVNULL,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Import-isolation and provenance check passed:" in result.stdout
