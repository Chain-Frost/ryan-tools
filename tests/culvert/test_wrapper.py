"""Process-boundary smoke tests for the maintained culvert wrapper."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = PROJECT_ROOT / "ryan-scripts" / "culvert.py"


def _environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(PROJECT_ROOT), str(PROJECT_ROOT / "vendor" / "ryan_culverts" / "src"))
    )
    return environment


def test_wrapper_analyse_writes_expected_outputs(tmp_path: Path) -> None:
    project_path = tmp_path / "project.json"
    project_path.write_text(
        json.dumps(
            {
                "name": "Smoke test",
                "crossings": [
                    {
                        "name": "Crossing",
                        "groups": [
                            {
                                "name": "Pipes",
                                "quantity": 2,
                                "barrel": {
                                    "shape": "circular",
                                    "diameter_mm": 1200,
                                    "length": 40,
                                    "inlet_invert": 10,
                                    "outlet_invert": 9.5,
                                    "roughness": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                    }
                ],
                "scenarios": [{"name": "Design", "discharge": 4, "tailwater_elevation": 10}],
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "analyse",
            "--directory",
            str(tmp_path),
            "--project",
            str(project_path),
            "--no-pause",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "culvert_results" / "scenario_results.json").is_file()
    assert (tmp_path / "culvert_results" / "scenario_results.csv").is_file()
    assert (tmp_path / "culvert_results" / "scenario_results.md").is_file()


def test_wrapper_missing_working_directory_returns_one(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    completed = subprocess.run(
        [sys.executable, str(WRAPPER), "analyse", "--directory", str(missing), "--no-pause"],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 1
    assert not missing.exists()
