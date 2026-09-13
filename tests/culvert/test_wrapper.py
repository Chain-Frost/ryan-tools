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


def _write_project(path: Path, *, discharge: float) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
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
                                    "length_m": 40,
                                    "inlet_invert_elevation_m": 10,
                                    "outlet_invert_elevation_m": 9.5,
                                    "roughness_manning_n": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                    }
                ],
                "scenarios": [
                    {
                        "name": "Design",
                        "discharge_m3s": discharge,
                        "tailwater": {"type": "fixed", "elevation_m": 10},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_wrapper_analyse_writes_expected_outputs(tmp_path: Path) -> None:
    project_path = _write_project(tmp_path / "project.json", discharge=4.0)

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


def test_wrapper_report_uses_saved_results_without_project_or_solve(tmp_path: Path) -> None:
    output = tmp_path / "culvert_results"
    output.mkdir()
    (output / "scenario_results.json").write_text(
        json.dumps(
            [
                {
                    "alternative": None,
                    "crossing": "Saved crossing",
                    "scenario": "Saved event",
                    "discharge_m3s": 2.0,
                    "headwater_elevation_m": 11.0,
                    "tailwater_elevation_m": 9.5,
                    "maximum_outlet_velocity_ms": 1.2,
                    "status": "valid",
                    "warning_codes": [],
                    "applicability_codes": [],
                }
            ]
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "report",
            "--directory",
            str(tmp_path),
            "--project",
            "does-not-exist.json",
            "--no-pause",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 0, completed.stderr
    assert "Saved crossing" in (output / "scenario_results.md").read_text(encoding="utf-8")


def test_wrapper_rejects_invalid_console_log_level_without_traceback() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "analyse",
            "--console-log-level",
            "not-a-log-level",
            "--no-pause",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 2
    assert "Traceback" not in completed.stderr


def test_wrapper_rating_defaults_support_low_flow_scenario(tmp_path: Path) -> None:
    project_path = _write_project(tmp_path / "low-flow.json", discharge=0.005)

    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "rating",
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
    assert (tmp_path / "culvert_results" / "rating_curve.csv").is_file()
    assert (tmp_path / "culvert_results" / "rating_curve.json").is_file()
