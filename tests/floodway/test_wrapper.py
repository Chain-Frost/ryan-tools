"""Process-boundary smoke tests for the maintained floodway wrapper."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = PROJECT_ROOT / "ryan-scripts" / "floodway.py"


def _environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(PROJECT_ROOT), str(PROJECT_ROOT / "vendor" / "ryan_culverts" / "src"))
    )
    return environment


def _write_inputs(directory: Path) -> tuple[Path, Path]:
    project = directory / "culvert_project.json"
    project.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "Floodway smoke test",
                "crossings": [
                    {
                        "name": "Floodway",
                        "groups": [
                            {
                                "name": "Relief pipe",
                                "barrel": {
                                    "shape": "circular",
                                    "diameter_mm": 600,
                                    "length_m": 30,
                                    "inlet_invert_elevation_m": 9.4,
                                    "outlet_invert_elevation_m": 9.2,
                                    "roughness_manning_n": 0.013,
                                    "material": "concrete_pipe",
                                },
                            }
                        ],
                        "roadway": {
                            "type": "profile",
                            "points": [
                                {"station_m": 0.0, "elevation_m": 10.30},
                                {"station_m": 10.0, "elevation_m": 10.10},
                                {"station_m": 20.0, "elevation_m": 10.25},
                            ],
                            "discharge_coefficient": 1.7,
                            "surface": "paved",
                        },
                    }
                ],
                "scenarios": [
                    {
                        "name": "Overtopping",
                        "aep_percent": 2.0,
                        "discharge_m3s": 8.0,
                        "tailwater": {"type": "fixed", "elevation_m": 9.5},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    formation = directory / "floodway_formation.json"
    formation.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "Floodway formation",
                "crest_flow_length_m": 9.0,
                "zones": [
                    {"zone": "D", "slope": 0.03, "roughness_manning_n": 0.015},
                    {"zone": "C", "elevation_m": 10.0},
                    {"zone": "B", "slope": 1.0 / 3.0, "roughness_manning_n": 0.04},
                ],
            }
        ),
        encoding="utf-8",
    )
    return project, formation


def test_wrapper_assessment_writes_reviewable_outputs(tmp_path: Path) -> None:
    project, formation = _write_inputs(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "--directory",
            str(tmp_path),
            "--project",
            str(project),
            "--formation",
            str(formation),
            "--sweep-points",
            "0",
            "--no-pause",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 0, completed.stderr
    output = tmp_path / "floodway_results"
    assert (output / "floodway_scenarios.json").is_file()
    assert (output / "floodway_scenarios.md").is_file()
    assert (output / "floodway_envelope.json").is_file()
    assert (output / "floodway_governors.csv").is_file()
    assert (output / "floodway_envelope.md").is_file()


def test_wrapper_missing_working_directory_returns_one(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    completed = subprocess.run(
        [sys.executable, str(WRAPPER), "--directory", str(missing), "--no-pause"],
        check=False,
        capture_output=True,
        text=True,
        env=_environment(),
    )

    assert completed.returncode == 1
    assert not missing.exists()


def test_wrapper_rejects_invalid_console_log_level_without_traceback() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
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
