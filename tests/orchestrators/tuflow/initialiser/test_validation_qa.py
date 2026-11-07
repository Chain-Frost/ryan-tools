"""Fail-closed stream QA, including signed slopes and null geometry."""

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import fiona
import pytest
from shapely.geometry import LineString, mapping

from ryan_library.orchestrators.tuflow.initialiser.verification import verify_project, verify_streams


def test_validation_checks_survive_optimisation() -> None:
    command = [
        sys.executable,
        "-O",
        "-c",
        "from ryan_library.orchestrators.tuflow.initialiser.verification import _require; _require('invalid output', condition=False)",
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[4] / "src")
    result = subprocess.run(
        command,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        cwd=Path(__file__).resolve().parents[4],
    )
    assert result.returncode != 0
    assert "ValueError: invalid output" in result.stderr


def test_missing_project_fails_validation(tmp_path: Path) -> None:
    with pytest.raises(fiona.errors.DriverError):
        verify_project(tmp_path)


@pytest.mark.parametrize("slope", [-0.01, 0.01])
def test_signed_stream_slopes_are_retained(tmp_path: Path, slope: float) -> None:
    path = tmp_path / "model/gis/streams.gpkg"
    path.parent.mkdir(parents=True)
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        crs_wkt=None,
        schema={
            "geometry": "LineString",
            "properties": {
                "path_id": "int",
                "path_type": "str",
                "length_m": "float",
                "slope_ea": "float",
            },
        },
    ) as collection:
        collection.write(
            {
                "geometry": mapping(LineString([(0, 0), (10, 0)])),
                "properties": {
                    "path_id": 1,
                    "path_type": "channel",
                    "length_m": 10.0,
                    "slope_ea": slope,
                },
            }
        )
    report = verify_streams(tmp_path)
    assert report["features"] == 1
    assert report["negative_slopes"] == int(slope < 0)
    if slope < 0:
        assert report["negative_slope_reaches"] == [
            {
                "path_id": 1,
                "path_type": "channel",
                "length_m": 10.0,
                "slope_ea": slope,
            }
        ]
    assert json.loads(json.dumps(report)) == report


def test_null_stream_geometry_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "model/gis/streams.gpkg"
    path.parent.mkdir(parents=True)
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        schema={
            "geometry": "LineString",
            "properties": {
                "path_id": "int",
                "path_type": "str",
                "length_m": "float",
                "slope_ea": "float",
            },
        },
    ) as collection:
        collection.write(
            {
                "geometry": None,
                "properties": {
                    "path_id": 1,
                    "path_type": "channel",
                    "length_m": 10.0,
                    "slope_ea": 0.01,
                },
            }
        )
    with pytest.raises(ValueError, match="no geometry"):
        verify_streams(tmp_path)


def test_windows_parity_runner_enables_catchment_outputs() -> None:
    """The parity verifier requires the outputs guarded by generate_catchments."""
    source = (
        Path(__file__).absolute().parents[4]
        / "repo-scripts"
        / "tuflow-initialiser"
        / "validate_test_project.py"
    )
    tree = ast.parse(source.read_text(encoding="utf-8"))
    configs = (
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "BuildConfig"
    )
    assert any(
        isinstance(keyword.value, ast.Constant) and keyword.value.value is True
        for config in configs
        for keyword in config.keywords
        if keyword.arg == "generate_catchments"
    )
