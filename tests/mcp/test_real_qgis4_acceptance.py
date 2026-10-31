"""Opt-in QGIS 4 Python-provider and CLI validation, never run in ordinary CI.

Use a QGIS 4 / OSGeo4W Python environment with installed QGIS bindings. This
test starts a separate QGIS process only when explicitly requested, and never
opens or modifies an existing engineering project.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RYAN_CF_QGIS_ACCEPTANCE") != "1",
    reason="Real QGIS 4 acceptance is opt-in: set RYAN_CF_QGIS_ACCEPTANCE=1.",
)


def test_real_qgis4_python_provider_reads_synthetic_vector(tmp_path: Path) -> None:
    source = tmp_path / "QGIS 4 測試 with spaces.geojson"
    source.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": "example"},
                        "geometry": {"type": "Point", "coordinates": [116.0, -22.0]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    before = source.read_bytes()
    code = (
        "import json, sys\n"
        "from qgis.core import QgsApplication, QgsVectorLayer, Qgis\n"
        "app = QgsApplication([], False)\n"
        "app.initQgis()\n"
        "try:\n"
        "    layer = QgsVectorLayer(sys.argv[1], 'synthetic', 'ogr')\n"
        "    assert Qgis.QGIS_VERSION_INT // 10000 == 4, Qgis.QGIS_VERSION\n"
        "    assert layer.isValid(), 'QGIS OGR provider cannot read GeoJSON'\n"
        "    assert layer.featureCount() == 1, layer.featureCount()\n"
        "    print(json.dumps({'qgis': Qgis.QGIS_VERSION, 'features': layer.featureCount()}))\n"
        "finally:\n"
        "    app.exitQgis()\n"
    )
    python = os.environ.get("RYAN_CF_QGIS_PYTHON") or sys.executable
    env = os.environ.copy()
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    result = subprocess.run(
        [python, "-c", code, str(source)], text=True, capture_output=True, env=env, check=False, timeout=90
    )
    assert result.returncode == 0, (
        "QGIS 4 Python/provider integration failed; use a configured QGIS/OSGeo4W environment. "
        f"stdout={result.stdout}, stderr={result.stderr}"
    )
    assert '"features": 1' in result.stdout
    assert source.read_bytes() == before


def test_real_qgis4_process_executable_version() -> None:
    command = os.environ.get("RYAN_CF_QGIS_PROCESS") or shutil.which("qgis_process")
    if command is None:
        pytest.fail("qgis_process not found. Use QGIS 4 shell or set RYAN_CF_QGIS_PROCESS.")
    candidate = Path(command).expanduser().absolute()
    if not candidate.is_file():
        pytest.fail(f"Configured QGIS process executable missing: {candidate}")
    result = subprocess.run([str(candidate), "--version"], text=True, capture_output=True, check=False, timeout=60)
    output = result.stdout + result.stderr
    assert result.returncode == 0, f"qgis_process --version failed: {output}"
    assert re.search(r"\b4\.\d+", output), f"QGIS 4 version not confirmed: {output}"
