"""Synthetic DXF smoke and no-clobber checks for the standalone CAD converter."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import ezdxf

SCRIPT = Path(__file__).absolute().parents[1] / "ryan-scripts" / "cad-python" / "cad_to_gpkg.py"


def _run_converter(source: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(source), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=45,
    )


def test_dxf_to_gpkg_keeps_geometry_layers_and_refuses_existing_output(tmp_path: Path) -> None:
    """Validate actual GDAL DXF conversion and non-destructive default reruns."""
    source = tmp_path / "sample.dxf"
    drawing = ezdxf.new("R2010")
    space = drawing.modelspace()
    space.add_point((5.0, 6.0, 0.0))
    space.add_line((0.0, 0.0, 0.0), (10.0, 0.0, 0.0))
    drawing.saveas(source)

    result = _run_converter(source, "--crs", "EPSG:28350")
    assert result.returncode == 0, result.stdout + result.stderr

    gpkg = source.with_suffix(".gpkg")
    assert gpkg.is_file()
    with sqlite3.connect(gpkg) as connection:
        layers = {
            str(row[0]): int(row[1])
            for row in connection.execute("SELECT table_name, srs_id FROM gpkg_contents WHERE data_type = 'features'")
        }
        assert layers == {"areas": 28350, "lines": 28350, "points": 28350}
        assert connection.execute('SELECT COUNT(*) FROM "lines"').fetchone()[0] >= 1
        assert connection.execute('SELECT COUNT(*) FROM "points"').fetchone()[0] >= 1

    before = gpkg.read_bytes()
    repeat = _run_converter(source, "--crs", "EPSG:28350")
    assert repeat.returncode != 0
    assert "--overwrite" in repeat.stderr
    assert gpkg.read_bytes() == before
