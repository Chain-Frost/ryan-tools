"""Opt-in real GDAL/OGR acceptance with entirely disposable synthetic files.

Never launch GIS executables in normal CI. Set RYAN_CF_GDAL_ACCEPTANCE=1 in
the intended Windows OSGeo4W/QGIS command environment to run these checks.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RYAN_CF_GDAL_ACCEPTANCE") != "1",
    reason="Real GDAL acceptance is opt-in: set RYAN_CF_GDAL_ACCEPTANCE=1.",
)


def _program(name: str) -> str:
    binary_folder = os.environ.get("RYAN_CF_GDAL_BIN_DIR")
    if binary_folder:
        filename = name + (".exe" if os.name == "nt" else "")
        candidate = Path(binary_folder) / filename
        if not candidate.is_file():
            pytest.fail(f"Configured GDAL binary missing: {candidate}")
        return str(candidate.absolute())
    found = shutil.which(name)
    if found is None:
        pytest.fail(f"GDAL executable {name!r} not found on PATH; check QGIS/OSGeo4W shell.")
    return found


def _run(program: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([_program(program), *args], text=True, capture_output=True, check=False, timeout=60)


def test_real_gdal_ascii_to_geotiff_and_failing_input_preservation(tmp_path: Path) -> None:
    folder = tmp_path / "DEM 測試 with spaces"
    folder.mkdir()
    source = folder / "input elevation_é.asc"
    source.write_text(
        "ncols 3\nnrows 2\nxllcorner 500000\nyllcorner 7500000\ncellsize 10\nNODATA_value -9999\n1 2 3\n4 -9999 6\n",
        encoding="ascii",
    )
    before = source.read_bytes()
    output = folder / "output elevation.tif"

    version = _run("gdalinfo", "--version")
    assert version.returncode == 0, f"gdalinfo missing or misconfigured: {version.stderr}"
    result = _run("gdal_translate", "-of", "GTiff", str(source), str(output))
    assert result.returncode == 0, f"GDAL translation failed: {result.stdout} {result.stderr}"
    assert output.is_file()
    info = _run("gdalinfo", "-json", str(output))
    assert info.returncode == 0, f"gdalinfo failed: {info.stderr}"
    metadata = json.loads(info.stdout)
    assert metadata["size"] == [3, 2]
    assert metadata["bands"][0]["noDataValue"] == -9999
    assert source.read_bytes() == before

    nonexistent = folder / "missing elevation.asc"
    failed_output = folder / "missing-output.tif"
    failed = _run("gdal_translate", "-of", "GTiff", str(nonexistent), str(failed_output))
    assert failed.returncode != 0
    assert not failed_output.exists()
    assert source.read_bytes() == before


def test_real_ogr_clip_and_missing_source_preserves_inputs(tmp_path: Path) -> None:
    folder = tmp_path / "vectors 日本語 with spaces"
    folder.mkdir()
    source = folder / "sources.geojson"
    clipper = folder / "extent.geojson"
    source.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    },
                    {
                        "type": "Feature",
                        "properties": {"id": 2},
                        "geometry": {"type": "Point", "coordinates": [5, 5]},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    clipper.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    before = (source.read_bytes(), clipper.read_bytes())
    output = folder / "clipped output.gpkg"
    version = _run("ogrinfo", "--version")
    assert version.returncode == 0, f"ogrinfo unavailable: {version.stderr}"
    result = _run("ogr2ogr", "-f", "GPKG", str(output), str(source), "-clipsrc", str(clipper))
    assert result.returncode == 0, f"ogr2ogr failed: {result.stdout} {result.stderr}"
    assert output.is_file()
    output_info = _run("ogrinfo", "-so", "-al", str(output))
    assert output_info.returncode == 0, f"ogrinfo failed: {output_info.stderr}"
    assert "Feature Count: 1" in output_info.stdout
    assert (source.read_bytes(), clipper.read_bytes()) == before

    failed_output = folder / "failed output.gpkg"
    failed = _run("ogr2ogr", "-f", "GPKG", str(failed_output), str(folder / "absent.geojson"))
    assert failed.returncode != 0
    assert not failed_output.exists()
    assert (source.read_bytes(), clipper.read_bytes()) == before
