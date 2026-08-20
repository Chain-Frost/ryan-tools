from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gdal_raster_to_xyz import convert_raster, discover_rasters


def test_discover_rasters_is_deterministic_and_deduplicated(tmp_path: Path) -> None:
    (tmp_path / "b.tif").touch()
    (tmp_path / "a.tif").touch()

    result = discover_rasters(tmp_path, ["*.tif", "a.*"], recursive=False)

    assert [path.name for path in result] == ["a.tif", "b.tif"]


def test_failed_conversion_removes_partial_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    input_path = tmp_path / "input.tif"
    output_path = tmp_path / "input.csv"

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        Path(command[-1]).write_text("partial", encoding="utf-8")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert not convert_raster(
        input_path,
        output_path,
        csv_output=True,
        skip_nodata=True,
        overwrite=False,
        dry_run=False,
    )
    assert not output_path.exists()
    assert not list(tmp_path.glob(".*.tmp.csv"))
