from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from batch_vector_clip import build_output_jobs, clip_vector


def test_output_jobs_reject_duplicate_stems(tmp_path: Path) -> None:
    first = tmp_path / "one" / "input.shp"
    second = tmp_path / "two" / "input.shp"
    extent = tmp_path / "extent.shp"

    with pytest.raises(ValueError, match="duplicate"):
        build_output_jobs([first, second], [extent], tmp_path / "outputs")


def test_clip_vector_promotes_temporary_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    input_path = tmp_path / "input.gpkg"
    extent_path = tmp_path / "extent.gpkg"
    output_path = tmp_path / "output.gpkg"

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        Path(command[1]).write_text("completed", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert clip_vector(
        input_path,
        extent_path,
        output_path,
        executable="ogr2ogr",
        overwrite=False,
        dry_run=False,
    )
    assert output_path.read_text(encoding="utf-8") == "completed"
    assert not list(tmp_path.glob(".*.tmp.gpkg"))
