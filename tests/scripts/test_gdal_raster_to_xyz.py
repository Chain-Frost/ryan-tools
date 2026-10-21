"""Nondestructive source-wrapper regression tests for staged GDAL XYZ exports."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest

ROOT = Path(__file__).absolute().parents[2]


def _load_wrapper() -> ModuleType:
    source = ROOT / "ryan-scripts" / "gdal-python" / "gdal_raster_to_xyz.py"
    spec = importlib.util.spec_from_file_location("gdal_raster_to_xyz_audit", source)
    if spec is None or spec.loader is None:
        msg = f"Cannot import {source}"
        raise RuntimeError(msg)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    return wrapper


wrapper = _load_wrapper()


def test_staged_overwrite_preserves_old_output_on_promotion_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Failure after conversion must never delete the pre-existing destination."""
    source = tmp_path / "terrain.tif"
    source.write_bytes(b"source")
    output = tmp_path / "terrain.csv"
    output.write_bytes(b"old-output")
    staged = tmp_path / ".terrain.test.tmp.csv"

    def fake_run(
        arguments: list[str],
        *,
        check: bool,
        text: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert arguments[-1] == str(staged)
        assert not check and text and capture_output
        staged.write_bytes(b"new-output")
        return subprocess.CompletedProcess(args=arguments, returncode=0, stdout="", stderr="")

    def fail_promotion(self: Path, destination: str | Path) -> Path:
        assert self == staged
        assert destination == output
        raise OSError("injected failed promotion")

    monkeypatch.setattr(wrapper, "_temporary_output", lambda _: staged)
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(Path, "replace", fail_promotion)

    assert not cast("bool", wrapper.convert_raster(source, output, csv_output=True, skip_nodata=True, overwrite=True, dry_run=False))
    assert source.read_bytes() == b"source"
    assert output.read_bytes() == b"old-output"
    assert not staged.exists()


def test_staged_overwrite_promotes_successful_conversion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Once staging succeeds, the new result atomically replaces the old bytes."""
    source = tmp_path / "terrain.tif"
    source.write_bytes(b"source")
    output = tmp_path / "terrain.csv"
    output.write_bytes(b"old-output")
    staged = tmp_path / ".terrain.test.tmp.csv"

    def fake_run(
        arguments: list[str],
        *,
        check: bool,
        text: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert arguments[-1] == str(staged)
        assert not check and text and capture_output
        staged.write_bytes(b"new-output")
        return subprocess.CompletedProcess(args=arguments, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(wrapper, "_temporary_output", lambda _: staged)
    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cast("bool", wrapper.convert_raster(source, output, csv_output=True, skip_nodata=True, overwrite=True, dry_run=False))
    assert source.read_bytes() == b"source"
    assert output.read_bytes() == b"new-output"
    assert not staged.exists()
