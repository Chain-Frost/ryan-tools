"""Standalone preflight wrapper defaults remain compatible with released PyTuflow."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest


class _Report:
    def __init__(self) -> None:
        self.run_count = 1
        self.checked_paths: set[str] = set()
        self.directories_scanned = 0
        self.individually_checked = 0
        self.issues: list[object] = []
        self.affected_runs = 0
        self.parse_seconds = 0.0
        self.context_seconds = 0.0
        self.inventory_seconds = 0.0
        self.csv_headers_checked = 0
        self.csv_column_selections_checked = 0
        self.dependency_fragment_evaluations = 0
        self.dependency_fragment_reuses = 0
        self.dependency_fallback_runs = 0
        self.passed = True

    def write_csv(self, path: Path) -> Path:
        return path


def test_standalone_uses_established_checker_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The published dependency must work without the unreleased fragment API."""
    script = Path(__file__).resolve().parents[2] / "ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py"
    spec = importlib.util.spec_from_file_location("_test_tuflow_input_wrapper", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    calls: list[str] = []

    def established(*_args: object, **_kwargs: object) -> _Report:
        calls.append("individual")
        return _Report()

    def fragment(*_args: object, **_kwargs: object) -> _Report:
        calls.append("fragment")
        return _Report()

    def read_run_commands(_path: Path) -> list[dict[str, str]]:
        return [{"s1": "DEV"}]

    monkeypatch.setattr(module, "read_run_commands", read_run_commands)
    monkeypatch.setattr(module, "check_simulation_inputs", established)
    monkeypatch.setattr(module, "check_batch_files", fragment)

    assert module.DEFAULT_FILES_ONLY is False
    options: dict[str, Any] = {
        "tcf": tmp_path / "model.tcf",
        "commands": tmp_path / "runs.txt",
        "report": tmp_path / "report.csv",
        "check_gpkg_layers": True,
    }
    assert module.main(**options) == 0
    assert calls == ["individual"]

    assert module.main(**options, files_only=True) == 0
    assert calls == ["individual", "fragment"]
