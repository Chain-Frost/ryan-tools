"""Standalone preflight wrapper defaults remain compatible with released PyTuflow."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest


class _Report:
    run_count = 1
    checked_paths: set[str] = set()
    directories_scanned = 0
    individually_checked = 0
    issues: list[object] = []
    affected_runs = 0
    parse_seconds = 0.0
    context_seconds = 0.0
    inventory_seconds = 0.0
    csv_headers_checked = 0
    csv_column_selections_checked = 0
    dependency_fragment_evaluations = 0
    dependency_fragment_reuses = 0
    dependency_fallback_runs = 0
    passed = True

    def write_csv(self, path: Path) -> Path:
        return path


def test_standalone_uses_established_checker_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The published dependency must work without the unreleased fragment API."""
    script = (
        Path(__file__).resolve().parents[2]
        / "ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py"
    )
    spec = importlib.util.spec_from_file_location("_test_tuflow_input_wrapper", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    calls: list[str] = []

    def established(*_args: object, **_kwargs: object) -> _Report:
        calls.append("individual")
        return _Report()

    def fragment(*_args: object, **_kwargs: object) -> _Report:
        calls.append("fragment")
        return _Report()

    monkeypatch.setattr(module, "read_run_commands", lambda _path: [{"s1": "DEV"}])
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
