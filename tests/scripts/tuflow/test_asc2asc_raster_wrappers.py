"""Dry-run the ASC-to-ASC wrappers against the synthetic TUFLOW layouts."""

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest

SCRIPTS_DIRECTORY = Path(__file__).parents[3] / "ryan-scripts" / "TUFLOW-python"


def _load_script(filename: str) -> ModuleType:
    script = SCRIPTS_DIRECTORY / filename
    module_name = f"{script.stem}_wrapper_test"
    spec = importlib.util.spec_from_file_location(module_name, script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_max_search_wrapper_dry_run_uses_configured_fixture_layout(
    raster_test_data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "max_search"
    module = _load_script("asc2asc_max_by_search.py")
    monkeypatch.setattr(module, "ASC_TO_ASC_EXE", Path(sys.executable))
    monkeypatch.chdir(search_root)

    result = module.main(
        working_directory=search_root,
        workers=1,
        dry_run=True,
        use_live_dashboard=False,
    )

    assert result == 0


def test_mean_then_max_wrapper_dry_run_supports_nested_plus_names(
    raster_test_data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "mean_then_max"
    module = _load_script("asc2asc_mean_then_max_by_search.py")
    monkeypatch.setattr(module, "ASC_TO_ASC_EXE", Path(sys.executable))
    monkeypatch.chdir(search_root)

    result = module.main(
        working_directory=search_root,
        workers=1,
        dry_run=True,
        strict=True,
        use_live_dashboard=False,
    )

    assert result == 0
