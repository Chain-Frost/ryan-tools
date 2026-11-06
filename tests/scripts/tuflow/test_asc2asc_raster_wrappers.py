"""Dry-run the ASC-to-ASC wrappers against the synthetic TUFLOW layouts."""

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPTS_DIRECTORY = Path(__file__).parents[3] / "ryan-scripts" / "TUFLOW-python" / "raster_processing"


def _load_script(filename: str) -> ModuleType:
    script = SCRIPTS_DIRECTORY / filename
    module_name = f"{script.stem}_wrapper_test"
    spec = importlib.util.spec_from_file_location(module_name, script)
    if spec is None or spec.loader is None:
        msg = f"Unable to load {script}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("statistic", ["mean", "median"])
@pytest.mark.parametrize(
    ("flags", "expected"),
    [((), True), (("--source",), True), (("--no-source",), False)],
)
def test_source_output_default_and_cli_override(
    statistic: str,
    flags: tuple[str, ...],
    expected: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script(f"run_asc_to_asc_{statistic}_then_maximum.py")
    captured: dict[str, object] = {}

    def workflow(**kwargs: object) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(module, "run_stat_then_max_workflow", workflow)
    monkeypatch.setattr(sys, "argv", ["wrapper", *flags, "--no-pause"])
    monkeypatch.chdir(tmp_path)
    options = module._parse_cli_arguments()
    assert module.WRITE_SOURCE_RASTERS is True
    assert module.main(working_directory=tmp_path, write_source=options.write_source) == 0
    assert captured["write_source"] is expected


@pytest.mark.parametrize("statistic", ["mean", "median"])
def test_source_cli_can_override_edited_false_default(
    statistic: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script(f"run_asc_to_asc_{statistic}_then_maximum.py")
    captured: dict[str, object] = {}

    def workflow(**kwargs: object) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(module, "run_stat_then_max_workflow", workflow)
    monkeypatch.setattr(module, "WRITE_SOURCE_RASTERS", False)
    monkeypatch.chdir(tmp_path)
    for flags, expected in [((), False), (("--source",), True)]:
        monkeypatch.setattr(sys, "argv", ["wrapper", *flags, "--no-pause"])
        options = module._parse_cli_arguments()
        assert module.main(working_directory=tmp_path, write_source=options.write_source) == 0
        assert captured["write_source"] is expected


def test_max_search_wrapper_dry_run_uses_configured_fixture_layout(
    raster_test_data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "max_search"
    module = _load_script("run_asc_to_asc_maximum_searches.py")
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
    module = _load_script("run_asc_to_asc_mean_then_maximum.py")
    monkeypatch.chdir(search_root)

    assert module.MEAN_VALUE_METHOD == "asc_to_asc"

    result = module.main(
        working_directory=search_root,
        workers=1,
        dry_run=True,
        strict=True,
        write_source=True,
        use_live_dashboard=False,
    )

    assert result == 0


def test_median_then_max_wrapper_dry_run_supports_nested_plus_names(
    raster_test_data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "mean_then_max"
    module = _load_script("run_asc_to_asc_median_then_maximum.py")
    monkeypatch.chdir(search_root)

    result = module.main(
        working_directory=search_root,
        workers=1,
        dry_run=True,
        strict=True,
        write_source=True,
        use_live_dashboard=False,
    )

    assert result == 0
