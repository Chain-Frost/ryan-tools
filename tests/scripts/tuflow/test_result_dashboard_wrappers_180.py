"""Focused editable/CLI propagation and path-base checks for six result wrappers."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import cast
from unittest.mock import Mock

import pytest

from ryan_library.functions.wrapper_utils import parse_common_cli_arguments

ROOT = Path(__file__).absolute().parents[3] / "ryan-scripts" / "TUFLOW-python"
WRAPPERS = (
    ("culvert_results/combine_culvert_maximums.py", "main_processing", "merge"),
    ("culvert_results/combine_culvert_timeseries.py", "main_processing", "merge"),
    ("culvert_results/create_mean_culvert_peak_report.py", "run_culvert_mean_report", "mean"),
    ("culvert_results/create_median_culvert_peak_report.py", "run_culvert_median_report", "mean"),
    ("po_and_timeseries/combine_po_results.py", "main_processing", "po"),
    ("po_and_timeseries/combine_pomm_results.py", "main_processing", "po"),
)


def _load_wrapper(path: str) -> ModuleType:
    source = ROOT / path
    spec = importlib.util.spec_from_file_location("dashboard_wrapper_" + source.stem, source)
    if spec is None or spec.loader is None:
        msg = f"Cannot import wrapper {source}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(("path", "entry", "kind"), WRAPPERS)
def test_result_wrapper_passes_effective_options_and_paths_without_chdir_leak(
    path: str, entry: str, kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_wrapper(path)
    job = tmp_path / "model"
    monkeypatch.chdir(tmp_path)

    def change_directory(*, target_dir: Path) -> bool:
        assert target_dir == job.absolute()
        return True

    monkeypatch.setattr(module, "change_working_directory", change_directory)
    worker = Mock(return_value=True)
    monkeypatch.setattr(module, entry, worker)
    main = cast("Callable[..., int]", module.main)
    extras: dict[str, object] = {}
    if kind == "merge":
        extras["output_directory"] = Path("exports")
        extras["export_mode"] = "both"
    elif kind == "mean":
        extras["export_raw"] = True
    else:
        extras["export_mode"] = "parquet"
    assert main(
        working_directory=job,
        paths_to_process=(Path("inputs"),),
        locations_to_include=("Loc1",),
        include_data_types=("PO",),
        use_live_dashboard=False,
        live_refresh_per_second=0.5,
        live_max_rows=2,
        console_log_level="DEBUG",
        log_file=Path("logging/detail.log"),
        file_log_level="TRACE",
        **extras,
    ) == 0
    kwargs = worker.call_args.kwargs
    assert kwargs["paths_to_process"] == [job / "inputs"]
    assert kwargs["locations_to_include"] == ("Loc1",)
    assert kwargs["use_live_dashboard"] is False
    assert kwargs["live_refresh_per_second"] == 0.5
    assert kwargs["live_max_rows"] == 2
    assert kwargs["log_file"] == str(job / "logging" / "detail.log")
    assert kwargs["file_log_level"] == "TRACE"
    if kind == "merge":
        assert kwargs["output_dir"] == job / "exports"
        assert kwargs["export_mode"] == "both"
    elif kind == "mean":
        assert kwargs["script_directory"] == job
        assert kwargs["export_raw"] is True
    else:
        assert kwargs["export_mode"] == "parquet"

    worker.return_value = False
    assert main(working_directory=job, use_live_dashboard=False) == 1
    with pytest.raises(ValueError, match="live_refresh_per_second"):
        main(working_directory=job, live_refresh_per_second=0)


@pytest.mark.parametrize(("path", "entry", "kind"), WRAPPERS)
@pytest.mark.parametrize(("flag", "expected"), [("--pause", True), ("--no-pause", False)])
def test_result_wrapper_pause_is_bidirectional(
    path: str, entry: str, kind: str, flag: str, expected: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    del entry, kind
    module = _load_wrapper(path)
    monkeypatch.setattr(sys, "argv", ["wrapper.py", flag])
    parse = cast("Callable[[], object]", module._parse_cli_arguments)
    options = parse_common_cli_arguments(cast("argparse.Namespace", parse()))
    assert options.pause_on_finish is expected
