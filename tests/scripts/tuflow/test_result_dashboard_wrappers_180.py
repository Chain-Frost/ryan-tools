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
    path: str,
    entry: str,
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    assert (
        main(
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
        )
        == 0
    )
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
    worker.side_effect = OSError("synthetic exporter failure")
    assert main(working_directory=job, use_live_dashboard=False) == 1
    worker.side_effect = None
    worker.reset_mock()
    assert main(working_directory=job, live_refresh_per_second=0) == 1
    worker.assert_not_called()


@pytest.mark.parametrize(("path", "entry", "kind"), WRAPPERS)
def test_result_wrapper_defaults_to_pause_with_only_no_pause_override(
    path: str,
    entry: str,
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del entry, kind
    module = _load_wrapper(path)
    parse = cast("Callable[[], argparse.Namespace]", module._parse_cli_arguments)
    assert module.PAUSE_ON_FINISH is True

    # The CLI does not override the editable pause default when omitted.
    monkeypatch.setattr(sys, "argv", ["wrapper.py"])
    default_options = parse_common_cli_arguments(parse())
    assert default_options.pause_on_finish is None
    assert default_options.no_pause is False

    # The only pause-related CLI option disables the ordinary interactive wait.
    monkeypatch.setattr(sys, "argv", ["wrapper.py", "--no-pause"])
    no_pause_options = parse_common_cli_arguments(parse())
    assert no_pause_options.pause_on_finish is False
    assert no_pause_options.no_pause is True

    # A redundant positive --pause flag is not part of this wrapper contract.
    monkeypatch.setattr(sys, "argv", ["wrapper.py", "--pause"])
    with pytest.raises(SystemExit) as error:
        parse()
    assert error.value.code == 2


@pytest.mark.parametrize(("path", "entry", "kind"), WRAPPERS)
def test_result_wrapper_expands_home_before_working_directory(
    path: str,
    entry: str,
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_wrapper(path)
    home = tmp_path / "home"
    job = tmp_path / "job"
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(module, "change_working_directory", Mock(return_value=True))
    worker = Mock(return_value=True)
    monkeypatch.setattr(module, entry, worker)
    main = cast("Callable[..., int]", module.main)
    extras = {"output_directory": Path("~/exports")} if kind == "merge" else {}
    assert (
        main(working_directory=job, paths_to_process=(Path("~/inputs"),), log_file=Path("~/detail.log"), **extras) == 0
    )
    assert worker.call_args.kwargs["paths_to_process"] == [home / "inputs"]
    assert worker.call_args.kwargs["log_file"] == str(home / "detail.log")
    if kind == "merge":
        assert worker.call_args.kwargs["output_dir"] == home / "exports"


@pytest.mark.parametrize(("path", "entry", "kind"), WRAPPERS)
def test_result_wrapper_configuration_error_does_not_change_directory(
    path: str,
    entry: str,
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del kind
    module = _load_wrapper(path)
    change_directory = Mock(return_value=True)
    worker = Mock(return_value=True)
    monkeypatch.setattr(module, "change_working_directory", change_directory)
    monkeypatch.setattr(module, entry, worker)
    main = cast("Callable[..., int]", module.main)
    assert main(working_directory=tmp_path, live_max_rows=0) == 1
    change_directory.assert_not_called()
    worker.assert_not_called()


@pytest.mark.parametrize(
    ("path", "entry", "paths_key"),
    [
        ("po_and_timeseries/check_po_timeseries_peaks.py", "main_processing", "paths_to_process"),
        ("po_and_timeseries/check_timeseries_stability.py", "main_processing", "paths_to_process"),
        ("po_and_timeseries/calculate_po_closure_durations.py", "run_closure_durations", "paths"),
    ],
)
def test_report_relative_working_directory_is_not_applied_twice(
    path: str,
    entry: str,
    paths_key: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_wrapper(path)
    job = tmp_path / "job"
    job.mkdir()
    monkeypatch.chdir(tmp_path)
    worker = Mock(return_value=None)
    monkeypatch.setattr(module, entry, worker)
    main = cast("Callable[..., int]", module.main)
    assert main(working_directory=Path("job")) == 0
    assert worker.call_args.kwargs[paths_key] == [job]
    monkeypatch.chdir(tmp_path)
    assert main(working_directory=Path("job"), paths_to_process=(Path("results"),)) == 0
    assert worker.call_args.kwargs[paths_key] == [job / "results"]


def test_log_summary_expands_home_for_working_directory_and_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_wrapper("log_processing/create_log_summary_report.py")
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    change_directory = Mock(return_value=True)
    worker = Mock(return_value=True)
    monkeypatch.setattr(module, "change_working_directory", change_directory)
    monkeypatch.setattr(module, "main_processing", worker)
    main = cast("Callable[..., int]", module.main)
    assert main(working_directory=Path("~/job"), log_file=Path("~/detail.log")) == 0
    change_directory.assert_called_once_with(target_dir=tmp_path / "job")
    assert worker.call_args.kwargs["log_file"] == str(tmp_path / "detail.log")
