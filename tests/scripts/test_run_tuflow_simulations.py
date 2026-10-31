"""Maintained wrapper contract and process-boundary acceptance for scope 03."""

from __future__ import annotations

import argparse
import importlib.util
import os
import runpy
import signal
import subprocess
import sys
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Protocol, cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.simulation_runner import BatchOutcome, RunnerSettings, RunOutcome


class _RunnerWrapper(Protocol):
    __file__: str
    EXPORT_COMMANDS: bool
    NEXT_RUN_FILE: Path | None
    OPERATION: str

    def main(self, argv: list[str] | None = None) -> int: ...


@pytest.fixture
def wrapper(tmp_path: Path) -> _RunnerWrapper:
    source = (
        Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py"
    )
    # The real wrapper owns an adjacent CSV. A disposable copy prevents tests
    # from writing history into the tracked repository wrapper directory.
    copied = tmp_path / "run_tuflow_simulations.py"
    copied.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("tested_runner_wrapper", copied)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    # A harmless file satisfies the wrapper's pre-launch existence check;
    # fake-process callbacks still prevent any solver execution.
    solver = tmp_path / "TUFLOW_Synthetic.exe"
    solver.write_bytes(b"synthetic solver placeholder")
    module.__dict__["TUFLOW_EXE"] = solver
    module.__dict__["OPERATION"] = "run"  # Other tests cover the safe shipped default.
    return cast(_RunnerWrapper, module)


def _source(path: Path) -> Path:
    batch = path / "models.bat"
    batch.write_text(
        f'START /WAIT "" "TUFLOW.exe" -b -e1 A "{path / "model.tcf"}"\n',
        encoding="utf-8",
    )
    return batch


def test_default_operation_is_export_only_and_cli_can_override(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    source = _source(tmp_path)
    monkeypatch.setattr(module, "OPERATION", "export_only")
    monkeypatch.setattr(module, "TUFLOW_EXE", tmp_path / "missing.exe")

    def forbid(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Export-only attempted to launch a solver")

    monkeypatch.setattr(module, "launch_plan", forbid)
    args = ["--mode", "textfiles", "--input-file", str(source), "--no-tcf", "--no-pause"]
    assert wrapper.main(args) == 0
    assert Path(wrapper.__file__).with_suffix(".txt").is_file()
    assert not Path(wrapper.__file__).with_name("run_tuflow_simulations_runs.csv").exists()

    # CLI launch intent overrides the safe file default, and the missing
    # solver then fails before writing a continuation CSV.
    assert wrapper.main([*args, "--run-simulations"]) == 1
    assert not Path(wrapper.__file__).with_name("run_tuflow_simulations_runs.csv").exists()


def test_editable_settings_and_cli_precedence(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = cast(_RunnerWrapper, sys.modules["tested_runner_wrapper"])
    monkeypatch.setattr(module, "USE_LIVE_DASHBOARD", True)
    monkeypatch.setattr(module, "LIVE_REFRESH_PER_SECOND", 1.0)
    monkeypatch.setattr(module, "WAIT_TIME_AFTER_RUN", 2.0)
    parse_arguments = cast(
        Callable[[list[str]], argparse.Namespace],
        getattr(module, "_arguments"),  # noqa: B009 - explicitly test the copied wrapper
    )
    build_settings = cast(
        Callable[[argparse.Namespace], RunnerSettings],
        getattr(module, "_settings"),  # noqa: B009 - explicitly test the copied wrapper
    )
    args = parse_arguments(
        [
            "--no-preflight",
            "--no-live-dashboard",
            "--live-refresh-per-second",
            "0.5",
            "--launch-delay",
            "0",
            "--working-directory",
            str(tmp_path),
            "--no-pause",
        ]
    )
    settings = build_settings(args)
    assert settings.use_live_dashboard is False
    assert settings.live_refresh_per_second == 0.5
    assert settings.wait_time_after_run == 0
    assert args.preflight is False
    with pytest.raises(SystemExit) as invalid:
        parse_arguments(["--pause", "--no-pause"])
    assert invalid.value.code == 2


def test_export_only_copied_workflow_needs_neither_solver_nor_gpu(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ryan_library.orchestrators.tuflow import simulation_runner

    source = _source(tmp_path)
    output = Path(wrapper.__file__).with_suffix(".txt")

    def forbidden_gpu(*_args: object) -> None:
        pytest.fail("Export-only inspected GPUs")

    monkeypatch.setattr(simulation_runner, "_gpu_startup_message", forbidden_gpu)
    assert (
        wrapper.main(
            [
                "--working-directory",
                str(tmp_path),
                "--mode",
                "textfiles",
                "--input-file",
                str(source),
                "--no-tcf",
                "--export-only",
                "--solver-exe",
                str(tmp_path / "TUFLOW_MISSING.EXE"),
                "--no-pause",
            ]
        )
        == 0
    )
    assert output.is_file()
    assert source.read_text(encoding="utf-8").startswith("START /WAIT")


def test_wrapper_preflight_only_waits_and_returns_failure(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    calls: list[str] = []
    module = sys.modules["tested_runner_wrapper"]

    def check(*_args: object, **_kwargs: object) -> bool:
        calls.append("preflight")
        return False

    monkeypatch.setattr(module, "check_plan_only", check)

    def forbidden_launch(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Launched during check-only")

    monkeypatch.setattr(module, "launch_plan", forbidden_launch)
    assert (
        wrapper.main(
            [
                "--working-directory",
                str(tmp_path),
                "--mode",
                "textfiles",
                "--input-file",
                str(source),
                "--no-tcf",
                "--preflight-only",
                "--no-export",
                "--no-pause",
            ]
        )
        == 1
    )
    assert calls == ["preflight"]


def test_failed_child_still_chains_but_keeps_failure_exit_code(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    calls: list[str] = []
    monkeypatch.setattr(module, "EXPORT_COMMANDS", False)
    monkeypatch.setattr(module, "NEXT_RUN_FILE", Path("post.py"))

    def fake_launch(plan: CommandPlan, *_args: object, **_kwargs: object) -> BatchOutcome:
        calls.append("launch")
        selected = plan.runs
        return BatchOutcome(
            selected,
            tuple(RunOutcome(run, 1, datetime.now(), datetime.now(), 123) for run in selected),
        )

    def post(*_args: object, **_kwargs: object) -> bool:
        calls.append("post")
        return True

    monkeypatch.setattr(module, "launch_plan", fake_launch)
    monkeypatch.setattr(module, "_run_follow_up", post)
    assert wrapper.main(["--working-directory", str(tmp_path), "--no-pause"]) == 1
    assert calls == ["launch", "post"]


def test_planning_failure_does_not_export_check_or_chain(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, "NEXT_RUN_FILE", Path("post.py"))

    def forbidden_follow_up(*_args: object) -> None:
        pytest.fail("Chained after preparation failure")

    def forbidden_launch(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Launched bad input")

    monkeypatch.setattr(module, "_run_follow_up", forbidden_follow_up)
    monkeypatch.setattr(module, "launch_plan", forbidden_launch)
    assert (
        wrapper.main(
            [
                "--working-directory",
                str(tmp_path),
                "--mode",
                "textfiles",
                "--input-file",
                str(tmp_path / "missing.bat"),
                "--no-tcf",
                "--no-pause",
            ]
        )
        == 1
    )
    assert not Path(wrapper.__file__).with_suffix(".txt").exists()


def test_process_signal_handler_is_restored(wrapper: _RunnerWrapper) -> None:
    previous = signal.getsignal(signal.SIGINT)
    interrupt_context = cast(
        Callable[[], AbstractContextManager[Event]],
        getattr(wrapper, "_interruption_flag"),  # noqa: B009 - copied module private hook
    )
    with interrupt_context():
        assert signal.getsignal(signal.SIGINT) is not previous
    assert signal.getsignal(signal.SIGINT) is previous


def test_optional_help_is_available_without_loading_pytuflow() -> None:
    source = (
        Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py"
    )
    content = source.read_text(encoding="utf-8")
    assert "sys.path.insert" not in content
    # Source-checkout subprocesses do not inherit pytest's sys.path; the copied
    # wrapper / installed-wheel acceptance runs separately with no PYTHONPATH.
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(value for value in (str(source.parents[3]), env.get("PYTHONPATH")) if value)
    result = subprocess.run(
        [sys.executable, str(source), "--help"], capture_output=True, text=True, check=False, env=env
    )
    assert result.returncode == 0
    assert "--export-only" in result.stdout
    assert "--preflight-only" in result.stdout
    assert "--no-pause" in result.stdout


def test_default_txt_is_refreshed_for_all_complete_resume_without_extra_logs(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    source = _source(tmp_path)
    wrapper_path = Path(wrapper.__file__)
    output = wrapper_path.with_suffix(".txt")
    history = wrapper_path.with_name(f"{wrapper_path.stem}_runs.csv")
    initial_files = set(tmp_path.iterdir())
    calls: list[list[int]] = []

    def complete(
        plan: CommandPlan,
        _settings: RunnerSettings,
        *,
        on_before_launch: Callable[[PlannedRun], None],
        on_observed_completion: Callable[[RunOutcome], None],
        **_options: object,
    ) -> BatchOutcome:
        calls.append([run.index for run in plan.runs])
        outcomes: list[RunOutcome] = []
        for run in plan.runs:
            on_before_launch(run)
            outcome = RunOutcome(run, 0, datetime.now(), datetime.now(), 123)
            on_observed_completion(outcome)
            outcomes.append(outcome)
        return BatchOutcome(plan.runs, tuple(outcomes))

    monkeypatch.setattr(module, "launch_plan", complete)
    arguments = ["--mode", "textfiles", "--input-file", str(source), "--no-tcf", "--no-pause"]
    assert wrapper.main(arguments) == 0
    first_export, first_history = output.read_bytes(), history.read_bytes()
    output.write_text("stale previous export\n", encoding="utf-8")
    assert wrapper.main(arguments) == 0
    assert output.read_bytes() == first_export
    assert history.read_bytes() == first_history
    assert calls == [[1]]
    assert set(tmp_path.iterdir()) == initial_files | {output, history}


def test_default_txt_cannot_replace_its_own_imported_source(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Path(wrapper.__file__).with_suffix(".txt")
    source.write_bytes(_source(tmp_path).read_bytes())
    before = source.read_bytes()
    module = sys.modules["tested_runner_wrapper"]

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Conflicting export launched a solver")

    monkeypatch.setattr(module, "launch_plan", forbidden)
    assert wrapper.main(["--mode", "textfiles", "--input-file", str(source), "--no-tcf", "--no-pause"]) == 1
    assert source.read_bytes() == before
    assert not tuple(tmp_path.glob("*_runs.csv"))


def test_script_entrypoint_parses_arguments_once(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    calls: list[int] = []
    original = argparse.ArgumentParser.parse_args

    def parse_once(
        parser: argparse.ArgumentParser, args: Sequence[str] | None = None, namespace: argparse.Namespace | None = None
    ) -> argparse.Namespace:
        calls.append(1)
        if namespace is None:
            return original(parser, args)
        return original(parser, args, namespace)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", parse_once)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            wrapper.__file__,
            "--mode",
            "textfiles",
            "--input-file",
            str(source),
            "--no-tcf",
            "--export-only",
            "--no-pause",
        ],
    )
    with pytest.raises(SystemExit) as ended:
        runpy.run_path(wrapper.__file__, run_name="__main__")
    assert ended.value.code == 0
    assert calls == [1]
