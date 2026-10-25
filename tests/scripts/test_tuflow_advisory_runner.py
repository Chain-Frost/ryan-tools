"""Deterministic advisory lifecycle and actual scheduler tests; no solver execution."""

from __future__ import annotations

import importlib.util
import logging
import subprocess
import sys
import types
from collections.abc import Callable
from pathlib import Path
from threading import Event
from typing import Protocol, cast

import psutil
import pytest

from ryan_library.functions.tuflow.gpu_occupancy import GpuOccupancySnapshot, TuflowGpuProcess
from ryan_library.orchestrators.tuflow.input_preflight import PreflightIssue, PreflightReport, RunRequest


class _Core(Protocol):
    tcf: Path
    preflight_check: bool
    preflight_context_mode: str
    preflight_report_csv: Path
    next_run_file: str | Path | None


class _Simulation(Protocol):
    index: int
    args_for_python: list[str]


class _Check(Protocol):
    requests: tuple[RunRequest, ...]
    finished: Event

    def start(self) -> None: ...
    def close(self) -> None: ...
    def drain(self) -> None: ...


class _Runner(Protocol):
    CoreParameters: Callable[..., _Core]
    Simulation: Callable[..., _Simulation]
    AdvisoryInputCheck: Callable[..., _Check]
    launch_simulations: Callable[..., None]
    main: Callable[[], None]


@pytest.fixture
def runner() -> _Runner:
    path = (
        Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py"
    )
    spec = importlib.util.spec_from_file_location("advisory_runner_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return cast(_Runner, module)


def setup(runner: _Runner, tmp_path: Path, **settings: object) -> tuple[_Core, list[_Simulation]]:
    core = runner.CoreParameters(
        tcf=tmp_path / "model.tcf",
        tuflowexe=tmp_path / "TUFLOW.exe",
        preflight_report_csv=tmp_path / "issues.csv",
        pause_on_finish=False,
        write_results_csv=False,
        capture_console_log=False,
        wait_time_after_run=0,
        minimize_on_launch=False,
        **settings,
    )
    sims = [
        runner.Simulation(
            index=i, args_for_python=["TUFLOW.exe", "-e1", str(i), "model.tcf"], command_for_batch="unused"
        )
        for i in (1, 2)
    ]
    return core, sims


@pytest.mark.parametrize("dashboard", [False, True])
def test_scheduler_launches_every_run_while_checker_is_held(
    runner: _Runner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    dashboard: bool,
) -> None:
    core, sims = setup(
        runner, tmp_path, use_live_dashboard=dashboard, gpu_devices=[["-pu0"]], preflight_context_mode="batch"
    )
    started, release = Event(), Event()
    captured: list[RunRequest] = []

    def check(_tcf: Path, requests: list[RunRequest], **_options: object) -> PreflightReport:
        captured.extend(requests)
        assert _options["context_mode"] == "batch"
        started.set()
        assert release.wait(5), "test did not release checker"
        return PreflightReport(len(requests))

    monkeypatch.setattr(runner, "check_simulation_inputs", check)

    def occupancy(**_options: object) -> GpuOccupancySnapshot:
        return GpuOccupancySnapshot((TuflowGpuProcess(99001, frozenset({0})),), (99002,), True)

    monkeypatch.setattr(runner, "local_gpu_occupancy", occupancy)
    launched: list[list[str]] = []

    class Process:
        pid = 99003
        returncode = 0

        def poll(self) -> int:
            return 0

    def popen(*, args: list[str], **_options: object) -> Process:
        assert started.is_set()
        assert not release.is_set()
        launched.append(args.copy())
        return Process()

    class Priority:
        def nice(self, *, value: int) -> None:
            del value

    monkeypatch.setattr(subprocess, "Popen", popen)

    def priority(**_options: object) -> Priority:
        return Priority()

    monkeypatch.setattr(psutil, "Process", priority)
    advisory = runner.AdvisoryInputCheck(sims, core)
    caplog.set_level(logging.INFO)
    advisory.start()
    try:
        assert started.wait(2)
        runner.launch_simulations(sims=sims, core=core, advisory=advisory)
        assert len(launched) == 2
        assert all("-pu0" in args for args in launched)
        assert [request.flags for request in captured] == [{"e1": "1"}, {"e1": "2"}]
        with pytest.raises(TypeError):
            captured[0].flags["e1"] = "mutated"  # pyright: ignore[reportIndexIssue] - immutable snapshot regression
    finally:
        release.set()
        assert advisory.finished.wait(2)
        advisory.close()
    assert caplog.text.count("All required TUFLOW input files are present") == 1
    output = capsys.readouterr().out
    assert "will be shared" in output
    assert "unknown" in output
    assert "Could not inspect every local process" in output


@pytest.mark.parametrize("failure", ["missing", "unsupported", "report", "partial"])
def test_all_findings_and_failures_remain_truthful(
    runner: _Runner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    core, sims = setup(runner, tmp_path)

    def check(_tcf: Path, _requests: list[RunRequest], **_options: object) -> PreflightReport:
        if failure == "unsupported":
            message = "Batch API unavailable"
            raise ValueError(message)
        report = PreflightReport(1 if failure == "partial" else 2)
        if failure in {"missing", "report"}:
            report.issues = [
                PreflightIssue(1, "-e1 1", "missing", "Read GIS", f"input-{i}", "absent") for i in range(25)
            ]
        return report

    if failure == "report":
        core.preflight_report_csv.mkdir()
    monkeypatch.setattr(runner, "check_simulation_inputs", check)
    caplog.set_level(logging.INFO)
    advisory = runner.AdvisoryInputCheck(sims, core)
    advisory.start()
    assert advisory.finished.wait(2)
    advisory.close()
    assert "All required TUFLOW input files are present" not in caplog.text
    if failure in {"missing", "report"}:
        assert sum("Input check run" in record.message for record in caplog.records) == 25
    if failure == "report":
        assert "report write failed" in caplog.text
    if failure == "unsupported":
        assert "Batch API unavailable" in caplog.text


def test_shutdown_does_not_wait_for_slow_io_or_emit_late_all_clear(
    runner: _Runner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    core, sims = setup(runner, tmp_path)
    started, release = Event(), Event()

    def check(_tcf: Path, _requests: list[RunRequest], **_options: object) -> PreflightReport:
        started.set()
        assert release.wait(5)
        return PreflightReport(2)

    monkeypatch.setattr(runner, "check_simulation_inputs", check)
    advisory = runner.AdvisoryInputCheck(sims, core)
    advisory.start()
    try:
        assert started.wait(2)
        advisory.close()
        assert not advisory.finished.is_set()
        assert "incomplete at runner shutdown" in caplog.text
    finally:
        release.set()
        assert advisory.finished.wait(2)
    advisory.drain()
    assert "All required TUFLOW input files are present" not in caplog.text
    assert not core.preflight_report_csv.exists()


@pytest.mark.parametrize("path", [Path(r"T:\model folder\model.tcf"), Path(r"\\server\share\model folder\model.tcf")])
def test_worker_keeps_user_path_spelling_without_resolving_links(
    runner: _Runner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
) -> None:
    core, sims = setup(runner, tmp_path)
    core.tcf = path
    observed: list[Path] = []

    def check(tcf: Path, requests: list[RunRequest], **_options: object) -> PreflightReport:
        observed.append(tcf)
        return PreflightReport(len(requests))

    monkeypatch.setattr(runner, "check_simulation_inputs", check)
    advisory = runner.AdvisoryInputCheck(sims, core)
    advisory.start()
    assert advisory.finished.wait(2)
    advisory.close()
    assert observed == [path.absolute()]


@pytest.mark.parametrize("ending", ["normal", "interrupt", "disabled", "check-only", "failed-check-only"])
def test_main_owns_worker_shutdown_and_check_only_policy(  # noqa: C901 - explicit lifecycle acceptance matrix
    runner: _Runner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    ending: str,
) -> None:
    core, sims = setup(runner, tmp_path, preflight_check=ending != "disabled")
    started, release = Event(), Event()
    calls: list[str] = []
    workers: list[_Check] = []
    core.next_run_file = tmp_path / "next.py"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["runner", "--no-pause"] + (["--preflight-only"] if "check-only" in ending else [])
    )

    def parameters() -> types.SimpleNamespace:
        return types.SimpleNamespace(core_params=core, run_variables={})

    def simulations(**_options: object) -> list[_Simulation]:
        return sims

    def ignore(*_args: object, **_options: object) -> None:
        return

    def check(_tcf: Path, requests: list[RunRequest], **_options: object) -> PreflightReport:
        calls.append("check")
        started.set()
        if "check-only" not in ending:
            assert release.wait(5)
        result = PreflightReport(len(requests))
        if ending == "failed-check-only":
            result.issues.append(PreflightIssue(1, "-e1 1", "missing", "test", "absent", "absent"))
        return result

    def scheduler(**options: object) -> None:
        assert "check-only" not in ending
        calls.append("launch")
        advisory = options["advisory"]
        if ending == "disabled":
            assert advisory is None
        else:
            workers.append(cast(_Check, advisory))
            assert started.wait(2)
            assert not release.is_set()
        if ending == "interrupt":
            raise KeyboardInterrupt

    def post_script(**_options: object) -> None:
        assert not release.is_set()
        calls.append("post")

    monkeypatch.setattr(runner, "run_post_script", post_script)
    monkeypatch.setattr(runner, "get_parameters", parameters)
    monkeypatch.setattr(runner, "compute_simulations", simulations)
    for name in (
        "configure_console_ansi",
        "print_wrapper_banner",
        "dump_run_variables",
        "dump_simulations_preview",
        "export_commands",
    ):
        monkeypatch.setattr(runner, name, ignore)
    monkeypatch.setattr(runner, "check_simulation_inputs", check)
    monkeypatch.setattr(runner, "launch_simulations", scheduler)
    try:
        if ending == "interrupt":
            with pytest.raises(KeyboardInterrupt):
                runner.main()
        elif ending == "failed-check-only":
            with pytest.raises(RuntimeError, match="diagnostic failed"):
                runner.main()
        else:
            runner.main()
        if ending in {"normal", "interrupt"}:
            assert "incomplete at runner shutdown" in caplog.text
        if "check-only" in ending:
            assert calls == ["check"]
        elif ending == "disabled":
            assert calls == ["launch", "post"]
        else:
            assert sorted(calls) == (["check", "launch"] if ending == "interrupt" else ["check", "launch", "post"])
    finally:
        release.set()
        for worker in workers:
            assert worker.finished.wait(2)
