"""Advisory check and one-time external GPU snapshot acceptance (fake processes)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from dataclasses import replace
from pathlib import Path
from subprocess import Popen
from threading import Event
from typing import cast

import pytest
from loguru import logger

from ryan_library.functions.tuflow.command_plan import CommandPlan
from ryan_library.functions.tuflow.gpu_occupancy import GpuOccupancySnapshot, TuflowGpuProcess
from ryan_library.orchestrators.tuflow import command_preflight
from ryan_library.orchestrators.tuflow import simulation_runner as runner
from ryan_library.orchestrators.tuflow.command_preflight import CommandPreflightIssue, CommandPreflightReport
from ryan_library.orchestrators.tuflow.input_preflight import PreflightIssue


@pytest.fixture(autouse=True)
def capture_loguru(caplog: pytest.LogCaptureFixture) -> Generator[None]:
    sink = logger.add(caplog.handler, format="{message}")
    try:
        yield
    finally:
        logger.remove(sink)


def settings(base: Path, **changes: object) -> runner.RunnerSettings:
    initial = runner.RunnerSettings(
        wrapper_file=base / "runner.py",
        working_directory=base,
        executable=base / "TUFLOW.exe",
        tcf=Path("runs/model_~e1~.tcf"),
        run_variables={"e1": ("A", "B")},
        batch_commands="-b",
        wait_time_after_run=0,
        use_live_dashboard=False,
    )
    return replace(initial, **changes)


class FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode = 0

    def poll(self) -> int:
        return 0


def _popen_stub(factory: Callable[..., FakeProcess]) -> Callable[..., Popen[bytes]]:
    """Type the deliberate fake-process substitution at the test boundary."""
    return cast(Callable[..., Popen[bytes]], factory)


def _no_op(*_args: object) -> None:
    """Disable priority and snapshot helpers where they are not under test."""


def _complete_report(plan: CommandPlan) -> CommandPreflightReport:
    result = CommandPreflightReport(plan)
    result.completed_runs = len(result.plan.runs)
    return result


@pytest.mark.parametrize("dashboard", [False, True])
def test_launches_during_held_advisory_and_delivers_all_findings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dashboard: bool,
) -> None:
    config = settings(tmp_path, use_live_dashboard=dashboard)
    plan = runner.prepare_plan(config)
    started, release = Event(), Event()
    launched: list[int] = []

    def check(_plan: object, **_kwargs: object) -> CommandPreflightReport:
        assert _kwargs.get("io_workers") == 1
        started.set()
        assert release.wait(5)
        result = _complete_report(plan)
        result.issues = [
            CommandPreflightIssue(plan.runs[0], PreflightIssue(1, "", "missing", "test", f"missing-{i}", "absent"))
            for i in range(25)
        ]
        return result

    monkeypatch.setattr(command_preflight, "check_command_plan", check)
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    caplog.set_level(logging.INFO)
    advisory = runner.AdvisoryInputCheck(plan)
    advisory.start()
    try:
        assert started.wait(2)

        def popen(**_kwargs: object) -> FakeProcess:
            assert not release.is_set()
            launched.append(1)
            return FakeProcess(len(launched))

        outcome = runner.launch_plan(plan, config, advisory=advisory, popen=_popen_stub(popen))
        assert outcome.successful
        assert len(launched) == 2
        release.set()
        assert advisory.finished.wait(3)
        advisory.drain()
        assert sum("Input check run" in record.message for record in caplog.records) == 25
        assert "passed for" not in caplog.text
    finally:
        release.set()
        advisory.close()


def test_shutdown_never_waits_for_slow_io_or_produces_late_all_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    plan = runner.prepare_plan(settings(tmp_path))
    started, release = Event(), Event()

    def check(_plan: object, **_kwargs: object) -> CommandPreflightReport:
        started.set()
        assert release.wait(5)
        return _complete_report(plan)

    monkeypatch.setattr(command_preflight, "check_command_plan", check)
    advisory = runner.AdvisoryInputCheck(plan)
    caplog.set_level(logging.INFO)
    advisory.start()
    try:
        assert started.wait(2)
        advisory.close()
        assert "incomplete at shutdown" in caplog.text
    finally:
        release.set()
        assert advisory.finished.wait(2)
    advisory.drain()
    assert "Input check passed" not in caplog.text


def test_preflight_only_reports_every_finding_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    plan = runner.prepare_plan(settings(tmp_path))
    result = _complete_report(plan)
    result.issues = [
        CommandPreflightIssue(plan.runs[0], PreflightIssue(1, "", "missing", "test", f"file-{i}", "unavailable"))
        for i in range(13)
    ]

    def return_findings(*_args: object, **_kwargs: object) -> CommandPreflightReport:
        return result

    monkeypatch.setattr(command_preflight, "check_command_plan", return_findings)
    caplog.set_level(logging.INFO)
    assert runner.check_plan_only(plan) is False
    assert sum("Input check run" in record.message for record in caplog.records) == 13

    def unavailable(*_args: object, **_kwargs: object) -> CommandPreflightReport:
        message = "public API unavailable"
        raise ImportError(message)

    monkeypatch.setattr(command_preflight, "check_command_plan", unavailable)
    assert runner.check_plan_only(plan) is False
    assert "public API unavailable" in caplog.text


def test_gpu_inspected_only_once_and_inspection_uncertainty_is_not_all_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from ryan_library.functions.tuflow import gpu_occupancy

    calls: list[int] = []

    def snapshot() -> GpuOccupancySnapshot:
        calls.append(1)
        return GpuOccupancySnapshot(
            assigned=(TuflowGpuProcess(9991, frozenset({0})),),
            unknown_pids=(9992,),
            inspection_incomplete=True,
        )

    monkeypatch.setattr(gpu_occupancy, "local_gpu_occupancy", snapshot)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    config = settings(tmp_path, gpu_devices=(("-pu0",),))
    plan = runner.prepare_plan(config)
    caplog.set_level(logging.INFO)
    count: list[int] = []

    def popen(**_kwargs: object) -> FakeProcess:
        count.append(1)
        return FakeProcess(len(count))

    outcome = runner.launch_plan(plan, config, popen=_popen_stub(popen))
    assert outcome.successful
    assert calls == [1]
    assert len(count) == 2
    assert "All configured GPU groups appear occupied" in caplog.text
    assert "PID 9991" in caplog.text
    assert "unknown" in caplog.text
    assert "uncertain" in caplog.text


def test_no_external_models_message_never_claims_overcapacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from ryan_library.functions.tuflow import gpu_occupancy

    monkeypatch.setattr(gpu_occupancy, "local_gpu_occupancy", lambda: GpuOccupancySnapshot((), (), False))
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    config = settings(tmp_path)
    caplog.set_level(logging.INFO)

    def popen(**_kwargs: object) -> FakeProcess:
        return FakeProcess(123)

    runner.launch_plan(runner.prepare_plan(config), config, popen=_popen_stub(popen))
    assert "No other running local TUFLOW models detected" in caplog.text
    assert "All configured GPU groups appear occupied" not in caplog.text
