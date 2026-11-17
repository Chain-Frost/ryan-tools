"""Advisory check and one-time external GPU snapshot acceptance (fake processes)."""

from __future__ import annotations

import logging
from collections.abc import Generator
from dataclasses import replace
from pathlib import Path
from threading import Event

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
    # Other workflow tests can leave this namespace disabled. A new sink alone
    # does not restore emission, so establish the capture configuration here.
    logger.enable("ryan_library")
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
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9


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
        assert advisory.status == "Running"

        def popen(**_kwargs: object) -> FakeProcess:
            assert not release.is_set()
            launched.append(1)
            return FakeProcess(len(launched))

        outcome = runner.launch_plan(plan, config, advisory=advisory, popen=popen)
        assert outcome.successful
        assert len(launched) == 2
        release.set()
        assert advisory.finished.wait(3)
        assert advisory.status == "Issues: 25 across 1 run"
        advisory.drain()
        assert sum("Input check run" in record.message for record in caplog.records) == 25
        assert "Input check completed for all 2 runs; 1 run has issues." in caplog.text
        assert "failed or incomplete for 2/2" not in caplog.text
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
        assert advisory.status == "Incomplete"
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
    assert "Control file:" in caplog.text
    assert "Source      :" in caplog.text
    assert "Command     : Full command not provided by validator" in caplog.text
    assert str(plan.runs[0].tcf.name) in caplog.text
    assert "Input check completed for all 2 runs; 1 run has issues." in caplog.text

    def unavailable(*_args: object, **_kwargs: object) -> CommandPreflightReport:
        message = "public API unavailable"
        raise ImportError(message)

    monkeypatch.setattr(command_preflight, "check_command_plan", unavailable)
    assert runner.check_plan_only(plan) is False
    assert "public API unavailable" in caplog.text


def test_input_check_summary_distinguishes_incomplete_coverage_from_issues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    plan = runner.prepare_plan(settings(tmp_path))
    report = CommandPreflightReport(plan, completed_runs=1)
    report.issues = [
        CommandPreflightIssue(plan.runs[0], PreflightIssue(1, "", "missing", "test", "file", "unavailable"))
    ]

    def return_report(*_args: object, **_kwargs: object) -> CommandPreflightReport:
        return report

    monkeypatch.setattr(command_preflight, "check_command_plan", return_report)
    caplog.set_level(logging.INFO)

    assert runner.check_plan_only(plan) is False
    assert "Input check incomplete: checked 1 of 2 runs; 1 run has issues." in caplog.text


def test_input_check_summary_reports_unchecked_runs_without_claiming_they_were_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    plan = runner.prepare_plan(settings(tmp_path))
    report = CommandPreflightReport(plan, completed_runs=0)
    report.issues = [
        CommandPreflightIssue(
            run, PreflightIssue(run.index, "", "unresolved", "test", "validator", "validation failed")
        )
        for run in plan.runs
    ]

    def return_report(*_args: object, **_kwargs: object) -> CommandPreflightReport:
        return report

    monkeypatch.setattr(command_preflight, "check_command_plan", return_report)
    caplog.set_level(logging.INFO)

    assert runner.check_plan_only(plan) is False
    assert "Input check incomplete: checked 0 of 2 runs; 2 runs have issues." in caplog.text

    caplog.clear()
    report.completed_runs = 1
    assert runner.check_plan_only(plan) is False
    assert "Input check incomplete: checked 1 of 2 runs; 2 runs have issues." in caplog.text


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
    config = settings(tmp_path, gpu_devices=(("-pu0",),), wait_for_available_gpu=False)
    plan = runner.prepare_plan(config)
    caplog.set_level(logging.INFO)
    count: list[int] = []

    def popen(**_kwargs: object) -> FakeProcess:
        count.append(1)
        return FakeProcess(len(count))

    outcome = runner.launch_plan(plan, config, popen=popen)
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
    caplog.set_level(logger.level("SUCCESS").no)

    def popen(**_kwargs: object) -> FakeProcess:
        return FakeProcess(123)

    runner.launch_plan(runner.prepare_plan(config), config, popen=popen)
    assert "No other running local TUFLOW models detected" in caplog.text
    assert (
        next(
            record.levelno
            for record in caplog.records
            if "No other running local TUFLOW models detected" in record.message
        )
        == logger.level("SUCCESS").no
    )
    assert "All configured GPU groups appear occupied" not in caplog.text


def test_repeated_bc_sources_have_compact_console_groups_without_losing_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    plan = runner.prepare_plan(settings(tmp_path))
    result = _complete_report(plan)
    shared = tmp_path / "rain_A.csv"
    result.issues = [
        CommandPreflightIssue(
            plan.runs[0],
            PreflightIssue(
                1, "", "missing", "BC", f"{shared} >> boundary_{number}", "file not found",
                kind="bc_source", path=shared, csv_columns=("Time", f"Q{number}"),
            ),
        )
        for number in range(12)
    ]
    result.issues.append(
        CommandPreflightIssue(
            plan.runs[1],
            PreflightIssue(2, "", "missing", "BC", str(shared), "file not found", kind="bc_source", path=shared),
        )
    )
    result.parse_seconds = 1.25
    result.context_seconds = 4.5
    result.inventory_seconds = 2.75

    monkeypatch.setattr(command_preflight, "check_command_plan", lambda *_a, **_kw: result)
    caplog.set_level(logging.INFO)
    assert runner.check_plan_only(plan) is False
    grouped = [record.message for record in caplog.records if "Input check run" in record.message]
    assert len(grouped) == 2
    assert "Repeated references: 12 findings" in grouped[0]
    assert "12 column selections" in grouped[0]
    assert "Input check run 2" in grouped[1]
    assert "13 blocking findings" in caplog.text
    assert "2 affected" in caplog.text
    assert "dependencies 4.50 s" in caplog.text


def test_preflight_csv_export_is_opt_in_and_no_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import csv

    plan = runner.prepare_plan(settings(tmp_path))
    report = _complete_report(plan)
    source = tmp_path / "missing.csv"
    report.issues = [
        CommandPreflightIssue(
            plan.runs[0],
            PreflightIssue(
                1, "", "missing", "BC", f"{source} >> BC_{i}", "missing",
                kind="bc_source", path=source, csv_columns=("Time", f"Q{i}"),
            ),
        )
        for i in range(2)
    ]
    monkeypatch.setattr(command_preflight, "check_command_plan", lambda *_a, **_kw: report)
    destination = tmp_path / "reports" / "findings.csv"
    caplog.set_level(logging.INFO)
    assert runner.check_plan_only(plan, report_csv=destination) is False
    with destination.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert [row["csv_columns_json"] for row in rows] == ['["Time", "Q0"]', '["Time", "Q1"]']
    original = destination.read_bytes()
    assert runner.check_plan_only(plan, report_csv=destination) is False
    assert "CSV was not created" in caplog.text
    assert destination.read_bytes() == original

    report.issues.clear()
    passed_csv = tmp_path / "passing.csv"
    passed_csv.write_text("existing", encoding="utf-8")
    assert runner.check_plan_only(plan, report_csv=passed_csv) is False
    assert passed_csv.read_text(encoding="utf-8") == "existing"
