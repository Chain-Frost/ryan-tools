"""Complete sessions remain callable without the copied wrapper's process state."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Event

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.functions.tuflow.execution_history import CsvExecutionLog
from ryan_library.functions.tuflow.runner_settings import OP
from ryan_library.orchestrators.tuflow import simulation_workflow as workflow
from ryan_library.orchestrators.tuflow.simulation_runner import BatchOutcome, RunnerSettings, RunOutcome


@pytest.fixture
def settings(tmp_path: Path) -> RunnerSettings:
    wrapper = tmp_path / "copied" / "project_v2.py"
    wrapper.parent.mkdir()
    wrapper.write_text("# Synthetic copied wrapper\n", encoding="utf-8")
    model = tmp_path / "model"
    model.mkdir()
    source = model / "models.bat"
    source.write_text(f'START /WAIT "" "TUFLOW.exe" -b -e1 A "{model / "model.tcf"}"\n', encoding="utf-8")
    return RunnerSettings(
        wrapper_file=wrapper,
        working_directory=model,
        executable=model / "TUFLOW.exe",
        tcf=None,
        input_files=(source,),
        mode="textfiles",
        wait_for_available_gpu=False,
        wait_time_after_run=0,
        use_live_dashboard=False,
        monitor_progress=False,
    )


def _forbid_runtime(*_args: object, **_kwargs: object) -> None:
    pytest.fail("A non-execution operation entered the run lifecycle")


def test_export_outcome_uses_supplied_wrapper_location(
    settings: RunnerSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Export-only forces export without requiring the absent solver/check stack."""
    for name in (
        "validate_launch_executable",
        "CsvExecutionLog",
        "AdvisoryInputCheck",
        "launch_plan",
        "check_plan_only",
    ):
        monkeypatch.setattr(workflow, name, _forbid_runtime)
    result = workflow.run_simulation_workflow(
        settings, workflow.WorkflowControls(operation=OP.EXPORT_ONLY, export_commands=False)
    )
    assert result.operation is OP.EXPORT_ONLY
    assert result.exit_code == 0
    assert result.export_path == settings.wrapper_file.with_suffix(".txt")
    assert result.export_path is not None
    assert result.export_path.is_file()
    assert result.preflight_passed is None
    assert result.batch is None
    assert list(settings.working_directory.glob("*.txt")) == []
    assert list(settings.wrapper_file.parent.glob("*.csv")) == []


@pytest.mark.parametrize("passed", [False, True])
def test_preflight_outcome_reports_strict_result_without_run_resources(
    settings: RunnerSettings, monkeypatch: pytest.MonkeyPatch, passed: bool
) -> None:
    checked: list[int] = []

    def check(plan: CommandPlan) -> bool:
        checked.append(len(plan.runs))
        return passed

    monkeypatch.setattr(workflow, "check_plan_only", check)
    for name in ("validate_launch_executable", "CsvExecutionLog", "AdvisoryInputCheck", "launch_plan"):
        monkeypatch.setattr(workflow, name, _forbid_runtime)
    result = workflow.run_simulation_workflow(
        settings,
        workflow.WorkflowControls(operation=OP.PREFLIGHT_ONLY, export_commands=False, check_inputs=False),
    )
    assert checked == [1]
    assert result.preflight_passed is passed
    assert result.exit_code == (0 if passed else 1)
    assert result.export_path is None
    assert result.batch is None
    assert list(settings.wrapper_file.parent.glob("*.csv")) == []


def test_library_session_resumes_outside_main_thread_and_releases_history(
    settings: RunnerSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default library launch context must not install process signal handlers."""
    settings.executable.write_bytes(b"synthetic solver placeholder")
    controls = workflow.WorkflowControls(export_commands=False, check_inputs=False)
    launched: list[int] = []

    def launch(
        plan: CommandPlan,
        _settings: RunnerSettings,
        *,
        proposed_plan: CommandPlan,
        interrupted: Event,
        on_before_launch: Callable[[PlannedRun], None],
        on_observed_completion: Callable[[RunOutcome], None],
        **_kwargs: object,
    ) -> BatchOutcome:
        assert proposed_plan == plan
        assert not interrupted.is_set()
        observations: list[RunOutcome] = []
        for run in plan.runs:
            launched.append(run.index)
            on_before_launch(run)
            now = datetime.now()
            observed = RunOutcome(run, 0, now, now, 42)
            on_observed_completion(observed)
            observations.append(observed)
        return BatchOutcome(plan.runs, tuple(observations))

    monkeypatch.setattr(workflow, "launch_plan", launch)
    with ThreadPoolExecutor(max_workers=1) as worker:
        result = worker.submit(workflow.run_simulation_workflow, settings, controls).result(timeout=10)
        assert result.exit_code == 0
        assert result.batch is not None
        assert result.batch.successful
        assert result.skipped == 0
        history_path = settings.wrapper_file.with_name("project_v2_runs.csv")
        with CsvExecutionLog(history_path) as history:
            assert len(history.successful_keys()) == 1
        before_resume = history_path.read_bytes()
        settings.executable.unlink()
        resumed = worker.submit(workflow.run_simulation_workflow, settings, controls).result(timeout=10)
    assert resumed.exit_code == 0
    assert resumed.batch == BatchOutcome((), ())
    assert resumed.skipped == 1
    assert launched == [1]
    assert history_path.read_bytes() == before_resume
    assert not (settings.working_directory / history_path.name).exists()
