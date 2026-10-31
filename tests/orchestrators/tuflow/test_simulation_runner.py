"""Scope 03 acceptance: deterministic fake solver processes, no TUFLOW execution."""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlanError, parse_command_plan
from ryan_library.functions.tuflow.gpu_occupancy import GpuOccupancySnapshot
from ryan_library.orchestrators.tuflow import simulation_runner as runner


def settings(base: Path, **changes: object) -> runner.RunnerSettings:
    initial = runner.RunnerSettings(
        wrapper_file=base / "run_tuflow_simulations.py",
        working_directory=base,
        executable=base / "TUFLOW_iSP_w64.exe",
        tcf=Path("runs/model_~e1~.tcf"),
        run_variables={"e1": ["A", "B"]},
        batch_commands="-b",
        wait_time_after_run=0,
        use_live_dashboard=False,
    )
    return replace(initial, **changes)


class FakeProcess:
    """Process whose completion is controlled by the test, not an operating system."""

    def __init__(self, pid: int, code: int = 0, polls_before_exit: int = 0) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self.code = code
        self.polls_before_exit = polls_before_exit
        self.terminated = False

    def poll(self) -> int | None:
        if self.terminated:
            self.returncode = -15
        elif self.polls_before_exit:
            self.polls_before_exit -= 1
        else:
            self.returncode = self.code
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.terminated = True
        self.returncode = -15
        return -15

    def kill(self) -> None:
        self.terminated = True


def test_parameter_product_keeps_deliberate_duplicates_and_order(tmp_path: Path) -> None:
    config = settings(tmp_path, run_variables={"e1": ["A", "A", "B"]})
    plan = runner.prepare_plan(config)
    assert [run.index for run in plan.runs] == [1, 2, 3]
    assert [run.flags for run in plan.runs] == [{"e1": "A"}, {"e1": "A"}, {"e1": "B"}]
    assert plan.runs[0] is not plan.runs[1]
    assert all(run.working_directory == tmp_path for run in plan.runs)
    assert all(run.source == config.wrapper_file for run in plan.runs)


def test_generated_quoted_model_value_roundtrip(tmp_path: Path) -> None:
    config = settings(tmp_path, run_variables={"e1": ["A with spaces", "B"]})
    plan = runner.prepare_plan(config)
    exported = tmp_path / "editable.bat"
    text = runner.render_batch_commands(plan, config, destination=exported)
    assert "%BCMD%" in text
    assert "%TUFLOW_EXE%" in text
    assert "%TCF%" in text
    assert '-e1 "A with spaces"' in text
    parsed = parse_command_plan(text, source=exported, initial_directory=tmp_path)
    assert [run.flags for run in parsed.runs] == [run.flags for run in plan.runs]


def _make_import(base: Path) -> Path:
    first, second = base / "Model A", base / "Model B"
    first.mkdir()
    second.mkdir()
    bat = base / "input.bat"
    bat.write_text(
        f'set "TCF_A={first / "one.tcf"}"\n'
        f'set "TCF_B={second / "two.tcf"}"\n'
        f'START /D "{first}" /LOW /MIN /WAIT "" "TUFLOW.exe" -b -pu0 -e1 A "%TCF_A%"\n'
        f'START /D "{second}" /LOW /MIN /WAIT "" "TUFLOW.exe" -b -pu1 -e1 B "%TCF_B%"\n'
        f'START /D "{first}" /LOW /MIN /WAIT "" "TUFLOW.exe" -b -pu0 -e1 A "%TCF_A%"\n',
        encoding="utf-8",
    )
    return bat


def test_import_multi_tcf_cwd_and_duplicates(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(bat,))
    plan = runner.prepare_plan(config)
    assert len(plan.runs) == 3
    assert [run.tcf.name for run in plan.runs] == ["one.tcf", "two.tcf", "one.tcf"]
    assert [run.working_directory.name for run in plan.runs] == ["Model A", "Model B", "Model A"]
    assert [run.line_start for run in plan.runs] == [3, 4, 5]
    assert all(run.source == bat for run in plan.runs)
    assert plan.runs[0].arguments == plan.runs[2].arguments


def test_late_conflict_fails_before_export_or_execution(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    expected = tmp_path / "Model A" / "one.tcf"
    config = settings(tmp_path, mode="textfiles", tcf=expected, input_files=(bat,))
    with pytest.raises(CommandPlanError, match=r"input\.bat.*Python TCF.*two\.tcf"):
        runner.prepare_plan(config)
    assert list(tmp_path.glob("*.bat")) == [bat]


def test_import_and_generated_use_one_ordered_plan(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    config = settings(tmp_path, mode="both", tcf=None, input_files=(bat,))
    with pytest.raises(CommandPlanError, match="Generated parameter-product runs require"):
        runner.prepare_plan(config)
    config = settings(tmp_path, mode="both", tcf=tmp_path / "Model A" / "one.tcf", input_files=(bat,))
    with pytest.raises(CommandPlanError, match="Python TCF"):
        runner.prepare_plan(config)


def test_import_roundtrip_preserves_model_executable_cwd_order_and_duplicates(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(bat,))
    plan = runner.prepare_plan(config)
    output = tmp_path / "copied_commands.bat"
    text = runner.render_batch_commands(plan, config, destination=output)
    assert "TCF_1" in text
    assert "TCF_2" in text
    assert 'set "TUFLOW_EXE=TUFLOW.exe"' in text
    assert 'set "BCMD=-b"' in text
    assert text.count("START /D") == 3
    new = runner.export_batch_commands(plan, config, output)
    reparsed = runner.prepare_plan(replace(config, input_files=(new,)))
    assert [runner._logical_signature(run, config) for run in plan.runs] == [
        runner._logical_signature(run, config) for run in reparsed.runs
    ]
    with pytest.raises(FileExistsError):
        runner.export_batch_commands(plan, config, output)
    with pytest.raises(CommandPlanError, match="Refusing to overwrite"):
        runner.export_batch_commands(plan, config, bat)


def test_export_without_solver_or_gpu_inspection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = settings(tmp_path, executable=tmp_path / "TUFLOW_missing.exe")
    monkeypatch.setattr(runner, "_gpu_startup_message", lambda *_args: pytest.fail("GPU scanned during export"))
    output = tmp_path / "runs.bat"
    runner.export_batch_commands(runner.prepare_plan(config), config, output)
    assert output.exists()


def test_per_child_directory_argument_list_and_runtime_gpu_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bat = _make_import(tmp_path)
    config = settings(
        tmp_path,
        mode="textfiles",
        tcf=None,
        input_files=(bat,),
        gpu_devices=(("-pu2",), ("-pu3",)),
        maximum_concurrent=2,
    )
    plan = runner.prepare_plan(config)
    launched: list[tuple[list[str], str]] = []
    snapshots: list[int] = []

    def inspect() -> GpuOccupancySnapshot:
        snapshots.append(1)
        return GpuOccupancySnapshot((), (), False)

    from ryan_library.functions.tuflow import gpu_occupancy

    monkeypatch.setattr(gpu_occupancy, "local_gpu_occupancy", inspect)
    monkeypatch.setattr(runner, "_apply_priority", lambda *_args: None)

    def popen(*, args: list[str], cwd: str, **_kwargs: object) -> FakeProcess:
        launched.append((args, cwd))
        return FakeProcess(len(launched), polls_before_exit=5 if len(launched) == 1 else 0)

    outcome = runner.launch_plan(plan, config, popen=popen)
    assert outcome.successful
    assert len(launched) == 3
    assert [cwd for _, cwd in launched] == [str(run.working_directory) for run in plan.runs]
    assert all(isinstance(args, list) and args[0] == "TUFLOW.exe" for args, _ in launched)
    assert all(sum(arg.startswith("-pu") for arg in args) == 1 for args, _ in launched)
    assert {arg for args, _ in launched for arg in args if arg.startswith("-pu")} == {"-pu2", "-pu3"}
    assert len(snapshots) == 1


def test_launch_failure_and_priority_failure_cannot_lose_scheduler_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = settings(tmp_path, gpu_devices=(("-pu0",),))
    plan = runner.prepare_plan(config)
    attempts: list[int] = []
    monkeypatch.setattr(runner, "_gpu_startup_message", lambda *_args: None)

    def popen(**_options: object) -> FakeProcess:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            message = "fake launch failure"
            raise FileNotFoundError(message)
        return FakeProcess(321)

    def failed_priority(*_args: object) -> None:
        message = "fake priority denial"
        raise PermissionError(message)

    monkeypatch.setattr(runner, "_apply_priority", failed_priority)
    outcome = runner.launch_plan(plan, config, popen=popen)
    assert attempts == [0, 1]
    assert len(outcome.outcomes) == 2
    assert not outcome.successful
    assert outcome.exit_code == 1
    assert [item.return_code for item in outcome.outcomes] == [None, 0]
    assert "fake launch failure" in str(outcome.outcomes[0].error)


def test_out_of_order_completion_callback_precedes_ordered_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config = settings(tmp_path, gpu_devices=(("-pu0",), ("-pu1",)), maximum_concurrent=2)
    plan = runner.prepare_plan(config)
    monkeypatch.setattr(runner, "_gpu_startup_message", lambda *_args: None)
    monkeypatch.setattr(runner, "_apply_priority", lambda *_args: None)
    processes = iter((FakeProcess(1, polls_before_exit=5), FakeProcess(2)))
    observed: list[int] = []
    began: list[int] = []
    caplog.set_level(logging.INFO)
    outcome = runner.launch_plan(
        plan,
        config,
        popen=lambda **_options: next(processes),
        on_before_launch=lambda run: began.append(run.index),
        on_observed_completion=lambda record: observed.append(record.run.index),
    )
    assert outcome.successful
    assert began == [1, 2]
    assert observed == [2, 1]
    assert [record.run.index for record in outcome.outcomes] == [2, 1]
    assert caplog.text.index("Run 1: OK") < caplog.text.index("Run 2: OK")


def test_interruption_terminates_owned_child_without_imaginary_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = settings(tmp_path)
    plan = runner.prepare_plan(config)
    stop = Event()
    monkeypatch.setattr(runner, "_gpu_startup_message", lambda *_args: None)
    monkeypatch.setattr(runner, "_apply_priority", lambda *_args: None)
    owned: list[FakeProcess] = []

    def start(**_options: object) -> FakeProcess:
        process = FakeProcess(1, polls_before_exit=999)
        owned.append(process)
        stop.set()
        return process

    outcome = runner.launch_plan(plan, config, interrupted=stop, popen=start)
    assert outcome.interrupted
    assert not outcome.successful
    assert len(owned) == 1
    assert owned[0].terminated
    assert [record.return_code for record in outcome.outcomes] == [-15]


def test_empty_plan_and_invalid_settings_rejected(tmp_path: Path) -> None:
    config = settings(tmp_path, mode="textfiles", input_files=())
    with pytest.raises(CommandPlanError, match="Input BAT/text"):
        runner.prepare_plan(config)
    with pytest.raises(CommandPlanError, match="GPU group"):
        runner.prepare_plan(settings(tmp_path, gpu_devices=(("-puA",),)))
    with pytest.raises(CommandPlanError, match="priority"):
        runner.prepare_plan(settings(tmp_path, computational_priority="INVALID"))
