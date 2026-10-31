"""Scope 03 acceptance: deterministic fake solver processes, no TUFLOW execution."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from collections.abc import Callable, Generator
from dataclasses import replace
from io import StringIO
from pathlib import Path
from threading import Event
from typing import cast

import pytest
from loguru import logger

from ryan_library.functions.tuflow.command_plan import (
    CommandPlanError,
    PlannedRun,
    parse_command_plan,
    read_command_plan,
)
from ryan_library.functions.tuflow.execution_history import identify_runs
from ryan_library.functions.tuflow.gpu_occupancy import GpuOccupancySnapshot
from ryan_library.orchestrators.tuflow import simulation_runner as runner


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


def _no_op(*_args: object) -> None:
    """Suppress unrelated GPU/priority behaviour in focused scheduler tests."""


def _unexpected_gpu_scan(*_args: object) -> None:
    pytest.fail("GPU scanned during export")


def _logical_signature(run: PlannedRun, config: runner.RunnerSettings) -> tuple[object, ...]:
    """Exercise the internal roundtrip contract with explicit, typed test access."""
    signature = cast(
        Callable[[PlannedRun, runner.RunnerSettings], tuple[object, ...]],
        getattr(runner, "_logical_signature"),  # noqa: B009 - explicitly exercise the internal test contract
    )
    return signature(run, config)


def _command(run: PlannedRun, config: runner.RunnerSettings, gpu: tuple[str, ...]) -> list[str]:
    build = cast(
        Callable[[PlannedRun, runner.RunnerSettings, tuple[str, ...]], list[str]],
        getattr(runner, "_command"),  # noqa: B009 - explicitly exercise the internal test contract
    )
    return build(run, config, gpu)


def test_parameter_product_discards_repeated_selections_in_order(tmp_path: Path) -> None:
    config = settings(tmp_path, run_variables={"e1": ["A", "A", "B"]})
    plan = runner.prepare_plan(config)
    assert [run.index for run in plan.runs] == [1, 2]
    assert [run.flags for run in plan.runs] == [{"e1": "A"}, {"e1": "B"}]
    assert plan.duplicates_dropped == 1
    assert all(run.working_directory == tmp_path for run in plan.runs)
    assert all(run.source == config.wrapper_file for run in plan.runs)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows command export requires native Windows paths")
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


@pytest.mark.skipif(sys.platform != "win32", reason="UNC rendering uses native Windows path objects")
def test_generated_unc_plan_roundtrips_as_inert_txt() -> None:
    # Rendering and reparsing are lexical; this share need not exist and no
    # network access, command execution or solver inspection is performed.
    directory = Path(r"\\server\share\Model")
    config = settings(directory, executable=Path(r"C:\TUFLOW\TUFLOW.exe"))
    plan = runner.prepare_plan(config)
    output = directory / "commands.txt"
    text = runner.render_batch_commands(plan, config, destination=output)
    replay = parse_command_plan(text, source=output, initial_directory=directory, syntax="commands")
    assert [run.tcf for run in replay.runs] == [run.tcf for run in plan.runs]
    assert all(run.working_directory == directory for run in replay.runs)
    assert [_logical_signature(run, config) for run in replay.runs] == [
        _logical_signature(run, config) for run in plan.runs
    ]


@pytest.mark.skipif(sys.platform != "win32", reason="UNC imports use native Windows path objects")
def test_runner_imports_bat_from_unc_cwd_as_inert_selections(tmp_path: Path) -> None:
    directory = Path(r"\\server\share\Model")
    selected_directory = directory / "alternate"
    source = tmp_path / "input.bat"
    source.write_text(f'CD /D "{selected_directory}"\nTUFLOW.exe -e1 A model.tcf\n', encoding="utf-8")
    config = settings(directory, mode="textfiles", input_files=(source,), tcf=None)
    plan = runner.prepare_plan(config)
    assert plan.runs[0].tcf == selected_directory / "model.tcf"
    assert plan.runs[0].working_directory == directory
    output = tmp_path / "commands.txt"
    output.write_text(runner.render_batch_commands(plan, config, destination=output), encoding="utf-8")
    replay = runner.prepare_plan(replace(config, input_files=(output,)))
    assert [_logical_signature(run, config) for run in replay.runs] == [
        _logical_signature(run, config) for run in plan.runs
    ]


def test_parent_components_roundtrip_without_changing_launch_spelling(tmp_path: Path) -> None:
    directory = tmp_path / "unused" / ".." / "project" / "scripts"
    config = settings(
        directory,
        tcf=Path("../runs/model_~e1~.tcf"),
        executable=Path("../engine/TUFLOW.exe"),
    )
    plan = runner.prepare_plan(config)
    output = runner.export_batch_commands(plan, config, tmp_path / "commands.txt")
    replay = read_command_plan(output, initial_directory=directory)
    assert all(run.tcf == tmp_path / "project" / "runs" / "model_~e1~.tcf" for run in replay.runs)
    assert all(run.working_directory == tmp_path / "project" / "scripts" for run in replay.runs)
    assert [run.flags for run in replay.runs] == [run.flags for run in plan.runs]
    assert all(run.working_directory == directory for run in plan.runs)
    command = _command(plan.runs[0], config, ())
    assert command[0] == str(directory / ".." / "engine" / "TUFLOW.exe")
    assert command[-1] == str(directory / ".." / "runs" / "model_~e1~.tcf")


def test_parent_relative_import_and_generated_selection_are_deduplicated(tmp_path: Path) -> None:
    directory = tmp_path / "scripts"
    directory.mkdir()
    source = directory / "input.txt"
    source.write_text('TUFLOW.exe -e1 A "../runs/model.tcf"\n', encoding="utf-8")
    config = settings(
        directory,
        mode="both",
        input_files=(source,),
        tcf=Path("../runs/model.tcf"),
        run_variables={"e1": ["A"]},
    )
    plan = runner.prepare_plan(config)
    assert len(plan.runs) == 1
    assert plan.duplicates_dropped == 1
    assert plan.runs[0].source == source
    assert plan.runs[0].tcf == tmp_path / "runs" / "model.tcf"


@pytest.mark.parametrize("value", ["Wet & Dry", "Storm(2)", "Rain|Wind", "Event>Base"])
def test_bat_export_quotes_supported_cmd_metacharacters(tmp_path: Path, value: str) -> None:
    project = tmp_path / "Hydrology & Drainage"
    project.mkdir()
    config = settings(project, tcf=Path("runs/model.tcf"), run_variables={"e1": [value]})
    plan = runner.prepare_plan(config)
    text = runner.render_batch_commands(plan, config, destination=project / "commands.txt")
    assert f'-e1 "{value}"' in text
    assert f'/D "{project}"' in text
    replay = parse_command_plan(text, source=project / "commands.txt", initial_directory=project)
    assert [run.flags for run in replay.runs] == [run.flags for run in plan.runs]


@pytest.mark.parametrize("value", ["Embedded%Env", "Delayed!Expansion", "Caret^Escape", 'Quote"Value'])
def test_bat_export_rejects_unrepresentable_values(tmp_path: Path, value: str) -> None:
    config = settings(tmp_path, tcf=Path("runs/model.tcf"), run_variables={"e1": [value]})
    with pytest.raises(CommandPlanError, match="Cannot safely represent BAT argument"):
        runner.render_batch_commands(runner.prepare_plan(config), config, destination=tmp_path / "commands.txt")


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


def test_import_multi_tcf_ignores_source_launch_context_and_duplicates(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(bat,))
    plan = runner.prepare_plan(config)
    assert len(plan.runs) == 2
    assert plan.duplicates_dropped == 1
    assert [run.tcf.name for run in plan.runs] == ["one.tcf", "two.tcf"]
    assert all(run.working_directory == tmp_path for run in plan.runs)
    assert [run.line_start for run in plan.runs] == [3, 4]
    assert all(run.source == bat for run in plan.runs)
    assert all(run.executable == str(config.executable) for run in plan.runs)


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


@pytest.mark.parametrize("source", ["-e1 A", 'SET "TCF=model.tcf"\\n-e1 A'])
@pytest.mark.skipif(sys.platform != "win32", reason="Windows command export requires native Windows paths")
def test_abbreviated_input_launches_and_roundtrips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    commands = tmp_path / "input.txt"
    commands.write_text(source.replace("\\n", "\n") + "\n", encoding="utf-8")
    use_python_tcf = source.startswith("-")
    config = settings(
        tmp_path,
        mode="textfiles",
        input_files=(commands,),
        tcf=Path("model.tcf") if use_python_tcf else None,
    )
    plan = runner.prepare_plan(config)
    assert len(plan.runs) == 1
    assert plan.runs[0].arguments == ("-b", "-e1", "A", str(tmp_path / "model.tcf"))
    assert plan.runs[0].tcf == tmp_path / "model.tcf"

    exported = runner.render_batch_commands(plan, config, destination=tmp_path / "out.bat")
    reparsed = parse_command_plan(exported, source=tmp_path / "out.bat", initial_directory=tmp_path)
    assert _logical_signature(plan.runs[0], config) == _logical_signature(reparsed.runs[0], config)

    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    commands_seen: list[list[str]] = []

    def fake_popen(*, args: list[str], **_options: object) -> FakeProcess:
        commands_seen.append(args)
        return FakeProcess(4455)

    result = runner.launch_plan(plan, config, popen=fake_popen)
    assert result.successful
    assert commands_seen == [[str(config.executable), "-b", "-e1", "A", str(tmp_path / "model.tcf")]]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows command export requires native Windows paths")
def test_text_flag_list_uses_configured_tcf_and_survives_csv_identity_export(tmp_path: Path) -> None:
    """A text file can list runs without repeating the Python-configured TCF."""
    commands = tmp_path / "run_list.txt"
    commands.write_text("-e1 100 -e2 creek -s4 dam\\n".replace("\\n", "\n"), encoding="utf-8")
    config = settings(
        tmp_path,
        mode="textfiles",
        tcf=Path("model.tcf"),
        input_files=(commands,),
    )
    plan = runner.prepare_plan(config)
    assert len(plan.runs) == 1
    run = plan.runs[0]
    assert run.tcf == tmp_path / "model.tcf"
    assert run.flags == {"e1": "100", "e2": "creek", "s4": "dam"}
    assert run.arguments == ("-b", "-e1", "100", "-e2", "creek", "-s4", "dam", str(tmp_path / "model.tcf"))
    assert run.executable == str(config.executable)
    identities = identify_runs(plan, fallback_executable=config.executable, base_directory=tmp_path)
    assert identities[run.index].key

    exported = runner.render_batch_commands(plan, config, destination=tmp_path / "export.bat")
    reparsed = parse_command_plan(exported, source=tmp_path / "export.bat", initial_directory=tmp_path)
    assert _logical_signature(run, config) == _logical_signature(reparsed.runs[0], config)
    assert (
        identify_runs(reparsed, fallback_executable=config.executable, base_directory=tmp_path)[1].key
        == identities[run.index].key
    )
    with pytest.raises(CommandPlanError, match="No effective TCF"):
        runner.prepare_plan(replace(config, tcf=None))


def test_nonterminal_tcf_argument_roundtrips_without_losing_flags(tmp_path: Path) -> None:
    commands = tmp_path / "input.txt"
    commands.write_text('TUFLOW.exe "model.tcf" -e1 A -s1 B\\n'.replace("\\n", "\n"), encoding="utf-8")
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(commands,))
    plan = runner.prepare_plan(config)
    assert plan.runs[0].arguments[:2] == ("-b", "-e1")
    assert _command(plan.runs[0], config, ()) == [
        str(config.executable),
        "-b",
        "-e1",
        "A",
        "-s1",
        "B",
        str(tmp_path / "model.tcf"),
    ]
    bat = runner.render_batch_commands(plan, config, destination=tmp_path / "out.bat")
    reparsed = parse_command_plan(bat, source=tmp_path / "out.bat", initial_directory=tmp_path)
    assert _logical_signature(plan.runs[0], config) == _logical_signature(reparsed.runs[0], config)


def test_scenario_value_ending_in_tcf_is_not_control_file(tmp_path: Path) -> None:
    commands = tmp_path / "input.txt"
    commands.write_text("-e1 event.tcf\\n".replace("\\n", "\n"), encoding="utf-8")
    config = settings(tmp_path, mode="textfiles", input_files=(commands,), tcf=Path("model.tcf"))
    run = runner.prepare_plan(config).runs[0]
    assert _command(run, config, ()) == [
        str(config.executable),
        "-b",
        "-e1",
        "event.tcf",
        str(tmp_path / "model.tcf"),
    ]


def test_import_roundtrip_preserves_python_executable_cwd_and_unique_runs(tmp_path: Path) -> None:
    bat = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(bat,))
    plan = runner.prepare_plan(config)
    output = tmp_path / "copied_commands.txt"
    text = runner.render_batch_commands(plan, config, destination=output)
    assert "TCF_1" in text
    assert "TCF_2" in text
    assert f'set "TUFLOW_EXE={config.executable}"' in text
    assert 'set "BCMD=-b"' in text
    assert text.count("START /D") == 2
    new = runner.export_batch_commands(plan, config, output)
    reparsed = runner.prepare_plan(replace(config, input_files=(new,)))
    assert [_logical_signature(run, config) for run in plan.runs] == [
        _logical_signature(run, config) for run in reparsed.runs
    ]
    first = output.read_bytes()
    runner.export_batch_commands(plan, config, output)
    assert output.read_bytes() == first
    with pytest.raises(CommandPlanError, match=r"\.txt"):
        runner.export_batch_commands(plan, config, bat)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows command export requires native Windows paths")
def test_export_without_solver_or_gpu_inspection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = settings(tmp_path, executable=tmp_path / "TUFLOW_missing.exe")
    monkeypatch.setattr(runner, "_gpu_startup_message", _unexpected_gpu_scan)
    output = tmp_path / "runs.txt"
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
    )
    plan = runner.prepare_plan(config)
    launched: list[tuple[list[str], str]] = []
    snapshots: list[int] = []

    def inspect() -> GpuOccupancySnapshot:
        snapshots.append(1)
        return GpuOccupancySnapshot((), (), False)

    from ryan_library.functions.tuflow import gpu_occupancy

    monkeypatch.setattr(gpu_occupancy, "local_gpu_occupancy", inspect)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)

    def popen(*, args: list[str], cwd: str, **_kwargs: object) -> FakeProcess:
        launched.append((args, cwd))
        return FakeProcess(len(launched), polls_before_exit=5 if len(launched) == 1 else 0)

    outcome = runner.launch_plan(plan, config, popen=popen)
    assert outcome.successful
    assert len(launched) == 2
    assert [cwd for _, cwd in launched] == [str(config.working_directory)] * 2
    assert all(isinstance(args, list) and args[0] == str(config.executable) for args, _ in launched)
    assert all(sum(arg.startswith("-pu") for arg in args) == 1 for args, _ in launched)
    assert {arg for args, _ in launched for arg in args if arg.startswith("-pu")} == {"-pu2", "-pu3"}
    assert len(snapshots) == 1


@pytest.mark.parametrize(
    ("gpu_devices", "expected_capacity"),
    [((), 1), ((("-pu0",),), 1), ((("-pu0",), ("-pu1",)), 2), ((("-pu0", "-pu1"),), 1)],
)
def test_concurrency_is_one_process_per_gpu_group_or_one_automatic_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gpu_devices: tuple[tuple[str, ...], ...],
    expected_capacity: int,
) -> None:
    config = settings(tmp_path, run_variables={"e1": ["A", "B", "C", "D"]}, gpu_devices=gpu_devices)
    plan = runner.prepare_plan(config)
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    monkeypatch.setattr(runner.time, "sleep", _no_op)
    processes: list[FakeProcess] = []
    peak_active = 0

    def popen(**_options: object) -> FakeProcess:
        nonlocal peak_active
        process = FakeProcess(len(processes) + 1, polls_before_exit=10)
        processes.append(process)
        peak_active = max(peak_active, sum(child.returncode is None for child in processes))
        return process

    outcome = runner.launch_plan(plan, config, popen=popen)
    assert outcome.successful
    assert len(processes) == 4
    assert peak_active == expected_capacity


def test_launch_failure_and_priority_failure_cannot_lose_scheduler_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = settings(tmp_path, gpu_devices=(("-pu0",),))
    plan = runner.prepare_plan(config)
    attempts: list[int] = []
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)

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
    config = settings(tmp_path, gpu_devices=(("-pu0",), ("-pu1",)))
    plan = runner.prepare_plan(config)
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    processes = iter((FakeProcess(1, polls_before_exit=5), FakeProcess(2)))
    observed: list[int] = []
    began: list[int] = []
    caplog.set_level(logging.INFO)

    def popen(**_options: object) -> FakeProcess:
        return next(processes)

    def on_before_launch(run: PlannedRun) -> None:
        began.append(run.index)

    def on_observed_completion(record: runner.RunOutcome) -> None:
        observed.append(record.run.index)

    outcome = runner.launch_plan(
        plan,
        config,
        popen=popen,
        on_before_launch=on_before_launch,
        on_observed_completion=on_observed_completion,
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
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
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
    with pytest.raises(CommandPlanError, match="GPU group"):
        runner.prepare_plan(settings(tmp_path, gpu_devices=(("-pu0,1",),)))
    with pytest.raises(CommandPlanError, match="priority"):
        runner.prepare_plan(settings(tmp_path, computational_priority="INVALID"))


@pytest.mark.parametrize("batch", ["-b", "-b -t -nlc"])
@pytest.mark.parametrize("complete_command", [False, True])
def test_python_engine_switches_replace_imported_modes_before_identity(
    tmp_path: Path, batch: str, complete_command: bool
) -> None:
    source = tmp_path / "input.txt"
    prefix, suffix = ("TUFLOW.exe ", " model.tcf") if complete_command else ("", "")
    source.write_text(f"{prefix}-t -x -nlc -pu7 -e1 A -et1{suffix}\n", encoding="utf-8")
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=Path("model.tcf"), batch_commands=batch)
    plan = runner.prepare_plan(config)
    expected = (*batch.split(), "-e1", "A", "-et1", str(tmp_path / "model.tcf"))
    assert plan.runs[0].arguments == expected
    assert _command(plan.runs[0], config, ("-pu2",))[1:] == [*expected[:-1], "-pu2", expected[-1]]
    identities = identify_runs(plan, fallback_executable=config.executable, base_directory=tmp_path)

    # Source execution switches have no effect on a runner's logical identity.
    source.write_text(f"{prefix}-b -pu9 -e1 A -et1{suffix}\n", encoding="utf-8")
    replanned = runner.prepare_plan(config)
    assert identify_runs(replanned, fallback_executable=config.executable, base_directory=tmp_path) == identities
    changed = runner.prepare_plan(replace(config, batch_commands="-x"))
    assert (
        identify_runs(changed, fallback_executable=config.executable, base_directory=tmp_path)[1].key
        != identities[1].key
    )


def test_export_uses_python_engine_switches_and_preserves_model_options(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_text("TUFLOW.exe -t -pu7 -e1 A -et1 model.tcf\n", encoding="utf-8")
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None, batch_commands="-b -t -nlc -pu2")
    plan = runner.prepare_plan(config)
    output = runner.export_batch_commands(plan, config, tmp_path / "commands.TXT")
    text = output.read_text(encoding="utf-8")
    assert 'set "BCMD=-b -t -nlc -pu2"' in text
    assert "-pu7" not in text
    assert "-et1" in text
    assert _command(plan.runs[0], config, ("-pu3",)) == [
        str(config.executable),
        "-b",
        "-t",
        "-nlc",
        "-e1",
        "A",
        "-et1",
        "-pu3",
        str(tmp_path / "model.tcf"),
    ]


def test_imported_environment_variables_are_available(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "environment.bat"
    source.write_text('TUFLOW.exe -e1 "%RUN_EVENT%" "%MODEL_TCF%"\n', encoding="utf-8")
    monkeypatch.setenv("RUN_EVENT", "A with spaces")
    monkeypatch.setenv("MODEL_TCF", "model.tcf")
    plan = runner.prepare_plan(settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None))
    assert plan.runs[0].flags == {"e1": "A with spaces"}
    assert plan.runs[0].tcf == tmp_path / "model.tcf"


@pytest.mark.parametrize("suffix", [".bat", ".CMD", ".csv"])
def test_export_refuses_executable_or_unrelated_extensions(tmp_path: Path, suffix: str) -> None:
    source = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None)
    target = tmp_path / f"commands{suffix}"
    with pytest.raises(CommandPlanError, match=r"\.txt"):
        runner.export_batch_commands(runner.prepare_plan(config), config, target)
    assert not target.exists()


@pytest.mark.parametrize("kind", ["source", "wrapper", "history", "model", "executable"])
def test_export_refuses_hardlink_aliases_of_protected_files(tmp_path: Path, kind: str) -> None:
    source = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None)
    plan = runner.prepare_plan(config)
    protected = {
        "source": source,
        "wrapper": config.wrapper_file,
        "history": config.wrapper_file.with_name(f"{config.wrapper_file.stem}_runs.csv"),
        "model": Path(str(plan.runs[0].tcf)),
        "executable": config.executable,
    }[kind]
    if not protected.exists():
        protected.write_bytes(b"protected content\n")
    previous = protected.read_bytes()
    target = tmp_path / "commands.txt"
    target.hardlink_to(protected)
    with pytest.raises(CommandPlanError, match="Refusing to overwrite"):
        runner.export_batch_commands(plan, config, target)
    assert protected.read_bytes() == previous
    assert target.samefile(protected)


@pytest.mark.parametrize("parent_alias", [False, True])
def test_export_refuses_symbolic_link_source_aliases(tmp_path: Path, parent_alias: bool) -> None:
    original = _make_import(tmp_path)
    source = tmp_path / "source.txt"
    source.write_bytes(original.read_bytes())
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None)
    plan = runner.prepare_plan(config)
    link = tmp_path / ("directory_alias" if parent_alias else "commands.txt")
    try:
        link.symlink_to(tmp_path if parent_alias else source, target_is_directory=parent_alias)
    except OSError as exc:
        if os.name == "nt" and exc.winerror in {5, 1314}:
            pytest.skip("Windows symlink creation requires an enabled privilege")
        raise
    target = link / source.name if parent_alias else link
    previous = source.read_bytes()
    with pytest.raises(CommandPlanError, match="Refusing to overwrite"):
        runner.export_batch_commands(plan, config, target)
    assert source.read_bytes() == previous


@pytest.mark.parametrize("failure", ["fsync", "replace", "render"])
def test_failed_export_preserves_previous_output_and_cleans_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    source = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None)
    plan = runner.prepare_plan(config)
    target = runner.export_batch_commands(plan, config, tmp_path / "commands.txt")
    previous, files = target.read_bytes(), set(tmp_path.iterdir())

    def fail(*_args: object, **_kwargs: object) -> None:
        message = "Synthetic export failure"
        raise OSError(message)

    if failure == "fsync":
        monkeypatch.setattr(runner.os, "fsync", fail)
    elif failure == "replace":
        monkeypatch.setattr(Path, "replace", fail)
    else:
        monkeypatch.setattr(runner, "parse_command_plan", fail)
    with pytest.raises(OSError, match="Synthetic export failure"):
        runner.export_batch_commands(plan, config, target)
    assert target.read_bytes() == previous
    assert set(tmp_path.iterdir()) == files


def test_export_rechecks_destination_alias_before_atomic_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _make_import(tmp_path)
    config = settings(tmp_path, mode="textfiles", input_files=(source,), tcf=None)
    plan = runner.prepare_plan(config)
    target = tmp_path / "commands.txt"
    original = source.read_bytes()
    real_fsync = os.fsync

    def redirect(_descriptor: int) -> None:
        real_fsync(_descriptor)
        target.hardlink_to(source)

    monkeypatch.setattr(runner.os, "fsync", redirect)
    with pytest.raises(CommandPlanError, match="Refusing to overwrite"):
        runner.export_batch_commands(plan, config, target)
    assert target.samefile(source)
    assert source.read_bytes() == original
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_shared_dashboard_shows_middle_active_run_and_original_resume_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from collections.abc import Sequence

    from rich.console import Console, RenderableType

    from ryan_library.functions import live_dashboard

    config = settings(tmp_path, run_variables={"e1": [str(i) for i in range(1, 401)]}, use_live_dashboard=True)
    full = runner.prepare_plan(config)
    plan = replace(full, runs=full.runs[100:])
    rendered: list[str] = []
    displays: list[live_dashboard.LiveWorkflowDashboard] = []
    observed: list[int] = []
    actual_type = live_dashboard.LiveWorkflowDashboard

    def display(
        *, title: str, refresh_per_second: float, transient: bool, columns: Sequence[live_dashboard.WorkflowColumn]
    ) -> live_dashboard.LiveWorkflowDashboard:
        assert transient is False
        result = actual_type(
            title=title, refresh_per_second=refresh_per_second, transient=transient, columns=columns, enabled=False
        )
        result.console = Console(file=StringIO(), record=True, width=100, height=25, color_system=None)
        displays.append(result)
        return result

    class CapturedProcess(FakeProcess):
        def poll(self) -> int | None:
            if self.pid == 250:
                dashboard = displays[0]
                render = cast(Callable[[], RenderableType], getattr(dashboard, "_render"))  # noqa: B009
                dashboard.console.print(render())
                rendered.append(dashboard.console.export_text())
            return super().poll()

    runs = iter(plan.runs)

    def popen(**_options: object) -> FakeProcess:
        return CapturedProcess(next(runs).index)

    def finished(result: runner.RunOutcome) -> None:
        observed.append(result.run.index)

    monkeypatch.setattr(live_dashboard, "LiveWorkflowDashboard", display)
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)
    outcome = runner.launch_plan(plan, config, popen=popen, on_observed_completion=finished)
    assert outcome.successful
    assert observed == list(range(101, 401))
    assert len(displays) == 1
    assert rendered
    assert "250" in rendered[0]
    assert "RUNNING" in rendered[0]


def test_imported_start_executable_and_directory_never_control_launch(tmp_path: Path) -> None:
    source_cwd = tmp_path / "Source-Only"
    source_cwd.mkdir()
    batch = tmp_path / "source.bat"
    batch.write_text(
        f'START /D "{source_cwd}" /HIGH /WAIT "" "TUFLOW.exe" -t -pu7 -e1 A "{tmp_path / "model.tcf"}"\n',
        encoding="utf-8",
    )
    config = settings(tmp_path, mode="textfiles", tcf=None, input_files=(batch,))
    plan = runner.prepare_plan(config)
    run = plan.runs[0]
    assert run.executable == str(config.executable)
    assert run.working_directory == tmp_path
    assert run.arguments == ("-b", "-e1", "A", str(tmp_path / "model.tcf"))
    assert _command(run, config, ("-pu2",))[0] == str(config.executable)
    assert "-pu7" not in _command(run, config, ("-pu2",))


def test_configured_solver_must_exist_for_actual_execution(tmp_path: Path) -> None:
    config = settings(tmp_path, executable=tmp_path / "unavailable.exe")
    plan = runner.prepare_plan(config)
    with pytest.raises(CommandPlanError, match="Configured Python TUFLOW_EXE does not exist"):
        runner.validate_launch_executable(plan, config)
    config.executable.write_bytes(b"synthetic solver placeholder")
    runner.validate_launch_executable(plan, config)


@pytest.mark.skipif(sys.platform != "win32", reason="Native Windows subprocess lifecycle smoke")
@pytest.mark.parametrize("child_exit_code", [0, 7])
def test_windows_real_python_child_exit_and_callbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, child_exit_code: int
) -> None:
    """Exercise the real OS/Popen path with harmless Python, never TUFLOW or BAT."""
    base = tmp_path / "Model With Spaces"
    base.mkdir()
    config = settings(base, run_variables={"e1": ("A",)}, minimize_on_launch=False)
    plan = runner.prepare_plan(config)
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)

    expected = [str(config.executable), "-b", "-e1", "A", str(plan.runs[0].tcf)]
    calls: list[tuple[list[str], str]] = []
    started: list[int] = []
    completed: list[runner.RunOutcome] = []
    marker = base / "harmless-python-child.txt"
    script = (
        "from pathlib import Path; import sys; "
        f"Path('harmless-python-child.txt').write_text('child ran', encoding='utf-8'); sys.exit({child_exit_code})"
    )

    def spawn(
        *,
        args: list[str],
        cwd: str,
        creationflags: int,
        startupinfo: subprocess.STARTUPINFO | None,
    ) -> subprocess.Popen[bytes]:
        # Verify the runner's TUFLOW argv without executing it.
        calls.append((args, cwd))
        return subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=cwd,
            creationflags=creationflags,
            startupinfo=startupinfo,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    result = runner.launch_plan(
        plan,
        config,
        popen=spawn,
        on_before_launch=lambda run: started.append(run.index),
        on_observed_completion=completed.append,
    )
    assert calls == [(expected, str(base))]
    assert marker.read_text(encoding="utf-8") == "child ran"
    assert started == [1]
    assert len(completed) == len(result.outcomes) == 1
    assert completed[0] is result.outcomes[0]
    assert completed[0].pid is not None
    assert completed[0].return_code == child_exit_code
    assert result.successful is (child_exit_code == 0)
    assert result.exit_code == (0 if child_exit_code == 0 else 1)


@pytest.mark.skipif(sys.platform != "win32", reason="Native Windows subprocess lifecycle smoke")
def test_windows_real_python_child_is_terminated_on_interrupt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reap a real sleeping Python child; record no unobserved success."""
    config = settings(tmp_path, run_variables={"e1": ("A",)}, minimize_on_launch=False)
    plan = runner.prepare_plan(config)
    stop = Event()
    processes: list[subprocess.Popen[bytes]] = []
    monkeypatch.setattr(runner, "_gpu_startup_message", _no_op)
    monkeypatch.setattr(runner, "_apply_priority", _no_op)

    def spawn(
        *,
        args: list[str],
        cwd: str,
        creationflags: int,
        startupinfo: subprocess.STARTUPINFO | None,
    ) -> subprocess.Popen[bytes]:
        assert args[0] == str(config.executable)
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            cwd=cwd,
            creationflags=creationflags,
            startupinfo=startupinfo,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(process)
        stop.set()
        return process

    try:
        result = runner.launch_plan(plan, config, interrupted=stop, popen=spawn)
        assert result.interrupted
        assert result.exit_code == 1
        assert len(processes) == len(result.outcomes) == 1
        assert result.outcomes[0].pid == processes[0].pid
        assert result.outcomes[0].return_code not in {None, 0}
        assert processes[0].poll() is not None
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
