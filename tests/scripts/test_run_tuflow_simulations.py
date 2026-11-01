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
from loguru import logger

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.functions.tuflow.runner_user_inputs import choice
from ryan_library.orchestrators.tuflow.simulation_runner import BatchOutcome, RunnerSettings, RunOutcome, prepare_plan


class _RunnerWrapper(Protocol):
    __file__: str
    EXPORT_COMMANDS: bool
    NEXT_RUN_FILE: str | Path | None
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
    module.__dict__["OPERATION"] = "run"  # Explicit execution mode for these run-focused fixtures.
    return cast(_RunnerWrapper, module)


def _source(path: Path) -> Path:
    batch = path / "models.bat"
    batch.write_text(
        f'START /WAIT "" "TUFLOW.exe" -b -e1 A "{path / "model.tcf"}"\n',
        encoding="utf-8",
    )
    return batch


def test_configured_export_only_operation_and_cli_override(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    source = _source(tmp_path)
    monkeypatch.setattr(module, "OPERATION", "export_only")
    monkeypatch.setattr(module, "TUFLOW_EXE", tmp_path / "TUFLOW_MISSING.exe")

    def forbid(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Export-only attempted to launch a solver")

    monkeypatch.setattr(module, "launch_plan", forbid)
    args = ["--mode", "textfiles", "--input-file", str(source), "--no-tcf", "--no-pause"]
    assert wrapper.main(args) == 0
    assert Path(wrapper.__file__).with_suffix(".txt").is_file()
    assert not Path(wrapper.__file__).with_name("run_tuflow_simulations_runs.csv").exists()

    # CLI launch intent overrides the configured export-only operation; the missing
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


def _settings_from_wrapper(wrapper: _RunnerWrapper) -> RunnerSettings:
    """Exercise the copied file's editable-default normalization."""
    module = sys.modules["tested_runner_wrapper"]
    parser = cast(Callable[[list[str]], argparse.Namespace], module.__dict__["_arguments"])
    settings_factory = cast(Callable[[argparse.Namespace], RunnerSettings], module.__dict__["_settings"])
    return settings_factory(parser(["--no-pause"]))


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ("-pu1", (("-pu1",),)),
        (["-pu0", "-pu1"], (("-pu0",), ("-pu1",))),
        (("-pu0", "-pu1"), (("-pu0",), ("-pu1",))),
        ([["-pu0", "-pu1"], "-pu2"], (("-pu0", "-pu1"), ("-pu2",))),
        ((("-pu0", "-pu1"), ("-pu2",)), (("-pu0", "-pu1"), ("-pu2",))),
        ([], ()),
        ((), ()),
        (None, ()),
    ],
)
def test_editable_gpu_formats_normalize_to_scheduler_slots(
    wrapper: _RunnerWrapper,
    monkeypatch: pytest.MonkeyPatch,
    configured: object,
    expected: tuple[tuple[str, ...], ...],
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, "GPU_DEVICES", configured)
    settings = _settings_from_wrapper(wrapper)
    assert settings.gpu_devices == expected


@pytest.mark.parametrize(
    "configured",
    [
        "-puA",
        "-pu0,1",
        "-pu0 -pu1",
        "pu1",
        ["-pu0", "-pu0"],
        ["-pu0", "-pu00"],
        [["-pu0", "-pu0"]],
        [[], "-pu1"],
        2,
        [3],
    ],
)
def test_invalid_gpu_settings_fail_before_export(
    wrapper: _RunnerWrapper,
    monkeypatch: pytest.MonkeyPatch,
    configured: object,
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, "GPU_DEVICES", configured)
    monkeypatch.setattr(module, "RUN_VARIABLES_RAW", {"e1": "A"})
    monkeypatch.setattr(module, "TCF", "model_~e1~.tcf")
    assert wrapper.main(["--no-pause", "--export-only"]) == 1
    assert not Path(wrapper.__file__).with_suffix(".txt").exists()
    assert not Path(wrapper.__file__).with_name("run_tuflow_simulations_runs.csv").exists()


def test_editable_string_paths_and_single_input_source_normalize(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, "WORKING_DIR", str(tmp_path))
    monkeypatch.setattr(module, "TCF", "model_~e1~.tcf")
    monkeypatch.setattr(module, "TUFLOW_EXE", str(tmp_path / "TUFLOW_Synthetic.exe"))
    monkeypatch.setattr(module, "INPUT_FILES", "runs1.bat")
    settings = _settings_from_wrapper(wrapper)
    assert settings.working_directory == tmp_path
    assert settings.tcf == Path("model_~e1~.tcf")
    assert settings.executable == tmp_path / "TUFLOW_Synthetic.exe"
    assert settings.input_files == (Path("runs1.bat"),)
    monkeypatch.setattr(module, "INPUT_FILES", ["runs1.bat", Path("runs2.txt")])
    assert _settings_from_wrapper(wrapper).input_files == (
        Path("runs1.bat"),
        Path("runs2.txt"),
    )


@pytest.mark.parametrize(
    ("setting", "invalid"),
    [
        ("WORKING_DIR", "   "),
        ("TCF", ""),
        ("TUFLOW_EXE", ""),
        ("INPUT_FILES", [""]),
    ],
)
def test_blank_editable_paths_are_rejected(
    wrapper: _RunnerWrapper,
    monkeypatch: pytest.MonkeyPatch,
    setting: str,
    invalid: object,
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, setting, invalid)
    # A malformed user setting must identify the actual field, including
    # the index of an invalid BAT/TXT source.
    with pytest.raises(ValueError, match=rf"^{setting}(?:\[0\])? is blank"):
        _settings_from_wrapper(wrapper)


@pytest.mark.parametrize(
    ("setting", "invalid"),
    [
        ("MODE", "spelling_error"),
        ("OPERATION", "launch_something"),
        ("CONTINUATION_MODE", "some"),
        ("COMPUTATIONAL_PRIORITY", "MAXIMUM"),
        ("INPUT_FILES", ["a.csv"]),
        ("INPUT_FILES", [123]),
        ("RUN_VARIABLES_RAW", {"e1": ""}),
        ("RUN_VARIABLES_RAW", {"e1": [""]}),
        ("RUN_VARIABLES_RAW", {"e1": [1]}),
        ("RUN_VARIABLES_RAW", {"e1": "A", "E1": "B"}),
        ("RUN_VARIABLES_RAW", {"e0": "A"}),
        ("RUN_VARIABLES_RAW", "e1 A"),
        ("PRIORITY_ORDER", "e3 bad"),
        ("PRIORITY_ORDER", "e1 e1"),
        ("TCF", "not_a_tcf.txt"),
        ("TUFLOW_EXE", "not_an_exe.bat"),
        ("NEXT_RUN_FILE", "next_batch.txt"),
        ("BATCH_COMMANDS", ["-b"]),
        ("WAIT_TIME_AFTER_RUN", -1),
        ("LIVE_REFRESH_PER_SECOND", 0),
        ("LIVE_REFRESH_PER_SECOND", float("nan")),
        ("CHECK_INPUTS", "False"),
        ("MINIMIZE_ON_LAUNCH", 1),
        ("EXPORT_COMMANDS", "True"),
        ("CONSOLE_LOG_LEVEL", "verbosity"),
    ],
)
def test_invalid_configuration_is_rejected_before_any_output(
    tmp_path: Path,
    wrapper: _RunnerWrapper,
    monkeypatch: pytest.MonkeyPatch,
    setting: str,
    invalid: object,
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, setting, invalid)
    assert wrapper.main(["--no-pause"]) == 1
    assert not Path(wrapper.__file__).with_suffix(".txt").exists()
    assert not Path(wrapper.__file__).with_name("run_tuflow_simulations_runs.csv").exists()


def test_enum_defaults_and_raw_string_values_both_work(
    wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    op = module.__dict__["settings"].OP
    src = module.__dict__["settings"].SRC
    cont = module.__dict__["settings"].CONT
    prio = module.__dict__["settings"].PRIO
    normal = choice
    assert normal("OPERATION", op.RUN, op) is op.RUN
    assert normal("OPERATION", "run", op) is op.RUN
    assert normal("OPERATION", "RUN", op) is op.RUN
    assert normal("CONTINUATION_MODE", cont.RESUME, cont) is cont.RESUME
    assert normal("CONTINUATION_MODE", "resume", cont) is cont.RESUME
    assert normal("PRIORITY", prio.LOW, prio) is prio.LOW
    assert normal("PRIORITY", "low", prio) is prio.LOW
    monkeypatch.setattr(module, "MODE", src.PARAMETER_PRODUCT)
    monkeypatch.setattr(module, "COMPUTATIONAL_PRIORITY", prio.HIGH)
    settings = _settings_from_wrapper(wrapper)
    assert settings.mode == "parameter_product"
    assert settings.computational_priority == "HIGH"


def test_missing_optional_paths_warn_once_each_not_fail_planning(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    monkeypatch.setattr(module, "TCF", "runs/model_~e1~.tcf")
    monkeypatch.setattr(module, "TUFLOW_EXE", "TUFLOW_missing.exe")
    monkeypatch.setattr(module, "NEXT_RUN_FILE", "follow_up.py")
    monkeypatch.setattr(module, "RUN_VARIABLES_RAW", {"e1": ["A", "B"]})
    settings = _settings_from_wrapper(wrapper)
    plan = prepare_plan(settings)
    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING", format="{message}")
    check = cast(Callable[[CommandPlan, RunnerSettings], None], module.__dict__["_warn_optional_file_paths"])
    try:
        check(plan, settings)
        assert sum("TCF not found" in warning for warning in warnings) == 1
        assert sum("TUFLOW_EXE not found" in warning for warning in warnings) == 1
        assert sum("NEXT_RUN_FILE not found" in warning for warning in warnings) == 1
        (tmp_path / "runs").mkdir()
        (tmp_path / "runs/model_~e1~.tcf").write_text("synthetic", encoding="utf-8")
        (tmp_path / "TUFLOW_missing.exe").write_bytes(b"synthetic")
        (tmp_path / "follow_up.py").write_text("pass\n", encoding="utf-8")
        warnings.clear()
        check(plan, settings)
        assert warnings == []
    finally:
        logger.remove(sink)


def test_one_string_input_file_can_be_imported_without_a_python_tcf(
    tmp_path: Path, wrapper: _RunnerWrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["tested_runner_wrapper"]
    path = _source(tmp_path)
    monkeypatch.setattr(module, "MODE", "textfiles")
    monkeypatch.setattr(module, "TCF", None)
    monkeypatch.setattr(module, "INPUT_FILES", path.name)
    settings = _settings_from_wrapper(wrapper)
    assert settings.input_files == (Path(path.name),)
    plan = prepare_plan(settings)
    assert len(plan.runs) == 1
    assert plan.runs[0].tcf == tmp_path / "model.tcf"


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
    monkeypatch.setattr(module, "NEXT_RUN_FILE", "post.py")

    def fake_launch(plan: CommandPlan, *_args: object, **_kwargs: object) -> BatchOutcome:
        calls.append("launch")
        selected = plan.runs
        return BatchOutcome(
            selected,
            tuple(RunOutcome(run, 1, datetime.now(), datetime.now(), 123) for run in selected),
        )

    def post(path: Path, working_directory: Path) -> bool:
        assert path == Path("post.py")
        assert working_directory == tmp_path
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
