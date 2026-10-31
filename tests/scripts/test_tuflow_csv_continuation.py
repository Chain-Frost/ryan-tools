"""Wrapper-level CSV continuation tests with fake subprocess observations."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Protocol, cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.simulation_runner import BatchOutcome, RunOutcome, RunnerSettings


class _Wrapper(Protocol):
    __file__: str
    EXPORT_COMMANDS: bool
    NEXT_RUN_FILE: Path | None
    RUN_VARIABLES_RAW: dict[str, str | list[str]]
    TCF: Path | None

    def main(self, argv: list[str] | None = None) -> int: ...


@pytest.fixture
def copied_wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Wrapper:
    source = (
        Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py"
    )
    folder = tmp_path / "wrapper"
    folder.mkdir()
    target = folder / "my_batch.py"
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("csv_runner_wrapper", target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "EXPORT_COMMANDS", False)
    monkeypatch.setattr(module, "RUN_VARIABLES_RAW", {"e1": "ONE TWO"})
    monkeypatch.setattr(module, "TCF", Path("model.tcf"))
    monkeypatch.setattr(module, "USE_LIVE_DASHBOARD", False)
    monkeypatch.setattr(module, "WAIT_TIME_AFTER_RUN", 0)
    return cast(_Wrapper, module)


def _args(tmp_path: Path, *extra: str) -> list[str]:
    work = tmp_path / "different-model-directory"
    work.mkdir(exist_ok=True)
    return ["--working-directory", str(work), "--no-export", "--no-pause", *extra]


def _fake_successful_launch(calls: list[list[int]]) -> Callable[..., BatchOutcome]:
    def launch(
        plan: CommandPlan,
        _settings: RunnerSettings,
        *,
        on_before_launch: Callable[[PlannedRun], None],
        on_observed_completion: Callable[[RunOutcome], None],
        **_kwargs: object,
    ) -> BatchOutcome:
        calls.append([run.index for run in plan.runs])
        observations: list[RunOutcome] = []
        for run in plan.runs:
            on_before_launch(run)
            now = datetime.now()
            result = RunOutcome(run, 0, now, now, 42)
            on_observed_completion(result)
            observations.append(result)
        return BatchOutcome(plan.runs, tuple(observations))

    return launch


def test_copied_wrapper_resume_no_work_all_reset_and_follow_up(
    tmp_path: Path, copied_wrapper: _Wrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["csv_runner_wrapper"]
    calls: list[list[int]] = []
    chained: list[Path] = []
    monkeypatch.setattr(module, "launch_plan", _fake_successful_launch(calls))
    monkeypatch.setattr(module, "NEXT_RUN_FILE", Path("follow-up.py"))
    monkeypatch.setattr(module, "_run_follow_up", lambda path, _base: chained.append(path) or True)

    assert copied_wrapper.main(_args(tmp_path)) == 0
    log = Path(copied_wrapper.__file__).with_name("my_batch_runs.csv")
    assert log.is_file() and log.parent.name == "wrapper"
    assert not (tmp_path / "different-model-directory" / log.name).exists()
    first_history = log.read_bytes()
    assert calls == [[1, 2]]

    monkeypatch.setattr(module, "launch_plan", lambda *_a, **_k: pytest.fail("No-work Resume launched"))
    from ryan_library.functions.tuflow import gpu_occupancy

    monkeypatch.setattr(gpu_occupancy, "local_gpu_occupancy", lambda: pytest.fail("No-work Resume scanned GPU"))
    assert copied_wrapper.main(_args(tmp_path)) == 0
    assert log.read_bytes() == first_history
    assert calls == [[1, 2]]
    assert len(chained) == 2  # Chaining policy applies to successful no-work Resume.

    monkeypatch.setattr(module, "launch_plan", _fake_successful_launch(calls))
    assert copied_wrapper.main(_args(tmp_path, "--run-mode", "all")) == 0
    assert calls == [[1, 2], [1, 2]]
    assert b",RESET," in log.read_bytes()
    after_all = log.read_bytes()
    monkeypatch.setattr(module, "launch_plan", lambda *_a, **_k: pytest.fail("Post-all Resume launched"))
    assert copied_wrapper.main(_args(tmp_path)) == 0
    assert log.read_bytes() == after_all


def test_export_and_configuration_failures_do_not_change_csv(
    tmp_path: Path, copied_wrapper: _Wrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["csv_runner_wrapper"]
    calls: list[list[int]] = []
    monkeypatch.setattr(module, "launch_plan", _fake_successful_launch(calls))
    assert copied_wrapper.main(_args(tmp_path)) == 0
    path = Path(copied_wrapper.__file__).with_name("my_batch_runs.csv")
    history = path.read_bytes()

    assert copied_wrapper.main(_args(tmp_path, "--export-only")) == 0
    assert path.read_bytes() == history

    def refuse_export(*_args: object, **_kwargs: object) -> Path:
        raise FileExistsError("Synthetic export failure")

    monkeypatch.setattr(module, "EXPORT_COMMANDS", True)
    monkeypatch.setattr(module, "export_batch_commands", refuse_export)
    assert copied_wrapper.main(_args(tmp_path, "--run-mode", "all", "--export")) == 1
    assert path.read_bytes() == history

    monkeypatch.setattr(module, "EXPORT_COMMANDS", False)
    missing = tmp_path / "missing.bat"
    assert copied_wrapper.main(_args(tmp_path, "--run-mode", "all", "--mode", "textfiles",
                                    "--no-tcf", "--input-file", str(missing))) == 1
    assert path.read_bytes() == history


def test_definitive_launch_failure_is_logged_and_requeued(
    tmp_path: Path, copied_wrapper: _Wrapper, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = sys.modules["csv_runner_wrapper"]
    observed: list[list[int]] = []

    def failed(
        plan: CommandPlan,
        _settings: RunnerSettings,
        *,
        on_before_launch: Callable[[PlannedRun], None],
        on_observed_completion: Callable[[RunOutcome], None],
        **_kwargs: object,
    ) -> BatchOutcome:
        observed.append([run.index for run in plan.runs])
        records = []
        for run in plan.runs:
            on_before_launch(run)
            result = RunOutcome(run, None, datetime.now(), datetime.now(), None, "Launch failed: mock solver")
            on_observed_completion(result)
            records.append(result)
        return BatchOutcome(plan.runs, tuple(records))

    monkeypatch.setattr(module, "launch_plan", failed)
    assert copied_wrapper.main(_args(tmp_path)) == 1
    assert copied_wrapper.main(_args(tmp_path)) == 1
    assert observed == [[1, 2], [1, 2]]
    content = Path(copied_wrapper.__file__).with_name("my_batch_runs.csv").read_text(encoding="utf-8")
    assert content.count(",LAUNCH_ERROR,") == 4
