"""Launch logs align changing flags first and group constant values together."""

from __future__ import annotations

from pathlib import Path

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.simulation_runner import (
    _launch_flag_layout,  # pyright: ignore[reportPrivateUsage] - exercise internal launch formatting
    _launch_flag_text,  # pyright: ignore[reportPrivateUsage] - exercise internal launch formatting
)


def _run(index: int, pairs: tuple[tuple[str, str], ...]) -> PlannedRun:
    tcf = Path("runs/model_~e1~.tcf")
    return PlannedRun(
        index,
        Path("runner.py"),
        0,
        0,
        tcf,
        Path(),
        pairs,
        ("-b", str(tcf)),
        "TUFLOW.exe",
    )


def test_launch_flag_order_and_alignment() -> None:
    first = _run(1, (("e3", "TP01"), ("s2", "M1B"), ("e2", "00120m"), ("e1", "01.0p"), ("s1", "EXG")))
    second = _run(2, (("s2", "Coondiner"), ("e1", "02.0p"), ("s1", "EXG"), ("e2", "00120m"), ("e3", "TP02")))
    plan = CommandPlan(Path("runner.py"), (first, second), (), 0, "commands", (Path("runner.py"),))
    layout = _launch_flag_layout(plan)

    assert [flag for flag, _, _ in layout] == ["e1", "e2", "e3", "s1", "s2"]
    changing1, constants1 = _launch_flag_text(first, layout)
    changing2, constants2 = _launch_flag_text(second, layout)
    assert changing1.index("-e1") < changing1.index("-e3") < changing1.index("-s2")
    assert changing1.index("-s2") == changing2.index("-s2")
    assert "M1B" in changing1
    assert "Coondiner" in changing2
    assert constants1 == constants2 == "-e2 00120m  -s1 EXG"


def test_missing_scenario_preserves_column() -> None:
    first = _run(1, (("e1", "01.0p"), ("s1", "EXG"), ("s2", "M1B")))
    second = _run(2, (("e1", "01.0p"), ("s2", "M3W")))
    plan = CommandPlan(Path("runner.py"), (first, second), (), 0, "commands", (Path("runner.py"),))
    layout = _launch_flag_layout(plan)
    first_changing, first_constants = _launch_flag_text(first, layout)
    second_changing, second_constants = _launch_flag_text(second, layout)
    assert first_changing.index("-s2") == second_changing.index("-s2")
    assert "-s1" in first_changing
    assert "-s1" not in second_changing
    assert first_constants == second_constants == "-e1 01.0p"


def test_constant_events_follow_changing_flags() -> None:
    first = _run(1, (("e1", "01.0p"), ("e4", "EnsembleILCL"), ("e5", "CCNear"), ("s1", "EXG")))
    second = _run(2, (("s1", "DEV"), ("e5", "CCNear"), ("e4", "EnsembleILCL"), ("e1", "02.0p")))
    plan = CommandPlan(Path("runner.py"), (first, second), (), 0, "commands", (Path("runner.py"),))
    layout = _launch_flag_layout(plan)
    first_changing, first_constants = _launch_flag_text(first, layout)
    second_changing, second_constants = _launch_flag_text(second, layout)

    assert first_changing == "-e1 01.0p  -s1 EXG"
    assert second_changing == "-e1 02.0p  -s1 DEV"
    assert first_constants == second_constants == "-e4 EnsembleILCL  -e5 CCNear"


def test_single_run_has_only_constant_flags() -> None:
    run = _run(1, (("e1", "01.0p"), ("s1", "EXG")))
    plan = CommandPlan(Path("runner.py"), (run,), (), 0, "commands", (Path("runner.py"),))
    assert _launch_flag_text(run, _launch_flag_layout(plan)) == ("", "-e1 01.0p  -s1 EXG")
