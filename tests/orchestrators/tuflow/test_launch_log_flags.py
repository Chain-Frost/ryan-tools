"""Launch log formatting must keep scenarios aligned and less prominent."""

from __future__ import annotations

from pathlib import Path

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.simulation_runner import _launch_flag_layout, _launch_flag_text


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

    assert [flag for flag, _ in layout] == ["e1", "e2", "e3", "s1", "s2"]
    events1, scenarios1 = _launch_flag_text(first, layout)
    events2, scenarios2 = _launch_flag_text(second, layout)
    assert events1.index("-e1") < events1.index("-e2") < events1.index("-e3")
    assert events1.index("-e2") == events2.index("-e2")
    assert scenarios1.index("-s2") == scenarios2.index("-s2")
    assert "M1B" in scenarios1
    assert "Coondiner" in scenarios2


def test_missing_scenario_preserves_column() -> None:
    first = _run(1, (("e1", "01.0p"), ("s1", "EXG"), ("s2", "M1B")))
    second = _run(2, (("e1", "01.0p"), ("s2", "M3W")))
    plan = CommandPlan(Path("runner.py"), (first, second), (), 0, "commands", (Path("runner.py"),))
    layout = _launch_flag_layout(plan)
    _, first_scenarios = _launch_flag_text(first, layout)
    _, second_scenarios = _launch_flag_text(second, layout)
    assert first_scenarios.index("-s2") == second_scenarios.index("-s2")
