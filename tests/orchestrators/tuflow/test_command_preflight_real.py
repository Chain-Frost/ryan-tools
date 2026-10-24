"""Real installed-PyTUFLOW routing checks; no private inputs or TUFLOW solver."""

from pathlib import Path

from ryan_library.functions.tuflow.command_plan import read_command_plan
from ryan_library.orchestrators.tuflow.command_preflight import check_command_plan


def test_real_individual_preflight_checks_multiple_tcf_groups(tmp_path: Path) -> None:
    for name in ("first.tcf", "second.tcf"):
        (tmp_path / name).write_text("Tutorial Model == ON\nCell Size == 5\n", encoding="utf-8")
    source = tmp_path / "runs.bat"
    source.write_text("set TCF=first.tcf\n-e1 A\nset TCF=second.tcf\n-e1 B\nset TCF=first.tcf\n-e1 A", encoding="utf-8")
    report = check_command_plan(read_command_plan(source, initial_directory=tmp_path))
    assert report.passed, report.issues
    assert report.groups_checked == 2
    assert report.completed_runs == 3
    assert [run.index for run in report.plan.runs] == [1, 2, 3]


def test_real_missing_tcf_does_not_hide_other_runs(tmp_path: Path) -> None:
    (tmp_path / "present.tcf").write_text("Tutorial Model == ON\nCell Size == 5\n", encoding="utf-8")
    source = tmp_path / "runs.bat"
    source.write_text("-e1 A absent.tcf\n-e1 B present.tcf", encoding="utf-8")
    report = check_command_plan(read_command_plan(source, initial_directory=tmp_path))
    assert not report.passed
    assert report.completed_runs == 1
    assert [issue.run.index for issue in report.issues] == [1]
