"""Group routing and source provenance without executing the model or shell."""

import csv
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import NoReturn

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlan, CommandPlanError, PlannedRun, parse_command_plan
from ryan_library.orchestrators.tuflow import command_preflight as subject


def plan(tmp_path: Path, text: str = "-e1 A a.tcf\n-e1 B b.tcf\n-e1 A a.tcf") -> CommandPlan:
    return parse_command_plan(text, source=tmp_path / "runs.bat", initial_directory=tmp_path)


@dataclass
class FakeIssue:
    run_index: int
    status: str = "missing"
    origin: str = "test"
    reference: str = "file"
    detail: str = "missing"


@dataclass
class FakeReport:
    run_count: int
    issues: Sequence[FakeIssue] = ()
    checked_paths: set[str] = field(default_factory=lambda: {"shared"})
    parse_seconds: float = 1.0
    context_seconds: float = 2.0
    inventory_seconds: float = 3.0
    passed_override: bool | None = None

    @property
    def passed(self) -> bool:
        return self.passed_override if self.passed_override is not None else not self.issues


def outcome(runs: tuple[PlannedRun, ...], issues: Sequence[FakeIssue] = ()) -> FakeReport:
    return FakeReport(len(runs), issues)


def test_interleaved_tcf_groups_preserve_global_indices_duplicates_and_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[tuple[PlannedRun, ...], dict[str, object]]] = []

    def checker(runs: tuple[PlannedRun, ...], **options: object) -> FakeReport:
        calls.append((runs, options))
        return outcome(runs)

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path), io_workers=3, check_gpkg_layers=False)
    assert report.passed
    assert report.run_count == report.completed_runs == 3
    assert [[run.index for run in runs] for runs, _ in calls] == [[1, 3], [2]]
    assert all(
        options == {"check_gpkg_layers": False, "io_workers": 3, "maximum_directory_entries": 50_000}
        for _, options in calls
    )
    assert report.groups_checked == 2
    assert report.parse_seconds == 2.0
    assert report.context_seconds == 4.0
    assert report.inventory_seconds == 6.0
    assert report.checked_paths == {"shared"}


def test_one_failed_model_invalidates_its_runs_and_does_not_drop_other_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def checker(runs: tuple[PlannedRun, ...], **_options: object) -> FakeReport:
        if runs[0].tcf.name == "a.tcf":
            message = "missing model"
            raise FileNotFoundError(message)
        return outcome(runs)

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path))
    assert not report.passed
    assert report.completed_runs == 1
    assert [issue.run.index for issue in report.issues] == [1, 3]
    assert all(issue.status == "unresolved" for issue in report.issues)


def test_issues_csv_contains_original_line_model_and_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def checker(runs: tuple[PlannedRun, ...], **_options: object) -> FakeReport:
        issues = [FakeIssue(run.index) for run in reversed(runs)]
        return outcome(runs, issues)

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path))
    assert not report.passed
    assert report.affected_runs == 3
    target = report.write_csv(tmp_path / "report.csv")
    with target.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert [row["run_index"] for row in rows] == ["1", "2", "3"]
    assert [row["line_start"] for row in rows] == ["1", "2", "3"]
    assert rows[2]["tcf"].endswith("a.tcf")
    before = target.read_bytes()
    with pytest.raises(FileExistsError):
        report.write_csv(target)
    assert target.read_bytes() == before


@pytest.mark.parametrize(("bad_count", "bad_index"), [(1, None), (2, 99)])
def test_malformed_group_reports_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_count: int, bad_index: int | None
) -> None:
    def checker(runs: tuple[PlannedRun, ...], **_options: object) -> FakeReport:
        result = outcome(runs)
        result.run_count = bad_count
        if bad_index is not None:
            result.issues = [FakeIssue(bad_index)]
        return result

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path, "-e1 A a.tcf\n-e1 B a.tcf"))
    assert not report.passed
    assert len(report.issues) == 2
    assert report.completed_runs == 0


def test_missing_cd_destination_rejects_plan_before_checking(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> NoReturn:
        pytest.fail("model check must not run")

    monkeypatch.setattr(subject, "_check_group", forbidden)
    with pytest.raises(CommandPlanError, match="directory is unavailable"):
        subject.check_command_plan(plan(tmp_path, "cd absent\n-e1 A model.tcf"))


def test_invalid_limits_fail_before_checking(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="io_workers"):
        subject.check_command_plan(plan(tmp_path), io_workers=0)
    with pytest.raises(ValueError, match="maximum_directory_entries"):
        subject.check_command_plan(plan(tmp_path), maximum_directory_entries=0)


def test_interrupt_not_converted_to_passing_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(*_args: object, **_kwargs: object) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(subject, "_check_group", interrupt)
    with pytest.raises(KeyboardInterrupt):
        subject.check_command_plan(plan(tmp_path))


def test_groups_preserve_source_attribution_and_forward_bounds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[PlannedRun, ...], dict[str, object]]] = []

    def checker(runs: tuple[PlannedRun, ...], **options: object) -> FakeReport:
        calls.append((runs, options))
        return outcome(runs, [FakeIssue(runs[-1].index)])

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path), io_workers=2, maximum_directory_entries=200)
    assert [[run.index for run in runs] for runs, _ in calls] == [[1, 3], [2]]
    assert all(
        options == {"check_gpkg_layers": True, "io_workers": 2, "maximum_directory_entries": 200}
        for _, options in calls
    )
    assert report.completed_runs == 3
    assert [issue.run.index for issue in report.issues] == [2, 3]
    assert not report.passed


@pytest.mark.parametrize("workers", [0, 33])
def test_invalid_file_workers_fail_before_group_checks(tmp_path: Path, workers: int) -> None:
    with pytest.raises(ValueError, match="io_workers"):
        subject.check_command_plan(plan(tmp_path), io_workers=workers)


def test_scope_01_global_findings_map_to_interleaved_occurrences_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def checker(runs: tuple[PlannedRun, ...], **_options: object) -> FakeReport:
        # Scope 01 has already translated upstream local positions into the
        # global IDs it received from this layer's RunRequest list.
        return outcome(runs, [FakeIssue(runs[0].index, reference="first"), FakeIssue(runs[-1].index, reference="last")])

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path))
    assert [issue.run.index for issue in report.issues] == [1, 2, 2, 3]
    assert [issue.run.source for issue in report.issues] == [tmp_path / "runs.bat"] * 4


def test_incomplete_group_with_no_findings_never_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def checker(runs: tuple[PlannedRun, ...], **_options: object) -> FakeReport:
        result = outcome(runs)
        result.passed_override = False
        return result

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path))
    assert not report.passed
    assert report.completed_runs == 0
    assert report.affected_runs == 3
    assert all(issue.status == "unresolved" for issue in report.issues)


def test_directory_failure_identifies_actual_second_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.functions.tuflow.command_plan import read_command_plans

    first, second = tmp_path / "first.bat", tmp_path / "second.bat"
    first.write_text("TUFLOW.exe one.tcf", encoding="utf-8")
    second.write_text("cd unavailable\\nested\nTUFLOW.exe two.tcf", encoding="utf-8")
    both = read_command_plans((first, second), initial_directory=tmp_path)

    def forbidden(*_args: object, **_kwargs: object) -> NoReturn:
        pytest.fail("A missing required directory must prevent all validation")

    monkeypatch.setattr(subject, "_check_group", forbidden)
    with pytest.raises(CommandPlanError, match=r"second\.bat:1"):
        subject.check_command_plan(both)


def test_validator_seam_preserves_global_ids_and_spaced_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scope 01 owns the only conversion of PyTUFLOW's local positions."""
    @dataclass(frozen=True)
    class PublicRunRequest:
        index: int
        flags: Mapping[str, str]

    captured: list[object] = []

    def fake_validator(tcf: Path, requests: list[PublicRunRequest], **options: object) -> FakeReport:
        captured.append(tcf)
        captured.append(tuple((item.index, dict(item.flags)) for item in requests))
        captured.append(options)
        return FakeReport(run_count=len(requests), issues=[FakeIssue(requests[-1].index)])

    fake_module = ModuleType("ryan_library.orchestrators.tuflow.input_preflight")
    fake_module.__dict__["RunRequest"] = PublicRunRequest
    fake_module.__dict__["check_simulation_inputs"] = fake_validator
    monkeypatch.setitem(sys.modules, fake_module.__name__, fake_module)

    selected = plan(
        tmp_path,
        'TUFLOW.exe -e1 "Wet Season" a.tcf\n'
        'TUFLOW.exe -e1 Other b.tcf\n'
        'TUFLOW.exe -e1 "Wet Season" a.tcf',
    )
    result = subject._check_group(
        (selected.runs[0], selected.runs[2]),
        check_gpkg_layers=False,
        io_workers=2,
        maximum_directory_entries=200,
    )
    assert result.run_count == 2
    assert result.issues[0].run_index == 3
    assert captured[0] == tmp_path / "a.tcf"
    assert captured[1] == ((1, {"e1": "Wet Season"}), (3, {"e1": "Wet Season"}))
    assert captured[2] == {
        "check_gpkg_layers": False,
        "io_workers": 2,
        "maximum_directory_entries": 200,
    }
