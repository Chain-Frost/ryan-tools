"""Group routing and source provenance without executing the model or shell."""

import csv
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
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
    csv_headers_checked: int = 0
    csv_column_selections_checked: int = 0
    dependency_fragment_evaluations: int = 0
    dependency_fragment_reuses: int = 0
    dependency_fallback_runs: int = 0


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
    report = subject.check_command_plan(plan(tmp_path), context_mode="batch", check_gpkg_layers=False)
    assert report.passed
    assert report.run_count == report.completed_runs == 3
    assert [[run.index for run in runs] for runs, _ in calls] == [[1, 3], [2]]
    assert all(options == {"context_mode": "batch", "check_gpkg_layers": False} for _, options in calls)
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


def test_invalid_context_mode_fails_before_checking(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="context mode"):
        subject.check_command_plan(plan(tmp_path), context_mode="invalid")  # pyright: ignore[reportArgumentType] - deliberate invalid mode


def test_interrupt_not_converted_to_passing_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(*_args: object, **_kwargs: object) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(subject, "_check_group", interrupt)
    with pytest.raises(KeyboardInterrupt):
        subject.check_command_plan(plan(tmp_path))


def test_file_only_checks_each_model_and_preserves_source_attribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[tuple[PlannedRun, ...], dict[str, object]]] = []

    def checker(runs: tuple[PlannedRun, ...], **options: object) -> FakeReport:
        calls.append((runs, options))
        report = outcome(runs, [FakeIssue(runs[-1].index)])
        report.csv_headers_checked = 1
        report.csv_column_selections_checked = len(runs)
        return report

    monkeypatch.setattr(subject, "_check_group", checker)
    report = subject.check_command_plan(plan(tmp_path), files_only=True, io_workers=2)
    assert [[run.index for run in runs] for runs, _ in calls] == [[1, 3], [2]]
    assert all(options["files_only"] is True and options["io_workers"] == 2 for _, options in calls)
    assert report.completed_runs == 3
    assert report.csv_headers_checked == 2
    assert report.csv_column_selections_checked == 3
    assert [issue.run.index for issue in report.issues] == [2, 3]
    assert not report.passed


@pytest.mark.parametrize("workers", [0, 33])
def test_invalid_file_workers_fail_before_group_checks(tmp_path: Path, workers: int) -> None:
    with pytest.raises(ValueError, match="io_workers"):
        subject.check_command_plan(plan(tmp_path), files_only=True, io_workers=workers)
