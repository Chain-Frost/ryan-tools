"""Synthetic acceptance for the single-file TUFLOW continuation log.

No TUFLOW executable, external process inventory or model data is accessed.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
from datetime import datetime, tzinfo
from pathlib import Path, PureWindowsPath
import subprocess

import pytest

import ryan_library.functions.tuflow.execution_history as execution_history
from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun, read_command_plan
from ryan_library.functions.tuflow.execution_history import (
    COLUMNS,
    CompletionSession,
    CsvExecutionLog,
    ExecutionLogError,
    RunIdentity,
    identify_runs,
    select_pending,
)


@dataclass
class Observed:
    run: PlannedRun
    return_code: int | None
    pid: int | None = 123
    error: str | None = None


def _plan(base: Path, count: int = 3) -> CommandPlan:
    tcf = base / "models" / "effective.tcf"
    runs = tuple(
        PlannedRun(
            index=i,
            source=base / "batch.py",
            line_start=i,
            line_end=i,
            tcf=tcf,
            working_directory=base / "work",
            flag_pairs=(("e1", f"event{i}"),),
            arguments=("-b", "-e1", f"event{i}", str(tcf)),
            executable=r"C:\TUFLOW\TUFLOW_iSP_w64.exe",
        )
        for i in range(1, count + 1)
    )
    return CommandPlan(base / "batch.py", runs, (), 0, "commands", (base / "batch.py",))


def _success(log: CsvExecutionLog, plan: CommandPlan, identities: dict[int, RunIdentity]) -> None:
    # Full callback contract is covered in the integration test; here use the
    # public history methods directly for concise transition-state fixtures.
    for run in plan.runs:
        identity = identities[run.index]
        attempt = log.start(identity)
        log.finish(identity, attempt, Observed(run, 0))


def test_csv_recorded_at_uses_local_computer_timezone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    local_time = datetime(2026, 11, 3, 14, 25, 30, 123456)
    expected = local_time.astimezone().isoformat(timespec="microseconds")

    class LocalDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> LocalDateTime:
            assert tz is None
            return cls(2026, 11, 3, 14, 25, 30, 123456)

    monkeypatch.setattr(execution_history, "datetime", LocalDateTime)
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.start(identity)

    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    assert rows[0]["recorded_at"] == expected


def test_new_csv_prioritizes_operator_facing_columns(tmp_path: Path) -> None:
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path):
        pass

    with path.open(encoding="utf-8", newline="") as source:
        header = next(csv.reader(source))
    assert header == [
        "recorded_at",
        "event",
        "outcome",
        "return_code",
        "pid",
        "error",
        "arguments",
        "tcf",
        "working_directory",
        "executable",
        "run_key",
        "attempt_id",
        "schema_version",
    ]


def test_existing_operator_column_order_is_still_appendable_without_rewriting_history(tmp_path: Path) -> None:
    previous_columns = (
        "recorded_at",
        "event",
        "outcome",
        "return_code",
        "tcf",
        "pid",
        "error",
        "arguments",
        "working_directory",
        "executable",
        "run_key",
        "attempt_id",
        "schema_version",
    )
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.finish(identity, log.start(identity), Observed(plan.runs[0], 0))
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer: csv.DictWriter[str] = csv.DictWriter(stream, fieldnames=previous_columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    original = path.read_bytes()
    with CsvExecutionLog(path) as log:
        assert log.successful_keys() == {identity.key}
        log.start(identity)
    assert path.read_bytes().startswith(original)
    with path.open(encoding="utf-8", newline="") as stream:
        assert tuple(next(csv.reader(stream))) == previous_columns


def test_history_uses_relative_windows_tcf_and_command_without_changing_execution_plan(tmp_path: Path) -> None:
    base = PureWindowsPath(r"Q:\26\RP26188 HD4 TO HD1 PIPELINE - UON\TUFLOW_HD14Pipeline")
    tcf = base / "runs" / "UON_04" / "_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf"
    flags = (
        ("e3", "TP02"),
        ("e2", "00540m"),
        ("e1", "00.5p"),
        ("e4", "EnsembleILCL"),
        ("e5", "CCNear"),
        ("s1", "EXG"),
        ("s2", "M3W"),
        ("s4", "16M"),
    )
    model_args = tuple(arg for key, value in flags for arg in (f"-{key}", value))
    original_args = ("-b", *model_args, str(tcf))
    run = PlannedRun(
        index=1,
        source=base / "run_tuflow.py",
        line_start=1,
        line_end=1,
        tcf=tcf,
        working_directory=base,
        flag_pairs=flags,
        arguments=original_args,
        executable=r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe",
    )
    plan = CommandPlan(base / "run_tuflow.py", (run,), (), 0, "commands")
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    expected_tcf = r"runs\UON_04\_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf"
    assert identity.tcf == expected_tcf
    assert identity.arguments == subprocess.list2cmdline(["-b", *model_args, expected_tcf])
    assert str(tcf) not in identity.arguments
    assert identity.working_directory == str(base)
    assert run.arguments == original_args  # The actual subprocess command stays absolute.
    key = identity.key

    path = tmp_path / "run_tuflow_runs.csv"
    with CsvExecutionLog(path) as log:
        log.finish(identity, log.start(identity), Observed(run, 1, pid=2340))
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == list(COLUMNS)
        assert reader.fieldnames.index("arguments") < reader.fieldnames.index("tcf")
        rows = list(reader)
    assert [row["event"] for row in rows] == ["START", "FINISH"]
    assert rows[-1]["outcome"] == "FAILED"
    assert rows[-1]["return_code"] == "1"
    assert rows[-1]["pid"] == "2340"
    assert rows[-1]["arguments"] == identity.arguments
    assert rows[-1]["tcf"] == expected_tcf
    assert rows[-1]["run_key"] == key
    with CsvExecutionLog(path) as log:
        assert key not in log.successful_keys()


def test_relative_command_only_rewrites_the_explicit_tcf_token(tmp_path: Path) -> None:
    base = PureWindowsPath(r"Q:\model")
    tcf = base / "runs" / "model.tcf"
    flag_value = str(tcf)
    run = PlannedRun(
        index=1,
        source=base / "runs.txt",
        line_start=1,
        line_end=1,
        tcf=tcf,
        working_directory=base,
        flag_pairs=(("e1", flag_value),),
        arguments=(str(tcf), "-e1", flag_value, "-b"),
        executable=r"C:\TUFLOW\solver.exe",
    )
    plan = CommandPlan(base / "runs.txt", (run,), (), 0, "commands")
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    assert identity.tcf == r"runs\model.tcf"
    assert identity.arguments == subprocess.list2cmdline([r"runs\model.tcf", "-e1", flag_value, "-b"])

    no_explicit = replace(run, arguments=("-e1", flag_value, "-b"))
    plan_without = replace(plan, runs=(no_explicit,))
    bare = identify_runs(plan_without, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    assert bare.tcf == identity.tcf
    assert bare.arguments == subprocess.list2cmdline(list(no_explicit.arguments))


@pytest.mark.parametrize(
    ("working_directory", "tcf", "expected"),
    [
        (
            PureWindowsPath(r"Q:\Project"),
            PureWindowsPath(r"Q:\Project\runs\model.tcf"),
            r"runs\model.tcf",
        ),
        (
            PureWindowsPath(r"q:\project"),
            PureWindowsPath(r"Q:\PROJECT\runs\model.tcf"),
            r"runs\model.tcf",
        ),
        (
            PureWindowsPath(r"\\server\share\model"),
            PureWindowsPath(r"\\server\share\model\runs\case.tcf"),
            r"runs\case.tcf",
        ),
        (
            PureWindowsPath(r"Q:\Project"),
            PureWindowsPath(r"R:\Project\runs\model.tcf"),
            r"R:\Project\runs\model.tcf",
        ),
        (
            PureWindowsPath(r"Q:\Project"),
            PureWindowsPath(r"Q:\AnotherProject\runs\model.tcf"),
            r"Q:\AnotherProject\runs\model.tcf",
        ),
    ],
)
def test_windows_tcf_display_respects_model_containment(
    tmp_path: Path, working_directory: PureWindowsPath, tcf: PureWindowsPath, expected: str
) -> None:
    run = PlannedRun(
        index=1,
        source=working_directory / "run.bat",
        line_start=1,
        line_end=1,
        tcf=tcf,
        working_directory=working_directory,
        flag_pairs=(),
        arguments=("-b", str(tcf)),
        executable=r"C:\TUFLOW\solver.exe",
    )
    plan = CommandPlan(working_directory / "run.bat", (run,), (), 0, "commands")
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    assert identity.tcf == expected
    assert identity.arguments == subprocess.list2cmdline(["-b", expected])
    assert str(tcf) == run.arguments[-1]


def test_posix_tcf_display_is_relative_when_inside_working_directory(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    assert identity.tcf == str(plan.runs[0].tcf)
    inside = replace(plan.runs[0], working_directory=tmp_path)
    identity_inside = identify_runs(
        replace(plan, runs=(inside,)), fallback_executable=Path("unused.exe"), base_directory=tmp_path
    )[1]
    assert identity_inside.tcf == str(Path("models") / "effective.tcf")
    assert identity_inside.arguments == subprocess.list2cmdline(
        ["-b", "-e1", "event1", str(Path("models") / "effective.tcf")]
    )


def test_existing_internal_first_column_order_remains_appendable(tmp_path: Path) -> None:
    legacy_columns = (
        "schema_version",
        "event",
        "recorded_at",
        "run_key",
        "attempt_id",
        "outcome",
        "return_code",
        "tcf",
        "working_directory",
        "executable",
        "arguments",
        "pid",
        "error",
    )
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.finish(identity, log.start(identity), Observed(plan.runs[0], 0))
    with path.open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer: csv.DictWriter[str] = csv.DictWriter(destination, fieldnames=legacy_columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    with CsvExecutionLog(path) as log:
        assert log.successful_keys() == {identity.key}
        log.start(identity)
    with path.open(encoding="utf-8", newline="") as source:
        assert tuple(next(csv.reader(source))) == legacy_columns


def test_400_run_resume_queues_300_in_original_order(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 400)
    identities = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        for run in plan.runs[:100]:
            identity = identities[run.index]
            attempt = log.start(identity)
            log.finish(identity, attempt, Observed(run, 0))
        for run in plan.runs[100:103]:
            identity = identities[run.index]
            attempt = log.start(identity)
            log.finish(identity, attempt, Observed(run, 2))
        for run in plan.runs[103:108]:
            log.start(identities[run.index])  # Abrupt interruption: no FINISH.
    with CsvExecutionLog(path) as log:
        pending = select_pending(plan, identities, log.successful_keys())
    assert len(pending.runs) == 300
    assert [r.index for r in pending.runs] == list(range(101, 401))


def test_out_of_order_finishes_persist_before_earlier_process(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 2)
    keys = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        session = CompletionSession(log, keys)
        first, second = plan.runs
        session.before_launch(first)
        session.before_launch(second)
        session.observed_completion(Observed(second, 0))
        with path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
        assert [r["event"] for r in rows] == ["START", "START", "FINISH"]
        assert rows[-1]["run_key"] == keys[second.index].key
        assert log.successful_keys() == {keys[second.index].key}
        session.observed_completion(Observed(first, 0))


def test_latest_attempt_overrides_older_late_finish_and_duplicate_occurrences(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 2)
    second = replace(plan.runs[1], flag_pairs=plan.runs[0].flag_pairs, arguments=plan.runs[0].arguments)
    plan = replace(plan, runs=(plan.runs[0], second))
    identities = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)
    first_key, second_key = (identities[run.index].key for run in plan.runs)
    assert first_key != second_key
    with CsvExecutionLog(tmp_path / "log.csv") as log:
        first = identities[1]
        older = log.start(first)
        newer = log.start(first)
        log.finish(first, older, Observed(plan.runs[0], 0))
        assert first_key not in log.successful_keys()
        log.finish(first, newer, Observed(plan.runs[0], 4))
        log.finish(identities[2], log.start(identities[2]), Observed(plan.runs[1], 0))
        assert log.successful_keys() == {second_key}
        latest = log.start(first)
        assert first_key not in log.successful_keys()
        log.finish(first, latest, Observed(plan.runs[0], 0))
        assert log.successful_keys() == {first_key, second_key}
        latest = log.start(first)  # A later unclosed START supersedes success.
        assert latest
        assert log.successful_keys() == {second_key}


def test_interrupted_all_resets_old_success_baseline(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    keys = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        _success(log, plan, keys)
        assert not select_pending(plan, keys, log.successful_keys()).runs
        log.reset()
        first = plan.runs[0]
        log.finish(keys[1], log.start(keys[1]), Observed(first, 0))
        log.start(keys[2])
    with CsvExecutionLog(path) as log:
        pending = select_pending(plan, keys, log.successful_keys())
        assert [run.index for run in pending.runs] == [2, 3]
        assert sum(row["event"] == "RESET" for row in csv.DictReader(path.open(encoding="utf-8"))) == 1


@pytest.mark.parametrize("tail", [b"1,START,broken", b"1,FINISH,broken", b"1,RESET,broken"])
def test_torn_last_record_is_removed_without_losing_valid_history(tmp_path: Path, tail: bytes) -> None:
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.finish(identity, log.start(identity), Observed(plan.runs[0], 0))
    intact = path.read_bytes()
    with path.open("ab") as output:
        output.write(tail)
    with CsvExecutionLog(path) as log:
        assert log.successful_keys() == {identity.key}
        assert path.read_bytes() == intact
        # A repaired file remains usable for future appends.
        log.start(identity)
    assert path.read_bytes().startswith(intact)


def test_torn_finish_leaves_unclosed_attempt_pending(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 1)
    keys = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.start(keys[1])
    with path.open("ab") as output:
        output.write(b"1,FINISH,2026-01-01T00:00:00+00:00,")
    with CsvExecutionLog(path) as log:
        assert not log.successful_keys()
        assert [r.index for r in select_pending(plan, keys, log.successful_keys()).runs] == [1]


@pytest.mark.parametrize("replacement", ["BOGUS", "START\\ncorrupt".replace("\\n", "\n")])
def test_corrupt_complete_records_fail_without_history_mutation(tmp_path: Path, replacement: str) -> None:
    plan = _plan(tmp_path, 1)
    identity = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1]
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path) as log:
        log.start(identity)
    data = path.read_text(encoding="utf-8")
    path.write_text(data.replace(",START,", f",{replacement},", 1), encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(ExecutionLogError):
        with CsvExecutionLog(path):
            pass
    assert path.read_bytes() == before


@pytest.mark.parametrize("content", ["run,completed,return_code\n", "schema_version,event\n", "x,y,z\n"])
def test_unknown_and_old_report_schemas_are_rejected(tmp_path: Path, content: str) -> None:
    path = tmp_path / "batch_runs.csv"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ExecutionLogError, match="Unsupported execution CSV schema"):
        with CsvExecutionLog(path):
            pass
    assert path.read_text(encoding="utf-8") == content


def test_single_writer_lock_and_nondestructive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "batch_runs.csv"
    with CsvExecutionLog(path):
        with pytest.raises(ExecutionLogError, match="writer or cannot be locked"):
            with CsvExecutionLog(path):
                pass
    first = path.read_bytes()
    with CsvExecutionLog(path):
        pass
    assert path.read_bytes() == first
    assert first.startswith(",".join(COLUMNS).encode())


def test_changed_model_inputs_change_identity_but_runtime_gpu_does_not(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 1)
    run = plan.runs[0]
    key = identify_runs(plan, fallback_executable=Path("fallback.exe"), base_directory=tmp_path)[1].key
    gpu_args = replace(run, arguments=("-pu4", *run.arguments[:-1], "-pu2", run.arguments[-1]))
    assert (
        identify_runs(
            replace(plan, runs=(gpu_args,)), fallback_executable=Path("fallback.exe"), base_directory=tmp_path
        )[1].key
        == key
    )
    # Moving the same named TCF or selecting another installed solver must not
    # force an already completed logical simulation to rerun.
    for changed in (
        replace(run, executable="other.exe"),
        replace(run, tcf=tmp_path / "relocated" / "effective.tcf"),
        replace(run, working_directory=tmp_path / "elsewhere"),
    ):
        assert (
            identify_runs(
                replace(plan, runs=(changed,)), fallback_executable=Path("other.exe"), base_directory=tmp_path
            )[1].key
            == key
        )
    # Model identity and execution mode still matter. Test-only (-t) must not
    # record a successful full simulation.
    for changed in (
        replace(run, tcf=tmp_path / "another.tcf"),
        replace(run, arguments=("-et5", *run.arguments)),
        replace(run, arguments=("-t", *run.arguments)),
        replace(run, flag_pairs=(("e1", "other"),), arguments=("-b", "-e1", "other", str(run.tcf))),
    ):
        assert (
            identify_runs(
                replace(plan, runs=(changed,)), fallback_executable=Path("fallback.exe"), base_directory=tmp_path
            )[1].key
            != key
        )


def test_windows_unc_and_switch_order_have_stable_identity() -> None:
    base = PureWindowsPath(r"Q:\Models")
    tcf = PureWindowsPath(r"\\server\share\models\flow.tcf")
    first = PlannedRun(
        index=1,
        source=base / "list.bat",
        line_start=1,
        line_end=1,
        tcf=tcf,
        working_directory=base,
        executable=r"C:\\TUFLOW\\TUFLOW.exe",
        flag_pairs=(("e1", "01p"), ("s1", "With Space")),
        arguments=("-b", "-e1", "01p", "-s1", "With Space", str(tcf)),
    )
    second = replace(
        first,
        tcf=PureWindowsPath(r"\\SERVER\SHARE\models\FLOW.tcf"),
        flag_pairs=(("s1", "With Space"), ("e1", "01p")),
        arguments=("-S1", "With Space", "-E1", "01p", "-B", str(tcf)),
    )

    def signature(run: PlannedRun) -> str:
        plan = CommandPlan(base / "list.bat", (run,), (), 0, "commands")
        return identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=Path.cwd())[1].key

    assert signature(first) == signature(second)


def test_logical_v2_duplicate_ordinals_remain_distinct() -> None:
    """Identical repeated occurrences retain separate CSV attempt identities."""
    base = PureWindowsPath(r"Q:\Models")
    tcf = PureWindowsPath(r"\\server\share\models\flow.tcf")
    first = PlannedRun(
        1,
        base / "list.txt",
        1,
        1,
        tcf,
        base,
        (("e1", "A.tcf"), ("s1", "With Space")),
        ("-PU2", "-E1", "A.tcf", "-S1", "With Space", "-e1", "A.tcf", str(tcf), "-ET5", "-B"),
        r"C:\TUFLOW\TUFLOW.exe",
    )
    plan = CommandPlan(base / "list.txt", (first, replace(first, index=2)), (), 0, "commands")
    identities = identify_runs(plan, fallback_executable=Path("unused.exe"), base_directory=Path.cwd())
    assert len({identity.key for identity in identities.values()}) == 2
    assert all(len(identity.key) == 64 for identity in identities.values())


def test_resume_identity_depends_on_filename_not_directory(tmp_path: Path) -> None:
    """Only the TCF filename and effective flags determine a completed run."""
    original = _plan(tmp_path, 1)
    identity = identify_runs(original, fallback_executable=Path("solver_a.exe"), base_directory=tmp_path)[1]
    moved = replace(
        original.runs[0],
        tcf=tmp_path / "elsewhere" / "effective.tcf",
        executable="solver_b.exe",
        working_directory=tmp_path / "other_work",
    )
    moved_plan = replace(original, runs=(moved,))
    moved_identities = identify_runs(moved_plan, fallback_executable=Path("solver_b.exe"), base_directory=tmp_path)
    assert moved_identities[1].key == identity.key
    with CsvExecutionLog(tmp_path / "batch_runs.csv") as history:
        attempt = history.start(identity)
        history.finish(identity, attempt, Observed(original.runs[0], 0))
        assert select_pending(moved_plan, moved_identities, history.successful_keys()).runs == ()

    network = PureWindowsPath(r"\\server\share\project\effective.tcf")
    mapped = PureWindowsPath(r"P:\project\effective.tcf")
    for control in (network, mapped):
        target = replace(original.runs[0], tcf=control)
        target_plan = replace(original, runs=(target,))
        assert identify_runs(target_plan, fallback_executable=Path("unused.exe"), base_directory=tmp_path)[1].key == (
            identity.key
        )


def test_same_filename_in_different_directories_needs_no_path_matching(tmp_path: Path) -> None:
    """Separate logical occurrences retain distinct ordinals without filesystem comparisons."""
    plan = _plan(tmp_path, 1)
    first = plan.runs[0]
    other = replace(first, index=2, tcf=tmp_path / "other" / first.tcf.name)
    identities = identify_runs(
        replace(plan, runs=(first, other)), fallback_executable=Path("solver.exe"), base_directory=tmp_path
    )
    assert identities[1].key != identities[2].key


def test_history_rejects_unsupported_gpu_syntax(tmp_path: Path) -> None:
    plan = _plan(tmp_path, 1)
    run = plan.runs[0]
    unsupported = replace(run, arguments=("-pu0,1", *run.arguments))
    with pytest.raises(ExecutionLogError, match="Unsupported simulation argument"):
        identify_runs(
            replace(plan, runs=(unsupported,)), fallback_executable=Path("unused.exe"), base_directory=tmp_path
        )


def test_imported_bare_and_nonterminal_tcf_rows_have_stable_csv_identity(tmp_path: Path) -> None:
    """The valid parser surface must also work with CSV continuation."""
    source = tmp_path / "input.bat"
    fallback = Path("tuflow.exe")

    source.write_text('SET "TCF=model.tcf"\n-e1 A\n', encoding="utf-8")
    bare = read_command_plan(source, initial_directory=tmp_path)
    assert bare.runs[0].arguments == ("-e1", "A")
    bare_key = identify_runs(bare, fallback_executable=fallback, base_directory=tmp_path)[1].key
    assert bare_key

    source.write_text("-e1 A\n", encoding="utf-8")
    python_fallback = read_command_plan(source, initial_directory=tmp_path, fallback_tcf="model.tcf")
    assert identify_runs(python_fallback, fallback_executable=fallback, base_directory=tmp_path)[1].key == bare_key

    source.write_text("tuflow.exe model.tcf -e1 A\n", encoding="utf-8")
    nonterminal = read_command_plan(source, initial_directory=tmp_path)
    nonterminal_key = identify_runs(nonterminal, fallback_executable=fallback, base_directory=tmp_path)[1].key

    source.write_text("tuflow.exe -e1 A model.tcf\n", encoding="utf-8")
    terminal = read_command_plan(source, initial_directory=tmp_path)
    assert identify_runs(terminal, fallback_executable=fallback, base_directory=tmp_path)[1].key == nonterminal_key

    # The .tcf suffix in a flag value is not the model's control-file token.
    source.write_text("tuflow.exe -e1 A.tcf model.tcf\n", encoding="utf-8")
    value_tcf = read_command_plan(source, initial_directory=tmp_path)
    assert identify_runs(value_tcf, fallback_executable=fallback, base_directory=tmp_path)[1].key != nonterminal_key

    # A successful bare-row run is persisted and subsequently skipped on Resume.
    with CsvExecutionLog(tmp_path / "batch_runs.csv") as history:
        attempt = history.start(identify_runs(bare, fallback_executable=fallback, base_directory=tmp_path)[1])
        history.finish(
            identify_runs(bare, fallback_executable=fallback, base_directory=tmp_path)[1],
            attempt,
            Observed(bare.runs[0], 0),
        )
    with CsvExecutionLog(tmp_path / "batch_runs.csv") as history:
        assert (
            select_pending(
                bare,
                identify_runs(bare, fallback_executable=fallback, base_directory=tmp_path),
                history.successful_keys(),
            ).runs
            == ()
        )
