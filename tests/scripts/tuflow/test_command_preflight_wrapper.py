"""Wrapper behaviour: plan before checking, no solver calls and no overwrite."""

import argparse
import json
import runpy
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol, cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlanError, parse_command_plan
from ryan_library.orchestrators.tuflow import command_preflight
from ryan_library.orchestrators.tuflow.command_preflight import CommandPreflightIssue, CommandPreflightReport
from ryan_library.orchestrators.tuflow.input_preflight import PreflightIssue

WRAPPER = Path(__file__).absolute().parents[3] / "ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py"


class WrapperMain(Protocol):
    def __call__(
        self,
        *,
        tcf: Path | None,
        commands: Path | Sequence[Path],
        report: Path,
        working_directory: Path | None = None,
        configuration_directory: Path | None = None,
        plan_only: bool = False,
        plan_report: Path | None = None,
    ) -> int: ...


def entry() -> WrapperMain:
    return cast(WrapperMain, runpy.run_path(str(WRAPPER))["main"])


def test_checker_exposes_concise_editable_settings_and_cli_overrides() -> None:
    namespace = runpy.run_path(str(WRAPPER))
    assert namespace["TCF"] is None
    assert namespace["RUN_FILES"] == (WRAPPER.with_name("run_tuflow_simulations.txt"),)
    assert namespace["REPORT_CSV"] is None
    assert namespace["PLAN_ONLY"] is False
    assert namespace["INPUT_SYNTAX"] == "auto"
    assert namespace["IO_WORKERS"] == 4
    assert namespace["MAX_DIRECTORY_ENTRIES"] == 50_000
    assert not any(name.startswith("DEFAULT_") for name in namespace)

    parse = cast(Callable[[list[str]], argparse.Namespace], namespace["_parse_arguments"])
    opts = parse(["--no-tcf", "--no-report", "--no-plan-only", "--io-workers", "0", "--no-pause"])
    assert opts.no_tcf is True
    assert opts.no_report is True
    assert opts.plan_only is False
    assert opts.io_workers == 0
    with pytest.raises(SystemExit) as conflicting:
        parse(["--pause", "--no-pause"])
    assert conflicting.value.code == 2


def test_plan_only_writes_manifest_without_preflight(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_options: object) -> None:
        pytest.fail("preflight called during plan only")

    monkeypatch.setattr(command_preflight, "_check_group", forbidden)
    source = tmp_path / "runs.bat"
    source.write_text("set TCF=first.tcf\n-e1 A\nset TCF=second.tcf\n-e1 B", encoding="utf-8")
    before = source.read_bytes()
    output = tmp_path / "plan.json"
    assert (
        entry()(
            tcf=None,
            commands=source,
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_only=True,
            plan_report=output,
        )
        == 0
    )
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["preflight_performed"] is False
    assert [Path(row["tcf"]).name for row in manifest["runs"]] == ["first.tcf", "second.tcf"]
    assert source.read_bytes() == before
    assert not (tmp_path / "issues.csv").exists()


def test_ignored_exclamations_are_prominent_in_console_and_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "runs.bat"
    source.write_text("set TCF=mo!del.tcf\n-e1 !EVENT! !!!", encoding="utf-8")
    output = tmp_path / "plan.json"

    assert (
        entry()(
            tcf=None,
            commands=source,
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_only=True,
            plan_report=output,
        )
        == 0
    )

    console = capsys.readouterr().out
    assert "WARNING: SOURCE TEXT WAS IGNORED" in console
    assert "Ignored 6 literal '!' character(s) on 2 line(s)." in console
    assert "Planned runs and input-check outputs may be wrong." in console
    assert "runs.bat:1" in console
    assert "runs.bat:2" in console
    assert "model.tcf" in console
    assert str(tmp_path) not in console
    assert "PLAN ONLY: Commands were interpreted, but model inputs were not checked." in console

    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert Path(manifest["runs"][0]["tcf"]).name == "model.tcf"
    assert manifest["runs"][0]["flags"] == {"e1": "EVENT"}
    assert [(warning["line"], warning["count"]) for warning in manifest["warnings"]] == [(1, 1), (2, 5)]
    assert all("outputs may be wrong" in warning["message"] for warning in manifest["warnings"])


def test_preflight_presentation_separates_summary_issue_detail_and_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = (tmp_path / "runs.bat").absolute()
    plan = parse_command_plan(
        "-e1 A",
        source=source,
        initial_directory=tmp_path.absolute(),
        fallback_tcf="model.tcf",
    )
    result = CommandPreflightReport(plan=plan, completed_runs=1)
    result.checked_paths.add(str(tmp_path / "boundary.csv"))
    result.issues.append(
        CommandPreflightIssue(
            run=plan.runs[0],
            finding=PreflightIssue(
                run_index=1,
                flags="-e1 A",
                status="unresolved",
                origin="boundary database",
                reference="boundary.csv",
                detail="Required CSV header was not found.",
                kind="csv_header",
                owner_control_file=tmp_path / "model" / "boundary.tbc",
                csv_columns=("Time", "Flow"),
                command_text="BC Database == boundary.csv",
                line_number=12,
            ),
        )
    )
    namespace = runpy.run_path(str(WRAPPER))
    printer = cast(Callable[..., None], namespace["_print_preflight"])

    printer(result, display_base=tmp_path, elapsed_seconds=12.345)

    console = capsys.readouterr().out
    assert "INPUT CHECK\n-----------" in console
    assert "Status            : FAIL" in console
    assert "Runs checked      : 1 of 1" in console
    assert "Script wall time : 12.35 s" in console
    assert "Input-check phases:" in console
    assert "ISSUES\n------" in console
    assert "1. UNRESOLVED — model.tcf -e1 A (run 1; csv_header)" in console
    assert "Source   : runs.bat:1-1" in console
    assert f"Control file: {Path('model/boundary.tbc')}:12" in console
    assert "Command  : BC Database == boundary.csv" in console
    assert "Reference: boundary.csv" in console
    assert "Detail   : Required CSV header was not found." in console
    assert "Required CSV columns: 'Time', 'Flow'." in console
    assert "RESULT\n------" in console
    assert "FAIL: 1 issue(s) affect 1 planned run(s)." in console
    assert "before relying on the planned simulations or their outputs" in console


def test_failed_plan_does_not_emit_a_partial_manifest(tmp_path: Path) -> None:
    source = tmp_path / "runs.bat"
    source.write_text("-e1 A\nIF unknown -e1 B", encoding="utf-8")
    output = tmp_path / "plan.json"
    with pytest.raises(CommandPlanError):
        entry()(
            tcf=Path("fallback.tcf"),
            commands=source,
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_only=True,
            plan_report=output,
        )
    assert not output.exists()


def test_manifest_cannot_replace_source(tmp_path: Path) -> None:
    source = tmp_path / "runs.bat"
    source.write_text("-e1 A", encoding="utf-8")
    before = source.read_bytes()
    with pytest.raises(FileExistsError):
        entry()(
            tcf=Path("fallback.tcf"),
            commands=source,
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_only=True,
            plan_report=source,
        )
    assert source.read_bytes() == before


def test_help_does_not_import_preflight_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    before = set(sys.modules)
    monkeypatch.setattr(sys, "argv", [str(WRAPPER), "--help"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(WRAPPER), run_name="__main__")
    assert exc.value.code == 0
    added = set(sys.modules) - before
    assert not any(name == "pytuflow" or name.startswith("pytuflow.") for name in added)


def test_multiple_command_files_plan_only_preserves_provenance_and_order(tmp_path: Path) -> None:
    first = tmp_path / "first.bat"
    second = tmp_path / "second.txt"
    first.write_text("set TCF=a.tcf\n-e1 A\n-e1 A", encoding="utf-8")
    second.write_text("# comment\nset TCF=b.tcf\n-e1 A", encoding="utf-8")
    output = tmp_path / "combined.json"
    assert (
        entry()(
            tcf=None,
            commands=(first, second),
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_only=True,
            plan_report=output,
        )
        == 0
    )
    runs = json.loads(output.read_text(encoding="utf-8"))["runs"]
    assert [run["run_index"] for run in runs] == [1, 2]
    assert [run["source_file"] for run in runs] == [str(first), str(second)]
    assert [Path(run["tcf"]).name for run in runs] == ["a.tcf", "b.tcf"]


def test_late_tcf_conflict_fails_before_checker_or_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first = tmp_path / "first.bat"
    second = tmp_path / "second.bat"
    first.write_text("TUFLOW.exe good.tcf", encoding="utf-8")
    second.write_text("rem skipped\nTUFLOW.exe other.tcf", encoding="utf-8")
    output = tmp_path / "plan.json"

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Validation must not run for a conflicting plan")

    monkeypatch.setattr(command_preflight, "check_command_plan", forbidden)
    with pytest.raises(CommandPlanError, match=r"second\.bat:2-2: Python TCF.*good\.tcf.*other\.tcf"):
        entry()(
            tcf=Path("good.tcf"),
            commands=(first, second),
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            plan_report=output,
        )
    assert not output.exists()
    assert not (tmp_path / "issues.csv").exists()


def test_relative_command_source_uses_working_directory_not_invoking_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model = tmp_path / "model"
    invocation = tmp_path / "invocation"
    model.mkdir()
    invocation.mkdir()
    source = model / "runs.txt"
    source.write_text("TUFLOW.exe correct_model.tcf", encoding="utf-8")
    (invocation / "runs.txt").write_text("TUFLOW.exe WRONG_model.tcf", encoding="utf-8")
    manifest_path = tmp_path / "plan.json"

    monkeypatch.chdir(invocation)
    assert (
        entry()(
            tcf=None,
            commands=Path("runs.txt"),
            report=tmp_path / "issues.csv",
            working_directory=model,
            plan_only=True,
            plan_report=manifest_path,
        )
        == 0
    )
    planned = json.loads(manifest_path.read_text(encoding="utf-8"))["runs"]
    assert len(planned) == 1
    assert planned[0]["source_file"] == str(source)
    assert planned[0]["tcf"] == str(model / "correct_model.tcf")
    assert not (tmp_path / "issues.csv").exists()


def test_cli_mixed_relative_and_absolute_sources_preserve_order_and_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model = tmp_path / "model"
    invocation = tmp_path / "invocation"
    elsewhere = tmp_path / "elsewhere"
    for directory in (model, invocation, elsewhere):
        directory.mkdir()
    relative = model / "first.bat"
    absolute = elsewhere / "second.txt"
    relative.write_text("TUFLOW.exe first_model.tcf", encoding="utf-8")
    absolute.write_text("TUFLOW.exe other_model.tcf", encoding="utf-8")
    (invocation / "first.bat").write_text("TUFLOW.exe wrong_model.tcf", encoding="utf-8")
    output = tmp_path / "mixed.json"

    monkeypatch.chdir(invocation)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(WRAPPER),
            "--working-directory",
            str(model),
            "--runs",
            "first.bat",
            "--runs",
            str(absolute),
            "--plan-only",
            "--plan-report",
            str(output),
            "--no-pause",
        ],
    )
    with pytest.raises(SystemExit) as finished:
        runpy.run_path(str(WRAPPER), run_name="__main__")
    assert finished.value.code == 0
    planned = json.loads(output.read_text(encoding="utf-8"))["runs"]
    assert [run["source_file"] for run in planned] == [str(relative), str(absolute)]
    assert [run["tcf"] for run in planned] == [
        str(model / "first_model.tcf"),
        str(model / "other_model.tcf"),
    ]


def test_configuration_base_explicitly_controls_python_tcf(tmp_path: Path) -> None:
    model = tmp_path / "config" / "model.tcf"
    model.parent.mkdir()
    source = tmp_path / "inputs.bat"
    source.write_text('START /D "config" "" TUFLOW.exe model.tcf', encoding="utf-8")
    assert (
        entry()(
            tcf=Path("model.tcf"),
            commands=source,
            report=tmp_path / "issues.csv",
            working_directory=tmp_path,
            configuration_directory=tmp_path / "config",
            plan_only=True,
        )
        == 0
    )


def test_cli_plan_only_accepts_repeated_runs_without_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = tmp_path / "a.bat", tmp_path / "b.txt"
    first.write_text("TUFLOW.exe first.tcf", encoding="utf-8")
    second.write_text("TUFLOW.exe second.tcf", encoding="utf-8")
    output = tmp_path / "from-cli.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(WRAPPER),
            "--runs",
            str(first),
            "--runs",
            str(second),
            "--working-directory",
            str(tmp_path),
            "--plan-only",
            "--plan-report",
            str(output),
            "--no-pause",
        ],
    )
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(WRAPPER), run_name="__main__")
    assert exc.value.code == 0
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert [Path(run["tcf"]).name for run in manifest["runs"]] == ["first.tcf", "second.tcf"]
