"""Executable command-plan contracts, using public synthetic strings only."""

import subprocess
from collections.abc import Mapping
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import NoReturn

import pytest

from ryan_library.functions.tuflow.command_plan import (
    CommandPlan,
    CommandPlanError,
    Syntax,
    parse_command_plan,
    read_command_plan,
)

SOURCE = PureWindowsPath(r"T:\project\launchers\runs.bat")
CWD = PureWindowsPath(r"T:\project")
HEADER = 'set "TUFLOW_EXE=C:\\TUFLOW\\TUFLOW_iSP_w64.exe"\n'
FULL = 'START /LOW /MIN /WAIT "" "%TUFLOW_EXE%" -b -pu0 -e1 A -s9 StdMat "%TCF%"'


def parse(
    text: str,
    *,
    fallback_tcf: str | None = None,
    environment: Mapping[str, str] | None = None,
    syntax: Syntax = "batch",
) -> CommandPlan:
    return parse_command_plan(
        text, source=SOURCE, initial_directory=CWD, fallback_tcf=fallback_tcf, environment=environment, syntax=syntax
    )


def test_reassignment_applies_only_to_later_runs_and_preserves_duplicates() -> None:
    text = HEADER + 'set "TCF=runs\\first_~e1~.tcf"\n' + FULL + "\n" + FULL + "\n"
    text += "ReM " + FULL + '\nset "tCf=runs\\second_~e1~.tcf"\n' + FULL + "\n"
    plan = parse(text, fallback_tcf="ignored.tcf")
    assert [run.index for run in plan.runs] == [1, 2, 3]
    assert [run.tcf.name for run in plan.runs] == ["first_~e1~.tcf", "first_~e1~.tcf", "second_~e1~.tcf"]
    assert [run.line_start for run in plan.runs] == [3, 4, 7]
    assert plan.runs[0].tcf == CWD / "runs/first_~e1~.tcf"
    assert plan.runs[0].arguments[-1] == r"runs\first_~e1~.tcf"
    assert plan.runs[0].flags == {"e1": "A", "s9": "StdMat"}
    flags = plan.runs[0].flags
    flags["e1"] = "changed"
    assert plan.runs[0].flags["e1"] == "A"


@pytest.mark.parametrize(
    "row",
    [
        "-e1 A -s1 EXG",
        'TUFLOW.exe -b -e1 A -s1 EXG "model.tcf"',
        '"C:\\Program Files\\TUFLOW\\TUFLOW_iSP_w64.exe" -e1 A -s1 EXG model.tcf',
        'START /LOW /WAIT "" TUFLOW.exe -e1 A -s1 EXG model.tcf',
        'start "Window title" /LOW TUFLOW.exe -e1 A -s1 EXG model.tcf',
        "START /WAIT TUFLOW.exe -e1 A -s1 EXG model.tcf",
    ],
)
def test_full_direct_and_bare_rows(row: str) -> None:
    run = parse(row, fallback_tcf="model.tcf").runs[0]
    assert run.tcf == CWD / "model.tcf"
    assert run.flags == {"e1": "A", "s1": "EXG"}


def test_bare_rows_use_current_assignment_and_explicit_tcf_takes_precedence() -> None:
    plan = parse("-e1 A\nset TCF=one.tcf\n-e1 B\n-e1 C two.tcf\nset TCF=three.tcf\n-e1 D", fallback_tcf="fallback.tcf")
    assert [run.tcf.name for run in plan.runs] == ["fallback.tcf", "one.tcf", "two.tcf", "three.tcf"]


def test_case_insensitive_environment_single_assignment_expansion_and_scope() -> None:
    plan = parse(
        "set ROOT=one\nset TCF=%root%\\a.tcf\nset ROOT=two\n"
        "-e1 A\nsetlocal EnableExtensions DisableDelayedExpansion\n"
        "set TCF=inner.tcf\n-e1 B\nendlocal\n-e1 C"
    )
    assert [run.tcf for run in plan.runs] == [CWD / "one/a.tcf", CWD / "inner.tcf", CWD / "one/a.tcf"]


def test_source_folder_and_launch_directory_are_distinct() -> None:
    plan = parse('set TCF=relative.tcf\n-e1 A\npushd "%~dp0"\n-e1 B\npopd\n-e1 C')
    assert [run.tcf for run in plan.runs] == [
        CWD / "relative.tcf",
        SOURCE.parent / "relative.tcf",
        CWD / "relative.tcf",
    ]


def test_relative_assignment_resolves_at_run_time_and_endlocal_does_not_restore_cwd() -> None:
    plan = parse("set TCF=model.tcf\nsetlocal\ncd /d T:\\other\n-e1 A\nendlocal\n-e1 B")
    assert all(run.tcf == PureWindowsPath(r"T:\other\model.tcf") for run in plan.runs)


def test_start_directory_is_child_only_and_quotes_preserve_spaces() -> None:
    text = 'START /D "T:\\space dir" /WAIT "" TUFLOW.exe -e1 A "model one.tcf"\n'
    text += 'TUFLOW.exe -e1 B "model two.tcf"'
    plan = parse(text)
    assert plan.runs[0].tcf == PureWindowsPath(r"T:\space dir\model one.tcf")
    assert plan.runs[1].tcf == CWD / "model two.tcf"


def test_caret_continuations_record_physical_ranges() -> None:
    plan = parse("set TCF=model.tcf\n-e1 A ^\n  -s1 EXG ^\n -s9 StdMat")
    run = plan.runs[0]
    assert (run.line_start, run.line_end) == (2, 4)
    assert run.flags == {"e1": "A", "s1": "EXG", "s9": "StdMat"}


@pytest.mark.parametrize("comment", ["REM -e1 Disabled", " @rEm -e1 Disabled", ":: -e1 Disabled", "@echo -e1 Disabled"])
def test_commented_and_echoed_flags_are_not_runs(comment: str) -> None:
    plan = parse(f"{comment}\n-e1 Active", fallback_tcf="m.tcf")
    assert len(plan.runs) == 1
    assert plan.runs[0].flags == {"e1": "Active"}


@pytest.mark.parametrize("prefix", ["#", ";", "//"])
def test_command_list_comments_are_not_claimed_as_cmd_comments(prefix: str) -> None:
    text = f"{prefix} -e1 Disabled\n-e1 Active"
    assert len(parse(text, fallback_tcf="m.tcf", syntax="commands").runs) == 1
    with pytest.raises(CommandPlanError):
        parse(text, fallback_tcf="m.tcf", syntax="batch")


@pytest.mark.parametrize("ending", ["EXIT", "exit /b", "exit /b 0", "goto :eof"])
def test_termination_ignores_unreachable_tail(ending: str) -> None:
    plan = parse(f"-e1 A\n{ending}\n-e1 B\nIF BAD GOTO nowhere", fallback_tcf="m.tcf")
    assert [run.flags for run in plan.runs] == [{"e1": "A"}]


@pytest.mark.parametrize(
    "bad",
    [
        "if 1==1 TUFLOW.exe -e1 A model.tcf",
        "for %%x in (A B) do TUFLOW.exe -e1 %%x model.tcf",
        "call TUFLOW.exe -e1 A model.tcf",
        "goto later",
        "set /p TCF=Enter:",
        "set /a N=1",
        "setlocal EnableDelayedExpansion",
        "setlocal DisableExtensions",
        "-e1 !EVENT!",
        "-e1 %UNKNOWN%",
        'set TCF=\n-e1 A "%TCF%"',
        "-e1 A & -e1 B",
        "-e1 A > output.txt",
        "-e1 A | more",
        "(-e1 A)",
        "-e1 A ^& bad",
        '-e1 A "unclosed.tcf',
        "python -e1 A model.tcf",
        'START /WAIT "TUFLOW.exe" -e1 A model.tcf',
        "TUFLOW.exe -e1 A",
        'START /I "" TUFLOW.exe -e1 A model.tcf',
        "TUFLOW.exe -e1 A -var X Y model.tcf",
        "-e1 A -e1 B",
        "-e1",
        "-e1 -s1 EXG",
        '-e1 "A B"',
        "-e1 A one.tcf two.tcf",
        "cd D:\\other\n-e1 A",
        "popd\n-e1 A",
        "endlocal\n-e1 A",
        "-e1 A C:relative.tcf",
        'pushd "\\\\server\\share"\n-e1 A',
        "REM unused & TUFLOW.exe -e1 A m.tcf",
    ],
)
def test_unsupported_or_ambiguous_plan_fails_not_silently_skipped(bad: str) -> None:
    with pytest.raises(CommandPlanError, match=r"runs\.bat:"):
        parse("-e1 First\n" + bad, fallback_tcf="m.tcf")


def test_equal_duplicate_switch_preserved_in_arguments_not_duplicate_flag_mapping() -> None:
    run = parse("-e1 A -e1 A", fallback_tcf="m.tcf").runs[0]
    assert run.flag_pairs == (("e1", "A"),)
    assert run.arguments == ("-e1", "A", "-e1", "A")


def test_full_tcf_without_flags_retains_default_context() -> None:
    run = parse("TUFLOW.exe model.tcf").runs[0]
    assert run.flags == {}


def test_no_runs_and_missing_fallback_fail() -> None:
    with pytest.raises(CommandPlanError, match="No active"):
        parse("REM disabled\npause")
    with pytest.raises(CommandPlanError, match="No effective TCF"):
        parse("-e1 A")


def test_explicit_encoding_and_read_preserve_source_bytes(tmp_path: Path) -> None:
    source = tmp_path / "runs.bat"
    original = b"REM caf\xe9\r\nset TCF=m.tcf\r\n-e1 A\r\n"
    source.write_bytes(original)
    with pytest.raises(UnicodeError):
        read_command_plan(source, initial_directory=tmp_path)
    plan = read_command_plan(source, initial_directory=tmp_path, encoding="cp1252")
    assert len(plan.runs) == 1
    assert source.read_bytes() == original


def test_reader_auto_format_and_default_working_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "list.txt"
    source.write_text("# ignored\n-e1 A", encoding="utf-8-sig")
    monkeypatch.chdir(tmp_path)
    plan = read_command_plan(source, fallback_tcf="m.tcf")
    assert plan.runs[0].tcf == tmp_path / "m.tcf"
    assert plan.syntax == "commands"


def test_parser_does_not_launch_any_process(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> NoReturn:
        message = "process launch"
        raise AssertionError(message)

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    assert len(parse(HEADER + "set TCF=m.tcf\n" + FULL).runs) == 1


def test_absolute_source_and_path_flavours_required() -> None:
    with pytest.raises(CommandPlanError, match="absolute"):
        parse_command_plan("-e1 A", source=PureWindowsPath("a.bat"), initial_directory=CWD)
    with pytest.raises(CommandPlanError, match="flavours"):
        parse_command_plan("-e1 A", source=PurePosixPath("/a.bat"), initial_directory=CWD)


def test_explicit_environment_and_cd_dynamic_variable() -> None:
    run = parse("set TCF=%CD%\\%MODEL%.tcf\n-e1 A", environment={"MoDeL": "first"}).runs[0]
    assert run.tcf == CWD / "first.tcf"


def test_malformed_disabled_command_remains_a_comment() -> None:
    result = parse('REM START "unfinished disabled command\n-e1 A', fallback_tcf="m.tcf")
    assert len(result.runs) == 1


@pytest.mark.parametrize(
    "bad",
    [
        "TUFLOW.exe.bat -e1 A m.tcf",
        "REM disabled ^\n-e1 B",
        'START /NODE bad "" TUFLOW.exe m.tcf',
        'START /AFFINITY zzz "" TUFLOW.exe m.tcf',
    ],
)
def test_ambiguous_command_forms_are_rejected(bad: str) -> None:
    with pytest.raises(CommandPlanError):
        parse("-e1 First\n" + bad, fallback_tcf="m.tcf")


def test_quoted_start_title_is_not_an_option() -> None:
    run = parse('START "/LOW" /WAIT "TUFLOW.exe" -e1 A model.tcf').runs[0]
    assert run.tcf == CWD / "model.tcf"


def test_unquoted_directory_whitespace_is_rejected_not_collapsed() -> None:
    with pytest.raises(CommandPlanError, match="Quote directory paths"):
        parse("cd /d T:\\two  spaces\n-e1 A model.tcf")


def test_unc_tcf_is_preserved_without_a_drive_mapping() -> None:
    run = parse('TUFLOW.exe -e1 A "\\\\server\\share\\model.tcf"').runs[0]
    assert run.tcf == PureWindowsPath(r"\\server\share\model.tcf")


def test_unc_launch_directory_is_not_assumed_to_be_a_cmd_directory() -> None:
    with pytest.raises(CommandPlanError, match="mapped-drive launch"):
        parse_command_plan(
            "-e1 A m.tcf",
            source=PureWindowsPath(r"\\server\share\runs.bat"),
            initial_directory=PureWindowsPath(r"\\server\share"),
        )


def test_empty_leading_continuation_preserves_original_line_range() -> None:
    run = parse("^\n-e1 A model.tcf").runs[0]
    assert (run.line_start, run.line_end) == (1, 2)
    assert run.flags == {"e1": "A"}


@pytest.mark.parametrize("prefix", ["#", ";", "//"])
def test_command_list_comment_caret_does_not_consume_active_row(prefix: str) -> None:
    plan = parse(
        f"-e1 First model.tcf\n{prefix} disabled ^\n-e1 Second model.tcf\n-e1 Third model.tcf",
        syntax="commands",
    )
    assert [run.flags["e1"] for run in plan.runs] == ["First", "Second", "Third"]
    assert [run.line_start for run in plan.runs] == [1, 3, 4]
    assert [run.index for run in plan.runs] == [1, 2, 3]


@pytest.mark.parametrize("prefix", ["#", ";", "//"])
def test_command_list_comment_caret_does_not_hide_unsupported_statement(prefix: str) -> None:
    with pytest.raises(CommandPlanError, match="Unsupported shell control flow"):
        parse(
            f"-e1 First model.tcf\n{prefix} disabled ^\nIF unknown -e1 Second model.tcf\n-e1 Third model.tcf",
            syntax="commands",
        )
