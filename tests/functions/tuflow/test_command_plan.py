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
    effective_executable,
    is_gpu_switch,
    model_arguments,
    parse_command_plan,
    partition_model_arguments,
    read_command_plan,
    read_command_plans,
    validate_run_arguments,
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


def test_reassignment_applies_only_to_later_unique_runs() -> None:
    text = HEADER + 'set "TCF=runs\\first_~e1~.tcf"\n' + FULL + "\n" + FULL + "\n"
    text += "ReM " + FULL + '\nset "tCf=runs\\second_~e1~.tcf"\n' + FULL + "\n"
    plan = parse(text)
    assert [run.index for run in plan.runs] == [1, 2]
    assert plan.duplicates_dropped == 1
    assert [run.tcf.name for run in plan.runs] == ["first_~e1~.tcf", "second_~e1~.tcf"]
    assert [run.line_start for run in plan.runs] == [3, 7]
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
    plan = parse("set TCF=fallback.tcf\n-e1 A\nset TCF=one.tcf\n-e1 B\n-e1 C two.tcf\nset TCF=three.tcf\n-e1 D")
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
        "-e1 A B",
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
    with pytest.raises(CommandPlanError, match="Cannot read or decode"):
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


def test_python_tcf_is_consistency_constraint_even_for_full_commands() -> None:
    with pytest.raises(CommandPlanError, match=r"runs\.bat:2-2: Python TCF .*first\.tcf.*second\.tcf"):
        parse("TUFLOW.exe first.tcf\nTUFLOW.exe second.tcf", fallback_tcf="first.tcf")
    with pytest.raises(CommandPlanError, match=r"runs\.bat:2-2: Python TCF"):
        parse("set TCF=first.tcf\n-e1 A other.tcf", fallback_tcf="first.tcf")
    assert [run.tcf.name for run in parse("TUFLOW.exe first.tcf\n-e1 A", fallback_tcf="first.tcf").runs] == [
        "first.tcf",
        "first.tcf",
    ]


def test_python_tcf_fallback_uses_configuration_base_not_changed_directory() -> None:
    source = PureWindowsPath(r"T:\\project\\launcher\\runs.bat")
    base = PureWindowsPath(r"T:\\python")
    plan = parse_command_plan(
        'cd /d T:\\\\python\n-e1 A\nSTART /D T:\\\\python "" TUFLOW.exe model.tcf',
        source=source,
        initial_directory=CWD,
        configuration_directory=base,
        fallback_tcf="model.tcf",
    )
    assert len(plan.runs) == 2
    assert all(run.tcf == base / "model.tcf" for run in plan.runs)
    with pytest.raises(CommandPlanError, match="conflicts with effective imported TCF"):
        parse_command_plan(
            "-e1 A",
            source=source,
            initial_directory=CWD,
            configuration_directory=base,
            fallback_tcf="../other/model.tcf",
            environment={"TCF": "model.tcf"},
        )


def test_verified_mapped_drive_alias_matches_configured_python_tcf(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only actual Windows drive mappings permit a UNC alias to satisfy the TCF constraint."""
    from ryan_library.functions.tuflow import command_plan

    source = PureWindowsPath(r"P:\project\launcher\runs.bat")
    cwd = PureWindowsPath(r"P:\project")
    expected = str(cwd / "model.tcf")
    unc = r"\\server\share\nested\project\model.tcf"
    calls: list[str] = []

    def drive_mapping(drive: str) -> str | None:
        calls.append(drive)
        return r"\\server\share\nested" if drive.casefold() == "p:" else None

    monkeypatch.setattr(command_plan, "_network_drive_target", drive_mapping)
    plan = parse_command_plan(f'TUFLOW.exe "{unc}"', source=source, initial_directory=cwd, fallback_tcf=expected)
    assert plan.runs[0].tcf == PureWindowsPath(unc)
    assert "P:" in calls

    # Two spellings of the same mapped file are one model selection.
    duplicate = parse_command_plan(
        f'TUFLOW.exe "{cwd / "model.tcf"}"\nTUFLOW.exe "{unc}"',
        source=source,
        initial_directory=cwd,
        fallback_tcf=expected,
    )
    assert len(duplicate.runs) == 1
    assert duplicate.duplicates_dropped == 1

    with pytest.raises(CommandPlanError, match="conflicts with effective imported TCF"):
        parse_command_plan(
            r'TUFLOW.exe "\\server\share\nested\other\model.tcf"',
            source=source,
            initial_directory=cwd,
            fallback_tcf=expected,
        )

    monkeypatch.setattr(command_plan, "_network_drive_target", lambda _drive: None)
    with pytest.raises(CommandPlanError, match="conflicts with effective imported TCF"):
        parse_command_plan(f'TUFLOW.exe "{unc}"', source=source, initial_directory=cwd, fallback_tcf=expected)


def test_overwritten_assignments_and_disabled_rows_do_not_conflict() -> None:
    plan = parse(
        "set TCF=unused.tcf\nset TCF=good.tcf\nrem TUFLOW.exe bad.tcf\nset TCF_05=other.tcf\n-e1 A\n@rem -e1 B bad.tcf",
        fallback_tcf="good.tcf",
    )
    assert len(plan.runs) == 1
    assert plan.runs[0].tcf == CWD / "good.tcf"


def test_multi_file_reader_deduplicates_and_preserves_isolated_state(tmp_path: Path) -> None:
    first = tmp_path / "first.bat"
    second = tmp_path / "second.txt"
    first.write_text("set TCF=a.tcf\n-e1 A\n-e1 A\ncd other\n-e1 B", encoding="utf-8")
    second.write_text("# list comment\nset TCF=b.tcf\n-e1 A", encoding="utf-8")
    plan = read_command_plans((first, second), initial_directory=tmp_path)
    assert [r.index for r in plan.runs] == [1, 2, 3]
    assert plan.duplicates_dropped == 1
    assert [r.tcf.name for r in plan.runs] == ["a.tcf", "a.tcf", "b.tcf"]
    assert [r.source for r in plan.runs] == [first, first, second]
    assert [r.working_directory for r in plan.runs] == [tmp_path, tmp_path / "other", tmp_path]
    assert plan.source_files == (first, second)
    assert plan.syntax == "mixed"
    assert plan.directories[-1].source == second


def test_separate_files_must_not_leak_tcf_or_cd(tmp_path: Path) -> None:
    first = tmp_path / "first.bat"
    second = tmp_path / "second.bat"
    first.write_text("set TCF=a.tcf\ncd sub\n-e1 A", encoding="utf-8")
    second.write_text("-e1 B", encoding="utf-8")
    with pytest.raises(CommandPlanError, match="No effective TCF"):
        read_command_plans((first, second), initial_directory=tmp_path)


@pytest.mark.parametrize(
    ("late_text", "match"),
    [
        ("-e1 B different.tcf", "Python TCF"),
        ("IF broken", "Unsupported shell control flow"),
        ("TUFLOW.exe -e1 B", "No effective TCF"),
    ],
)
def test_late_input_error_raises_without_a_partial_plan(tmp_path: Path, late_text: str, match: str) -> None:
    first = tmp_path / "first.bat"
    second = tmp_path / "second.bat"
    first.write_text("TUFLOW.exe good.tcf", encoding="utf-8")
    second.write_text(late_text, encoding="utf-8")
    with pytest.raises(CommandPlanError, match=match):
        read_command_plans((first, second), initial_directory=tmp_path, fallback_tcf="good.tcf")


def test_missing_or_undecodable_late_file_fails_with_source(tmp_path: Path) -> None:
    first = tmp_path / "a.bat"
    other = tmp_path / "b.bat"
    first.write_text("TUFLOW.exe a.tcf", encoding="utf-8")
    other.write_bytes(b"\xff")
    with pytest.raises(CommandPlanError, match="Cannot read or decode"):
        read_command_plans((first, other), initial_directory=tmp_path)
    other.unlink()
    with pytest.raises(CommandPlanError, match=r"b\.bat.*Cannot read or decode"):
        read_command_plans((first, other), initial_directory=tmp_path)


def test_named_tcf_variables_and_batch_roundtrip_keep_distinct_runs() -> None:
    batch = (
        "set TCF_05=Model_05.tcf\nset TCF_06=Model_06.tcf\n"
        'START /LOW /MIN /WAIT "" TUFLOW.exe -b -pu0 -e1 A "%TCF_05%"\n'
        'START /LOW /MIN /WAIT "" TUFLOW.exe -b -pu0 -e1 A "%TCF_06%"\n'
        'START /LOW /MIN /WAIT "" TUFLOW.exe -b -pu0 -e1 A "%TCF_05%"'
    )
    plan = parse(batch)
    assert [run.tcf.name for run in plan.runs] == ["Model_05.tcf", "Model_06.tcf"]
    assert plan.duplicates_dropped == 1
    assert len(plan.runs) == 2
    assert all(run.arguments[:4] == ("-b", "-pu0", "-e1", "A") for run in plan.runs)
    assert all(run.start_options == ("/low", "/min", "/wait") for run in plan.runs)


@pytest.mark.parametrize("modifier", ["/NODE 0", "/AFFINITY FF", "/SEPARATE", "/SHARED"])
def test_start_modifiers_that_runner_cannot_honor_are_rejected(modifier: str) -> None:
    with pytest.raises(CommandPlanError, match="Unsupported START execution modifier"):
        parse(f'START {modifier} "" TUFLOW.exe model.tcf')


def test_frozen_plan_and_fallback_executable_contract() -> None:
    from dataclasses import FrozenInstanceError

    run = parse("-e1 A", fallback_tcf="model.tcf").runs[0]
    assert run.requires_executable_fallback
    assert run.executable is None
    assert run.arguments == ("-e1", "A")
    with pytest.raises(FrozenInstanceError):
        run.index = 2  # pyright: ignore[reportAttributeAccessIssue] - frozen contract test


def test_quoted_event_value_remains_structured_for_upstream_validation() -> None:
    run = parse('TUFLOW.exe -e1 "Wet Season" -s1 "Case A" model.tcf').runs[0]
    assert run.flag_pairs == (("e1", "Wet Season"), ("s1", "Case A"))
    assert run.arguments == ("-e1", "Wet Season", "-s1", "Case A", "model.tcf")


def test_shared_argument_projection_preserves_values_order_and_duplicate_flags() -> None:
    run = parse('TUFLOW.exe model.tcf -PU2 -E1 storm.tcf -e1 storm.tcf -s1 "Wet Season" -ET5 -b').runs[0]
    model = ("-E1", "storm.tcf", "-e1", "storm.tcf", "-s1", "Wet Season", "-ET5", "-b")
    assert model_arguments(run) == ("-PU2", *model)
    assert model_arguments(run, include_gpu=False) == model
    assert model_arguments(run, include_gpu=False, include_batch=False) == model[:-1]
    assert partition_model_arguments(run) == (
        (("e1", "storm.tcf"), ("e1", "storm.tcf"), ("s1", "Wet Season")),
        ("-ET5", "-b"),
    )


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("model.tcf", "-e1"), "Missing value"),
        (("model.tcf", "-e1", ""), "Invalid/unsupported value"),
        (("model.tcf", "-e1", "-pu0"), "Invalid/unsupported value"),
        (("model.tcf", "-e1", "A", "-e1", "B"), "Conflicting duplicate"),
        (("-pu0,1", "model.tcf"), "Unsupported simulation argument"),
    ],
)
def test_generated_and_imported_arguments_share_rejections(arguments: tuple[str, ...], message: str) -> None:
    with pytest.raises(CommandPlanError, match=message):
        validate_run_arguments(arguments)
    with pytest.raises(CommandPlanError, match=message):
        parse("TUFLOW.exe " + subprocess.list2cmdline(arguments))


def test_supported_gpu_selectors_are_individual_pu_flags() -> None:
    for token in ("-pu0", "-PU2", "-pu15"):
        assert is_gpu_switch(token)
    for token in ("-pu0,1", "-pu", "-pu-1", "-gpu0"):
        assert not is_gpu_switch(token)
    run = parse("TUFLOW.exe -pu0 -PU2 model.tcf").runs[0]
    assert model_arguments(run) == ("-pu0", "-PU2")
    assert model_arguments(run, include_gpu=False) == ()


def test_effective_executable_uses_source_or_explicit_fallback_base(tmp_path: Path) -> None:
    explicit = parse(r'"C:\Engines\TUFLOW.exe" model.tcf').runs[0]
    bare = parse("-e1 A", fallback_tcf="model.tcf").runs[0]
    fallback = Path("engines/TUFLOW.exe")
    assert effective_executable(explicit, fallback_executable=fallback, base_directory=tmp_path) == explicit.executable
    assert effective_executable(bare, fallback_executable=fallback, base_directory=tmp_path) == str(tmp_path / fallback)


def test_deduplication_ignores_imported_executable_priority_engine_and_gpu() -> None:
    source = (
        "TUFLOW.exe -b -pu0 -e1 A -s1 Main model.tcf\n"
        'START /LOW /WAIT "" "C:\\Tools\\TUFLOW_iSP_w64.exe" -t -pu7 -s1 Main -e1 A model.tcf\n'
        "TUFLOW.exe -b -e1 A -s1 Main -et1 model.tcf\n"
        "TUFLOW.exe -b -e1 B -s1 Main model.tcf"
    )
    plan = parse(source)
    assert len(plan.runs) == 3
    assert plan.duplicates_dropped == 1
    assert [run.line_start for run in plan.runs] == [1, 3, 4]


def test_deduplication_across_command_files(tmp_path: Path) -> None:
    first, second = tmp_path / "first.txt", tmp_path / "second.txt"
    first.write_text("-e1 A model.tcf\n", encoding="utf-8")
    second.write_text("-e1 A model.tcf\n-e1 B model.tcf\n", encoding="utf-8")
    plan = read_command_plans((first, second), initial_directory=tmp_path)
    assert len(plan.runs) == 2
    assert plan.duplicates_dropped == 1
    assert [run.source for run in plan.runs] == [first, second]


def test_posix_start_accepts_absolute_executable_after_empty_title() -> None:
    plan = parse_command_plan(
        'START /WAIT "" "/opt/tuflow/TUFLOW_iSP_w64.exe" -e1 A model.tcf',
        source=PurePosixPath("/models/commands.bat"),
        initial_directory=PurePosixPath("/models"),
    )
    assert plan.runs[0].executable == "/opt/tuflow/TUFLOW_iSP_w64.exe"
    assert plan.runs[0].tcf == PurePosixPath("/models/model.tcf")
