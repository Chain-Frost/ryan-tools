"""Wrapper behaviour: plan before checking, no solver calls and no overwrite."""

import argparse
import json
import runpy
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol, cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlanError
from ryan_library.orchestrators.tuflow import command_preflight

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
