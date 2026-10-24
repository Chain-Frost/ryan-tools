"""Wrapper behaviour: plan before checking, no solver calls and no overwrite."""

import json
from pathlib import Path
import runpy
import sys
from typing import Protocol, cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlanError
from ryan_library.orchestrators.tuflow import command_preflight

WRAPPER = Path(__file__).absolute().parents[3] / 'ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py'


class WrapperMain(Protocol):
    def __call__(self, *, tcf: Path | None, commands: Path, report: Path, check_gpkg_layers: bool,
                 working_directory: Path | None = None, plan_only: bool = False,
                 plan_report: Path | None = None) -> int: ...


def entry() -> WrapperMain:
    return cast(WrapperMain, runpy.run_path(str(WRAPPER))['main'])


def test_plan_only_writes_manifest_without_preflight(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_options: object) -> None:
        pytest.fail('preflight called during plan only')
    monkeypatch.setattr(command_preflight, '_check_group', forbidden)
    source = tmp_path / 'runs.bat'
    source.write_text('set TCF=first.tcf\n-e1 A\nset TCF=second.tcf\n-e1 B', encoding='utf-8')
    before = source.read_bytes()
    output = tmp_path / 'plan.json'
    assert entry()(tcf=None, commands=source, report=tmp_path / 'issues.csv', check_gpkg_layers=True,
                   working_directory=tmp_path, plan_only=True, plan_report=output) == 0
    manifest = json.loads(output.read_text(encoding='utf-8'))
    assert manifest['preflight_performed'] is False
    assert [Path(row['tcf']).name for row in manifest['runs']] == ['first.tcf', 'second.tcf']
    assert source.read_bytes() == before and not (tmp_path / 'issues.csv').exists()


def test_failed_plan_does_not_emit_a_partial_manifest(tmp_path: Path) -> None:
    source = tmp_path / 'runs.bat'
    source.write_text('-e1 A\nIF unknown -e1 B', encoding='utf-8')
    output = tmp_path / 'plan.json'
    with pytest.raises(CommandPlanError):
        entry()(tcf=Path('fallback.tcf'), commands=source, report=tmp_path / 'issues.csv',
                check_gpkg_layers=True, working_directory=tmp_path, plan_only=True, plan_report=output)
    assert not output.exists()


def test_manifest_cannot_replace_source(tmp_path: Path) -> None:
    source = tmp_path / 'runs.bat'
    source.write_text('-e1 A', encoding='utf-8')
    before = source.read_bytes()
    with pytest.raises(FileExistsError):
        entry()(tcf=Path('fallback.tcf'), commands=source, report=tmp_path / 'issues.csv',
                check_gpkg_layers=True, working_directory=tmp_path, plan_only=True, plan_report=source)
    assert source.read_bytes() == before


def test_help_does_not_import_preflight_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    before = set(sys.modules)
    monkeypatch.setattr(sys, 'argv', [str(WRAPPER), '--help'])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(WRAPPER), run_name='__main__')
    assert exc.value.code == 0
    added = set(sys.modules) - before
    assert not any(name == 'pytuflow' or name.startswith('pytuflow.') for name in added)
