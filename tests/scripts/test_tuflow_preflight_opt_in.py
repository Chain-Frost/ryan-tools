"""Standalone input-checker integration with the supported grouped validator.

No TUFLOW executable or model parser is required for these synthetic tests.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

import pytest

from ryan_library.orchestrators.tuflow import input_preflight


class _WrapperMain(Protocol):
    def __call__(
        self,
        *,
        tcf: Path | None,
        commands: Path,
        report: Path,
        check_gpkg_layers: bool,
        working_directory: Path,
    ) -> int: ...


@dataclass
class _SuccessfulGroup:
    run_count: int
    issues: list[object] = field(default_factory=list[object])
    checked_paths: set[str] = field(default_factory=set[str])
    parse_seconds: float = 0.0
    context_seconds: float = 0.0
    inventory_seconds: float = 0.0

    @property
    def passed(self) -> bool:
        return not self.issues


def _load_wrapper() -> tuple[ModuleType, _WrapperMain]:
    script = Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py"
    spec = importlib.util.spec_from_file_location("_test_tuflow_input_wrapper", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, cast(_WrapperMain, module.main)


def test_standalone_uses_one_grouped_validator_with_original_run_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No removed parser backend or separate flags-only run list is selected."""
    module, main = _load_wrapper()
    calls: list[tuple[str, tuple[int, ...], tuple[dict[str, str], ...], dict[str, object]]] = []

    def check(
        tcf: Path,
        requests: Sequence[input_preflight.RunRequest],
        **options: object,
    ) -> _SuccessfulGroup:
        calls.append(
            (
                tcf.name,
                tuple(item.index for item in requests),
                tuple(dict(item.flags) for item in requests),
                options,
            )
        )
        return _SuccessfulGroup(run_count=len(requests))

    monkeypatch.setattr(input_preflight, "check_simulation_inputs", check)
    source = tmp_path / "runs.txt"
    source.write_text(
        'TUFLOW.exe -e1 "Wet Season" first.tcf\nTUFLOW.exe -e1 Dry second.tcf\nTUFLOW.exe -e1 "Wet Season" first.tcf',
        encoding="utf-8",
    )
    output = tmp_path / "issues.csv"

    assert module.DEFAULT_TCF is None
    assert (
        main(
            tcf=None,
            commands=source,
            report=output,
            check_gpkg_layers=False,
            working_directory=tmp_path,
        )
        == 0
    )
    assert output.is_file()
    assert [(tcf, indices) for tcf, indices, _, _ in calls] == [
        ("first.tcf", (1, 3)),
        ("second.tcf", (2,)),
    ]
    assert calls[0][2] == ({"e1": "Wet Season"}, {"e1": "Wet Season"})
    assert calls[1][2] == ({"e1": "Dry"},)
    assert all(
        options == {"check_gpkg_layers": False, "io_workers": 4, "maximum_directory_entries": 50_000}
        for _, _, _, options in calls
    )


def test_standalone_reports_unavailable_validator_as_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An exception in the sole validator must not become a successful check."""

    def unavailable(*_args: object, **_kwargs: object) -> _SuccessfulGroup:
        message = "public validation unavailable"
        raise RuntimeError(message)

    monkeypatch.setattr(input_preflight, "check_simulation_inputs", unavailable)
    _, main = _load_wrapper()
    source = tmp_path / "runs.bat"
    source.write_text("TUFLOW.exe model.tcf", encoding="utf-8")
    report = tmp_path / "issues.csv"
    assert (
        main(
            tcf=None,
            commands=source,
            report=report,
            check_gpkg_layers=True,
            working_directory=tmp_path,
        )
        == 1
    )
    assert "unresolved" in report.read_text(encoding="utf-8-sig")
    assert "public validation unavailable" in report.read_text(encoding="utf-8-sig")
