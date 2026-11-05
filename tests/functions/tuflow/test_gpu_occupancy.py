"""GPU sharing checks use live process data and never require a TUFLOW executable."""

# Worker regressions deliberately exercise internal process inspection directly.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from typing import Any

import pytest

from ryan_library.functions.tuflow import gpu_occupancy


@pytest.fixture(autouse=True)
def active_licences(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gpu_occupancy, "tuflow_licences_in_use", lambda: True)


@pytest.mark.parametrize("usage", [False, None])
def test_zero_or_unverified_licence_usage_skips_all_process_inspection(
    monkeypatch: pytest.MonkeyPatch, usage: bool | None
) -> None:
    monkeypatch.setattr(gpu_occupancy, "tuflow_licences_in_use", lambda: usage)

    def forbidden(**_kwargs: object) -> None:
        pytest.fail("CodeMeter zero/unverified usage must not enumerate processes")

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", forbidden)
    result = gpu_occupancy._local_gpu_occupancy(())
    assert result.assigned == ()
    assert result.unknown_pids == ()
    assert result.inspection_incomplete is (usage is None)


def test_extract_gpu_ids_and_find_conflicts() -> None:
    selected = gpu_occupancy.gpu_ids_from_flags(["-b", "-pu0", "-PU2", "-e1", "01.0p", "-pu2"])
    assert selected == frozenset({0, 2})
    active = (
        gpu_occupancy.TuflowGpuProcess(pid=100, gpu_ids=frozenset({1})),
        gpu_occupancy.TuflowGpuProcess(pid=200, gpu_ids=frozenset({2, 3})),
    )
    assert gpu_occupancy.gpu_conflicts(selected, active) == ((200, frozenset({2})),)
    assert gpu_occupancy.gpu_conflicts({0}, active) == ()


def test_comma_gpu_syntax_is_not_interpreted_as_device_selection() -> None:
    assert gpu_occupancy.gpu_ids_from_flags(["-pu0,1", "-PU2", "-pu3,4", "-pu5"]) == frozenset({2, 5})


def test_only_live_matching_engines_with_explicit_gpu_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeProcess:
        def __init__(self, pid: int, name: str, cmd: list[str]) -> None:
            self.info: dict[str, object] = {"pid": pid, "name": name, "cmdline": cmd}
            self._command = cmd

        def cmdline(self) -> list[str]:
            assert self.info["pid"] not in {20, 40}, "Excluded/unrelated processes must not have command lines read"
            return self._command

        def environ(self) -> dict[str, str]:
            return dict(os.environ)

    processes: list[Any] = [
        FakeProcess(10, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "-b", "-pu0", "run.tcf"]),
        FakeProcess(20, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "-pu1", "run.tcf"]),
        FakeProcess(30, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "run.tcf"]),
        FakeProcess(40, "python.exe", ["python", "run_tuflow_simulations.py"]),
    ]

    def fake_process_iter(**_kwargs: object) -> Iterator[Any]:
        assert _kwargs == {"attrs": ["pid", "name"]}
        return iter(processes)

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    found = gpu_occupancy._local_gpu_occupancy({20})
    assert found.assigned == (gpu_occupancy.TuflowGpuProcess(pid=10, gpu_ids=frozenset({0})),)
    assert found.unknown_pids == (30,)
    assert not found.inspection_incomplete


def test_missing_access_cannot_be_classified_as_free(monkeypatch: pytest.MonkeyPatch) -> None:
    class RestrictedProcess:
        @property
        def info(self) -> dict[str, object]:
            raise gpu_occupancy.psutil.AccessDenied(pid=99)

    def fake_process_iter(**_kwargs: object) -> Iterator[RestrictedProcess]:
        return iter([RestrictedProcess()])

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    found = gpu_occupancy._local_gpu_occupancy(())
    assert not found.assigned
    assert found.inspection_incomplete


def test_no_gpu_flags_means_assignment_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.info: dict[str, object] = {
                "pid": 5,
                "name": "TUFLOW_GPU_w64.exe",
                "cmdline": ["TUFLOW_GPU_w64.exe", "-b", "model.tcf"],
            }

        def cmdline(self) -> list[str]:
            return ["TUFLOW_GPU_w64.exe", "-b", "model.tcf"]

    def fake_process_iter(**_kwargs: object) -> Iterator[FakeProcess]:
        return iter([FakeProcess()])

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    result = gpu_occupancy._local_gpu_occupancy(())
    assert result.assigned == ()
    assert result.unknown_pids == (5,)


@pytest.mark.parametrize(
    ("runner_visible", "runner_order", "external_visible", "external_order", "shared"),
    [
        ("1,0", None, "0,1", None, False),
        (None, "PCI_BUS_ID", None, "FASTEST_FIRST", False),
        ("GPU-first,GPU-second", None, "GPU-second,GPU-first", None, False),
        (None, None, "", None, False),
        ("1,0", "PCI_BUS_ID", "1,0", "PCI_BUS_ID", True),
        (None, None, None, None, True),
    ],
)
def test_ordinals_require_a_shared_cuda_selection_environment(
    monkeypatch: pytest.MonkeyPatch,
    runner_visible: str | None,
    runner_order: str | None,
    external_visible: str | None,
    external_order: str | None,
    shared: bool,
) -> None:
    for key, value in (("CUDA_VISIBLE_DEVICES", runner_visible), ("CUDA_DEVICE_ORDER", runner_order)):
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    external: dict[str, str] = {}
    for key, value in (("CUDA_VISIBLE_DEVICES", external_visible), ("CUDA_DEVICE_ORDER", external_order)):
        if value is not None:
            external[key] = value

    class ExternalProcess:
        def __init__(self) -> None:
            self.info: dict[str, object] = {
                "pid": 77,
                "name": "TUFLOW_iSP_w64.exe",
                "cmdline": ["TUFLOW_iSP_w64.exe", "-pu1", "model.tcf"],
            }

        def cmdline(self) -> list[str]:
            return ["TUFLOW_iSP_w64.exe", "-pu1", "model.tcf"]

        def environ(self) -> dict[str, str]:
            return external

    def fake_process_iter(**_kwargs: object) -> Iterator[ExternalProcess]:
        return iter([ExternalProcess()])

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    snapshot = gpu_occupancy._local_gpu_occupancy(())
    assert bool(snapshot.assigned) is shared
    assert snapshot.unknown_pids == (() if shared else (77,))


@pytest.mark.parametrize("failure", ["denied", "exited"])
def test_unreadable_cuda_environment_is_not_assumed_default(monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    class ExternalProcess:
        def __init__(self) -> None:
            self.info: dict[str, object] = {
                "pid": 88,
                "name": "TUFLOW_iSP_w64.exe",
                "cmdline": ["TUFLOW_iSP_w64.exe", "-pu1", "model.tcf"],
            }

        def cmdline(self) -> list[str]:
            return ["TUFLOW_iSP_w64.exe", "-pu1", "model.tcf"]

        def environ(self) -> dict[str, str]:
            if failure == "exited":
                raise gpu_occupancy.psutil.NoSuchProcess(pid=88)
            raise gpu_occupancy.psutil.AccessDenied(pid=88)

    def fake_process_iter(**_kwargs: object) -> Iterator[ExternalProcess]:
        return iter([ExternalProcess()])

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    snapshot = gpu_occupancy._local_gpu_occupancy(())
    assert snapshot.assigned == ()
    assert snapshot.unknown_pids == ((88,) if failure == "denied" else ())


@pytest.mark.parametrize("failure", ["timeout", "unavailable", "invalid_output", "success"])
def test_optional_check_has_one_budget_and_fails_without_blocking(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    def run(args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        assert 0 < options["timeout"] < 1.0
        assert json.loads(args[3]) == [7, 8]
        assert options["stderr"] == subprocess.DEVNULL
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, options["timeout"])
        if failure == "unavailable":
            message = "worker unavailable"
            raise OSError(message)
        output = (
            "not JSON"
            if failure == "invalid_output"
            else json.dumps(
                {"assigned": [[99, [0]]], "unknown_pids": [100], "inspection_incomplete": False, "detail": ""}
            )
        )
        return subprocess.CompletedProcess(args, 0, stdout=output)

    monkeypatch.setattr(gpu_occupancy.subprocess, "run", run)
    result = gpu_occupancy.local_gpu_occupancy(exclude_pids={8, 7})
    if failure == "success":
        assert result.assigned == (gpu_occupancy.TuflowGpuProcess(99, frozenset({0})),)
        assert result.unknown_pids == (100,)
        assert not result.inspection_incomplete
    else:
        assert result.assigned == ()
        assert result.inspection_incomplete
        assert "continuing launches" in result.detail
        assert "CmWebAdmin.exe" in result.detail
