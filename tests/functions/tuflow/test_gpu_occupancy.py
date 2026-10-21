"""GPU sharing checks use live process data and never require a TUFLOW executable."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from ryan_library.functions.tuflow import gpu_occupancy


def test_extract_gpu_ids_and_find_conflicts() -> None:
    selected = gpu_occupancy.gpu_ids_from_flags(["-b", "-pu0", "-PU2", "-e1", "01.0p", "-pu2"])
    assert selected == frozenset({0, 2})
    active = (
        gpu_occupancy.TuflowGpuProcess(pid=100, gpu_ids=frozenset({1})),
        gpu_occupancy.TuflowGpuProcess(pid=200, gpu_ids=frozenset({2, 3})),
    )
    assert gpu_occupancy.gpu_conflicts(selected, active) == ((200, frozenset({2})),)
    assert gpu_occupancy.gpu_conflicts({0}, active) == ()


def test_only_live_matching_engines_with_explicit_gpu_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeProcess:
        def __init__(self, pid: int, name: str, cmd: list[str]) -> None:
            self.info: dict[str, object] = {"pid": pid, "name": name, "cmdline": cmd}

    processes: list[Any] = [
        FakeProcess(10, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "-b", "-pu0", "run.tcf"]),
        FakeProcess(20, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "-pu1", "run.tcf"]),
        FakeProcess(30, "TUFLOW_iSP_w64.exe", ["Q:\\TUFLOW_iSP_w64.exe", "run.tcf"]),
        FakeProcess(40, "python.exe", ["python", "run_tuflow_simulations.py"]),
    ]

    def fake_process_iter(**_kwargs: object) -> Iterator[Any]:
        return iter(processes)

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    found = gpu_occupancy.local_gpu_occupancy(exclude_pids={20})
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
    found = gpu_occupancy.local_gpu_occupancy()
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

    def fake_process_iter(**_kwargs: object) -> Iterator[FakeProcess]:
        return iter([FakeProcess()])

    monkeypatch.setattr(gpu_occupancy.psutil, "process_iter", fake_process_iter)
    result = gpu_occupancy.local_gpu_occupancy()
    assert result.assigned == ()
    assert result.unknown_pids == (5,)
