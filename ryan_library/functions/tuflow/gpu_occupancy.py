"""Read-only detection of GPU sharing between locally running TUFLOW processes.

Process IDs and live command lines are used rather than incomplete/stale TLFs:
the TUFLOW activity tracker records GPU_Device_IDs in TLFs, but a TLF by
itself does not prove a model is still running. CodeMeter allocations are
licences, not a reliable map of GPUs on this local computer.
"""

from __future__ import annotations

import os
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import PureWindowsPath
from typing import cast

import psutil

from .codemeter import tuflow_licences_in_use
from .command_plan import is_gpu_switch


@dataclass(frozen=True, slots=True)
class TuflowGpuProcess:
    """Live TUFLOW process with selected IDs in a verified shared CUDA namespace."""

    pid: int
    gpu_ids: frozenset[int]


@dataclass(frozen=True, slots=True)
class GpuOccupancySnapshot:
    """Local TUFLOW GPU assignments; unknown assignments are not assumed free."""

    assigned: tuple[TuflowGpuProcess, ...]
    unknown_pids: tuple[int, ...]
    inspection_incomplete: bool = False
    detail: str = ""


def gpu_ids_from_flags(flags: Sequence[str]) -> frozenset[int]:
    """Return distinct GPU IDs from TUFLOW -puN command-line arguments."""
    return frozenset(int(flag.strip()[3:]) for flag in flags if is_gpu_switch(flag.strip()))


def _is_tuflow_executable(name: str, command: Sequence[str]) -> bool:
    """Match the running engine executable, not a Python TUFLOW helper script."""
    executable = PureWindowsPath(name).name.casefold()
    if not executable and command:
        executable = PureWindowsPath(command[0]).name.casefold()
    return executable.startswith("tuflow") and executable.endswith(".exe")


def _cuda_selection(environment: Mapping[str, str]) -> tuple[str | None, str | None]:
    """Compare exact CUDA selectors; do not guess equivalence of different spellings."""
    if os.name == "nt":
        environment = {key.upper(): value for key, value in environment.items()}
    return environment.get("CUDA_VISIBLE_DEVICES"), environment.get("CUDA_DEVICE_ORDER")


def _shares_cuda_namespace(process: psutil.Process) -> bool:
    """Only compare ordinals when both processes have the same CUDA selectors.

    Differing selectors may reorder or hide physical GPUs. Missing permission
    to read the external environment is uncertainty, not evidence of defaults.
    Environment values are inspected locally and never logged or retained.
    """
    try:
        return _cuda_selection(process.environ()) == _cuda_selection(os.environ)
    except (psutil.AccessDenied, OSError, ValueError):
        return False


def local_gpu_occupancy(*, exclude_pids: Collection[int] = ()) -> GpuOccupancySnapshot:
    """Inspect currently running local TUFLOW executable command lines.

    TUFLOW runs without explicit -puN switches, or with different/unreadable
    CUDA selection environments, are unknown. Command-line ordinals are only
    comparable after verifying matching CUDA_VISIBLE_DEVICES and CUDA_DEVICE_ORDER.
    A fresh CodeMeter summary confirming zero TUFLOW licence use avoids process
    enumeration. Only confirmed active usage triggers local inspection; an
    unavailable or unverified summary returns incomplete inspection immediately.
    The caller owns snapshot timing. No TUFLOW or CodeMeter process is launched.
    """
    licence_usage = tuflow_licences_in_use()
    if licence_usage is not True:
        return GpuOccupancySnapshot(
            assigned=(),
            unknown_pids=(),
            inspection_incomplete=licence_usage is None,
            detail=(
                "CodeMeter reports no TUFLOW licences in use; GPU process inspection skipped."
                if licence_usage is False
                else (
                    "CodeMeter usage is unavailable or unverified; GPU process inspection skipped, continuing launches.\n"
                    "Check WebAdmin at http://127.0.0.1:22352.\n"
                    "Windows: open Services (services.msc); start CmWebAdmin if stopped (service name: CmWebAdmin.exe).\n"
                    "Optional: set that service's Startup type to Automatic to start WebAdmin after reboot."
                )
            ),
        )
    return _inspect_local_gpu_processes(exclude_pids)


def _inspect_local_gpu_processes(exclude_pids: Collection[int]) -> GpuOccupancySnapshot:
    """Inspect process state only after CodeMeter confirms active licence use."""
    excluded = set(exclude_pids)
    assigned: list[TuflowGpuProcess] = []
    unknown: list[int] = []
    incomplete = False
    try:
        processes = psutil.process_iter(attrs=["pid", "name"])
        for process in processes:
            try:
                info = cast(dict[str, object], process.info)
                pid_value = info.get("pid")
                if not isinstance(pid_value, int) or pid_value in excluded:
                    continue
                name = str(info.get("name") or "")
                if name and not _is_tuflow_executable(name, ()):
                    continue
                command = tuple(process.cmdline())
                if not _is_tuflow_executable(name, command):
                    continue
                if not command:
                    unknown.append(pid_value)
                    continue
                ids = gpu_ids_from_flags(command)
                if ids and _shares_cuda_namespace(process):
                    assigned.append(TuflowGpuProcess(pid=pid_value, gpu_ids=ids))
                else:
                    unknown.append(pid_value)
            except psutil.NoSuchProcess:
                continue
            except (psutil.AccessDenied, OSError, ValueError):
                incomplete = True
    except (psutil.AccessDenied, OSError, ValueError):
        incomplete = True
    return GpuOccupancySnapshot(
        assigned=tuple(sorted(assigned, key=lambda process: process.pid)),
        unknown_pids=tuple(sorted(set(unknown))),
        inspection_incomplete=incomplete,
    )


def gpu_conflicts(
    proposed: Collection[int],
    active: Collection[TuflowGpuProcess],
) -> tuple[tuple[int, frozenset[int]], ...]:
    """Return (PID, common GPUs) for each process that would share a device."""
    requested = set(proposed)
    return tuple(
        (process.pid, frozenset(overlap)) for process in active if (overlap := requested.intersection(process.gpu_ids))
    )
