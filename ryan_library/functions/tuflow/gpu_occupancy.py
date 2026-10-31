"""Read-only detection of GPU sharing between locally running TUFLOW processes.

Process IDs and live command lines are used rather than incomplete/stale TLFs:
the TUFLOW activity tracker records GPU_Device_IDs in TLFs, but a TLF by
itself does not prove a model is still running. CodeMeter allocations are
licences, not a reliable map of GPUs on this local computer.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import PureWindowsPath
from typing import cast

import psutil

from .command_plan import is_gpu_switch


@dataclass(frozen=True, slots=True)
class TuflowGpuProcess:
    """Live TUFLOW process with its explicitly selected GPU identifiers."""

    pid: int
    gpu_ids: frozenset[int]


@dataclass(frozen=True, slots=True)
class GpuOccupancySnapshot:
    """Local TUFLOW GPU assignments; unknown assignments are not assumed free."""

    assigned: tuple[TuflowGpuProcess, ...]
    unknown_pids: tuple[int, ...]
    inspection_incomplete: bool = False


def gpu_ids_from_flags(flags: Sequence[str]) -> frozenset[int]:
    """Return distinct GPU IDs from TUFLOW -puN command-line arguments."""
    return frozenset(int(flag.strip()[3:]) for flag in flags if is_gpu_switch(flag.strip()))


def _is_tuflow_executable(name: str, command: Sequence[str]) -> bool:
    """Match the running engine executable, not a Python TUFLOW helper script."""
    executable = PureWindowsPath(name).name.casefold()
    if not executable and command:
        executable = PureWindowsPath(command[0]).name.casefold()
    return executable.startswith("tuflow") and executable.endswith(".exe")


def local_gpu_occupancy(*, exclude_pids: Collection[int] = ()) -> GpuOccupancySnapshot:
    """Inspect currently running local TUFLOW executable command lines.

    TUFLOW runs without explicit -puN switches are returned as unknown,
    because their GPU assignment cannot safely be inferred from a command line.
    The caller owns snapshot timing; the simulation runner requests one startup
    snapshot. No TUFLOW or CodeMeter is launched.
    """
    excluded = set(exclude_pids)
    assigned: list[TuflowGpuProcess] = []
    unknown: list[int] = []
    incomplete = False
    try:
        processes = psutil.process_iter(attrs=["pid", "name", "cmdline"])
        for process in processes:
            try:
                info = cast(dict[str, object], process.info)
                pid_value = info.get("pid")
                if not isinstance(pid_value, int) or pid_value in excluded:
                    continue
                command_value = info.get("cmdline")
                command = (
                    tuple(str(arg) for arg in cast(Sequence[object], command_value))
                    if isinstance(command_value, list | tuple)
                    else ()
                )
                if not _is_tuflow_executable(str(info.get("name") or ""), command):
                    continue
                if not command:
                    unknown.append(pid_value)
                    continue
                ids = gpu_ids_from_flags(command)
                if ids:
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
