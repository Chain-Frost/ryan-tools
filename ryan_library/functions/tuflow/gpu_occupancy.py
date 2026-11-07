"""Read-only detection of GPU sharing between locally running TUFLOW processes.

Process IDs and live command lines are used rather than incomplete/stale TLFs:
the TUFLOW activity tracker records GPU_Device_IDs in TLFs, but a TLF by
itself does not prove a model is still running. CodeMeter allocations are
licences, not a reliable map of GPUs on this local computer.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import PureWindowsPath
from queue import SimpleQueue
from threading import Thread
from typing import TypedDict, cast

import psutil

from .codemeter import tuflow_licences_in_use
from .command_plan import is_gpu_switch

_INSPECTION_BUDGET_SECONDS = 2.0
_CLEANUP_ALLOWANCE_SECONDS = 0.1
_CODEMETER_TROUBLESHOOTING = (
    "Check WebAdmin at http://127.0.0.1:22352.\n"
    "Windows: open Services (services.msc); start CmWebAdmin if stopped (service name: CmWebAdmin.exe).\n"
    "Optional: set that service's Startup type to Automatic to start WebAdmin after reboot."
)
_PROCESS_INSPECTION_WORKER = """
import json, sys
from ryan_library.functions.tuflow import gpu_occupancy
snapshot = getattr(gpu_occupancy, sys.argv[2])(json.loads(sys.argv[1]))
print(json.dumps({
    'assigned': [[item.pid, sorted(item.gpu_ids)] for item in snapshot.assigned],
    'unknown_pids': list(snapshot.unknown_pids),
    'inspection_incomplete': snapshot.inspection_incomplete,
    'detail': snapshot.detail,
}))
"""


class _ProcessSnapshotPayload(TypedDict):
    assigned: list[tuple[int, list[int]]]
    unknown_pids: list[int]
    inspection_incomplete: bool
    detail: str


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
    A fresh CodeMeter summary and a local process scan run concurrently in an
    owned subprocess. When the summary reports zero use or is unverified, the
    scan result is discarded without waiting for it. Only confirmed active
    usage permits scan results to affect scheduling. Both checks share a
    two-second budget; the subprocess is killed and reaped on timeout.
    No TUFLOW or CodeMeter process is launched.
    """
    return _bounded_gpu_inspection(
        exclude_pids, timeout_seconds=_INSPECTION_BUDGET_SECONDS - _CLEANUP_ALLOWANCE_SECONDS
    )


def _local_gpu_occupancy(exclude_pids: Collection[int]) -> GpuOccupancySnapshot:
    """Overlap the GPU scan with CodeMeter; only active usage authorises its result.

    The scan uses a daemon thread confined to the short-lived inspection
    subprocess. A zero/unverified CodeMeter result never waits for the scan;
    the owning process exits promptly and discards any incomplete work.
    """
    scan_results: SimpleQueue[GpuOccupancySnapshot] = SimpleQueue()

    def scan_processes() -> None:
        try:
            result = _inspect_local_gpu_processes(exclude_pids)
        except Exception as exc:  # noqa: BLE001 - optional probe must not strand its result consumer
            result = GpuOccupancySnapshot(
                assigned=(),
                unknown_pids=(),
                inspection_incomplete=True,
                detail=f"GPU process inspection failed ({type(exc).__name__}); continuing launches.",
            )
        scan_results.put(result)

    Thread(target=scan_processes, name="tuflow-gpu-inspection", daemon=True).start()
    licence_usage = tuflow_licences_in_use()
    if licence_usage is not True:
        return GpuOccupancySnapshot(
            assigned=(),
            unknown_pids=(),
            inspection_incomplete=licence_usage is None,
            detail=(
                "CodeMeter reports no TUFLOW licences in use; parallel GPU process inspection ignored."
                if licence_usage is False
                else (
                    "CodeMeter usage is unavailable or unverified; parallel GPU process inspection ignored, "
                    "continuing launches.\n" + _CODEMETER_TROUBLESHOOTING
                )
            ),
        )
    # The parent owns the two-second deadline and kills/reaps this worker if
    # the scan is still blocked. Do not use Thread.join() or an executor here:
    # waiting for its shutdown would delay the no-licence fast path.
    return scan_results.get()


def _bounded_gpu_inspection(exclude_pids: Collection[int], *, timeout_seconds: float) -> GpuOccupancySnapshot:
    """Bound the whole check without leaving work running at interpreter exit."""
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                _PROCESS_INSPECTION_WORKER,
                json.dumps(sorted(exclude_pids)),
                _local_gpu_occupancy.__name__,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            encoding="utf-8",
            timeout=timeout_seconds,
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        payload = cast(_ProcessSnapshotPayload, json.loads(completed.stdout))
        return GpuOccupancySnapshot(
            assigned=tuple(TuflowGpuProcess(pid, frozenset(ids)) for pid, ids in payload["assigned"]),
            unknown_pids=tuple(payload["unknown_pids"]),
            inspection_incomplete=payload["inspection_incomplete"],
            detail=payload["detail"],
        )
    except subprocess.TimeoutExpired:
        detail = (
            f"Optional CodeMeter/GPU inspection exceeded the {_INSPECTION_BUDGET_SECONDS:g}-second budget; "
            "skipped, continuing launches."
        )
    except (OSError, subprocess.SubprocessError, ValueError, TypeError, KeyError):
        detail = "Optional CodeMeter/GPU inspection unavailable; skipped, continuing launches."
    return GpuOccupancySnapshot(
        assigned=(), unknown_pids=(), inspection_incomplete=True, detail=f"{detail}\n{_CODEMETER_TROUBLESHOOTING}"
    )


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
