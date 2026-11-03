"""Read-only GPU names in CUDA order and recorded TUFLOW device order.

NVIDIA management-tool indexes are deliberately not used as TUFLOW -pu IDs.
Names are presentation metadata, never evidence that a device is available.
"""

from __future__ import annotations

import ctypes
import os
import re
from collections.abc import Mapping, Sequence
from itertools import islice
from pathlib import Path


def local_cuda_device_names() -> dict[int, str]:
    """Enumerate CUDA devices in this process's environment without a solver.

    No context, model or licence is created. CUDA_VISIBLE_DEVICES and
    CUDA_DEVICE_ORDER apply; callers must not use this map for remote/history
    records or processes with a different CUDA environment. Driver absence or
    failure leaves names unknown and does not affect scheduling.
    """
    try:
        driver = ctypes.CDLL("nvcuda.dll" if os.name == "nt" else "libcuda.so.1")
        count = ctypes.c_int()
        if driver.cuInit(0) != 0 or driver.cuDeviceGetCount(ctypes.byref(count)) != 0:
            return {}
        names: dict[int, str] = {}
        buses: dict[int, str] = {}
        for ordinal in range(count.value):
            device = ctypes.c_int()
            name = ctypes.create_string_buffer(256)
            if driver.cuDeviceGet(ctypes.byref(device), ordinal) != 0:
                return {}
            if driver.cuDeviceGetName(name, len(name), device) != 0:
                return {}
            label = name.value.decode("utf-8", errors="replace").strip()
            if label:
                names[ordinal] = label
            bus = ctypes.create_string_buffer(32)
            if driver.cuDeviceGetPCIBusId(bus, len(bus), device) == 0:
                buses[ordinal] = bus.value.decode("ascii", errors="replace")
        duplicates = {label for label in names.values() if list(names.values()).count(label) > 1}
        for ordinal, label in tuple(names.items()):
            if label in duplicates and ordinal in buses:
                names[ordinal] = f"{label}; PCI {buses[ordinal]}"
        return names
    except (OSError, AttributeError):
        return {}


def gpu_device_label(identifier: int, names: Mapping[int, str] | None = None) -> str:
    """Render a TUFLOW switch with a name only when supplied by its context."""
    name = names.get(identifier) if names is not None else None
    return f"-pu{identifier} ({name})" if name else f"-pu{identifier} (name unknown)"


def gpu_group_label(flags: Sequence[str], names: Mapping[int, str] | None = None) -> str:
    """Label an explicit validated runner group or automatic placement."""
    return ", ".join(gpu_device_label(int(flag[3:]), names) for flag in flags) if flags else "Auto"


def recorded_gpu_names(tlf_path: Path) -> dict[int, str]:
    """Read the companion HPC header, conservatively rejecting stale companions.

    Both logs must have modification times within two minutes. A long-running
    model with an older HPC log therefore retains unknown names until both
    files are updated; a name is preferable to a guess only with usable evidence.
    """
    hpc_path = tlf_path.with_suffix(".hpc.tlf")
    try:
        if abs(hpc_path.stat().st_mtime - tlf_path.stat().st_mtime) > 120:
            return {}
        names: dict[int, str] = {}
        with hpc_path.open(encoding="utf-8", errors="replace") as stream:
            for line in islice(stream, 100):
                match = re.match(r"\s*Device\s+(\d+),\s*(.+?),\s*has compute capability\s*=", line)
                if match:
                    identifier = int(match[1])
                    name = match[2].strip()
                    if identifier in names and names[identifier] != name:
                        return {}
                    names[identifier] = name
        return names
    except OSError:
        return {}


def recorded_gpu_label(device_ids: str, names: Mapping[int, str]) -> str:
    """Label only the selected IDs, preserving unrecognised log values."""
    if not re.fullmatch(r"\d+(?:[\s,]+\d+)*", device_ids.strip()):
        return device_ids
    return ", ".join(gpu_device_label(int(value), names) for value in re.findall(r"\d+", device_ids))
