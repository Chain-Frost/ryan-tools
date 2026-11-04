"""GPU labels must preserve the device namespace and evidence source."""

from datetime import datetime
from os import utime
from pathlib import Path
from unittest.mock import patch

from ryan_library.functions.tuflow.gpu_devices import (
    gpu_group_label,
    local_cuda_device_names,
    recorded_gpu_label,
    recorded_gpu_names,
)


def test_missing_driver_keeps_names_unknown() -> None:
    with patch("ryan_library.functions.tuflow.gpu_devices.ctypes.CDLL", side_effect=OSError):
        assert local_cuda_device_names() == {}
    assert gpu_group_label(("-pu1",), {}) == "-pu1 (name unknown)"


def test_recorded_names_do_not_use_current_machine_order(tmp_path: Path) -> None:
    tlf = tmp_path / "model.tlf"
    tlf.write_text("GPU Device IDs == 1, 0,\n", encoding="utf-8")
    hpc = tlf.with_suffix(".hpc.tlf")
    hpc.write_text(
        "Device 0, NVIDIA GeForce RTX 3090, has compute capability = 8.6\n"
        "Device 1, NVIDIA GeForce RTX 3080, has compute capability = 8.6\n",
        encoding="utf-8",
    )
    assert recorded_gpu_label("1, 0", recorded_gpu_names(tlf)) == (
        "-pu1 (NVIDIA GeForce RTX 3080), -pu0 (NVIDIA GeForce RTX 3090)"
    )
    assert recorded_gpu_label("2", recorded_gpu_names(tlf)) == "-pu2 (name unknown)"
    assert recorded_gpu_label("-", recorded_gpu_names(tlf)) == "-"
    utime(hpc, (tlf.stat().st_mtime - 300, tlf.stat().st_mtime - 300))
    assert recorded_gpu_names(tlf) == {}


def test_conflicting_hpc_names_are_not_guessed(tmp_path: Path) -> None:
    tlf = tmp_path / "model.tlf"
    tlf.write_text("GPU Device IDs == 0\n", encoding="utf-8")
    tlf.with_suffix(".hpc.tlf").write_text(
        "Device 0, First card, has compute capability = 8.6\nDevice 0, Other card, has compute capability = 8.6\n",
        encoding="utf-8",
    )
    assert recorded_gpu_names(tlf) == {}


def test_active_run_can_use_older_hpc_header_from_same_run(tmp_path: Path) -> None:
    tlf = tmp_path / "model.tlf"
    tlf.write_text("GPU Device IDs == 0\n", encoding="utf-8")
    hpc = tlf.with_suffix(".hpc.tlf")
    hpc.write_text("Device 0, RTX 3090, has compute capability = 8.6\n", encoding="utf-8")
    now = tlf.stat().st_mtime
    utime(hpc, (now - 3600, now - 3600))
    assert recorded_gpu_names(tlf, started_at=datetime.fromtimestamp(now - 7200)) == {0: "RTX 3090"}
    assert recorded_gpu_names(tlf, started_at=datetime.fromtimestamp(now - 1800)) == {}
