"""Progress is advisory, bounded and specific to the current solver invocation."""

# pyright: reportPrivateUsage=false
# OS-handle guarantees require exercising the private sharing adapter directly.

from __future__ import annotations

import ctypes
import os
import re
import subprocess
import sys
from datetime import datetime
from io import FileIO
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import cast

import pytest

from ryan_library.functions.tuflow import run_progress as progress

_LAUNCH = datetime(2024, 12, 3, 19, 22, 25).timestamp()
_HEADER = "Simulation Started: 2024-Dec-03 19:22\n"
_HPC_HEADER = (
    "HPC version 3.40.0 (Aug 9 2024 11:50:24)\nStart Time = 0:00:00\niStep time maxNu maxNc maxNd nWet vol dt Eff\n"
)
_FIXTURES = Path(__file__).absolute().parents[2] / "test_data" / "tuflow"


def _write(path: Path, text: str, *, modified: float = _LAUNCH + 1) -> None:
    path.write_text(text, encoding="utf-8")
    os.utime(path, (modified, modified))


def _main(time_text: str = "0:00:00", *, start: float = 0, end: float = 3) -> str:
    return (
        f"{_HEADER}Start Time (h):....... {start}\nFinish Time (h):...... {end}\n"
        f"SIM: {time_text} CLK: 0:00:01 CPU: 0:00:01\n"
    )


def _row(clock: str, *, step: int = 100) -> str:
    return f"{step} {clock} 0.001 0.101 0.000 6 0.31756 0.721 99.4%\n"


def test_import_does_not_import_pytuflow() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from ryan_library.functions.tuflow import run_progress; assert 'pytuflow' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_real_pytuflow_resolves_dots_and_out_of_order_placeholders(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow")
    path = tmp_path / "model_~s2~_~s1~_~e1~_001.tcf"
    path.write_text("Log Folder == log\\<<~s1~>>\nStart Time == -1\nEnd Time == 3\n", encoding="utf-8")
    supplied = {"e1": "01.0p", "s1": "EXG", "s2": "5m"}
    result = progress.RunProgressResolver().resolve(path, supplied)
    assert result.tlf_path == tmp_path / "log" / "EXG" / "model_5m_EXG_01.0p_001.tlf", result.detail
    assert result.start_hours == -1
    assert result.end_hours == 3
    assert list(supplied) == ["e1", "s1", "s2"]
    assert result.tlf_path is not None
    assert not result.tlf_path.exists()


def test_real_pytuflow_handles_conditional_log_folder_and_simple_alias(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow")
    path = tmp_path / "model_~s~_001.tcf"
    path.write_text(
        "If Scenario == EXG\nLog Folder == logs_exg\nEnd Time == 5\n"
        "Else\nLog Folder == logs_dev\nEnd Time == 8\nEnd If\n",
        encoding="utf-8",
    )
    resolver = progress.RunProgressResolver()
    existing = resolver.resolve(path, {"s1": "EXG"})
    developed = resolver.resolve(path, {"s1": "DEV"})
    assert existing.tlf_path == tmp_path / "logs_exg" / "model_EXG_001.tlf", existing.detail
    assert existing.end_hours == 5
    assert developed.tlf_path == tmp_path / "logs_dev" / "model_DEV_001.tlf", developed.detail
    assert developed.end_hours == 8


@pytest.mark.parametrize("selection", ["none", "generic", "machine", "both"])
def test_real_pytuflow_resolves_override_log_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selection: str
) -> None:
    pytest.importorskip("pytuflow")
    monkeypatch.setattr("platform.node", lambda: "test-node")
    path = tmp_path / "model_~e1~_001.tcf"
    path.write_text(
        "Set Variable OutputBase == base\nLog Folder == <<OutputBase>>/logs\nStart Time == 0\nEnd Time == 1\n",
        encoding="utf-8",
    )
    for kind, name in (
        ("generic", "_TUFLOW_Override.tcf"),
        ("machine", "_TUFLOW_Override_test-node.tcf"),
    ):
        if selection in (kind, "both"):
            (tmp_path / name).write_text(
                f"Set Variable OutputBase == {tmp_path / kind}\nLog Folder == <<OutputBase>>/logs\n",
                encoding="utf-8",
            )
    result = progress.RunProgressResolver().resolve(path, {"e1": "01.0p"})
    effective = "machine" if selection == "both" else "base" if selection == "none" else selection
    assert result.tlf_path == tmp_path / effective / "logs" / "model_01.0p_001.tlf", result.detail
    assert result.start_hours == 0
    assert result.end_hours == 1
    assert not result.detail


def test_missing_filename_slot_fails_without_importing_parser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "model_~s2~.tcf"
    path.touch()

    def unexpected_import(_name: str) -> ModuleType:
        pytest.fail("Missing filename flags should be rejected before parser import")

    monkeypatch.setattr(progress.importlib, "import_module", unexpected_import)
    result = progress.RunProgressResolver().resolve(path, {"s1": "EXG"})
    assert result.tlf_path is None
    assert "No value for ~s2~" in result.detail


def test_real_pytuflow_mixed_aliases_and_repeated_slot(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow")
    path = tmp_path / "model_~e~_~e1~_~s~_~s1~.tcf"
    path.write_text("Log Folder == log\n", encoding="utf-8")
    result = progress.RunProgressResolver().resolve(path, {"e1": "01.0p", "s1": "EXG"})
    assert result.tlf_path == tmp_path / "log" / "model_01.0p_01.0p_EXG_EXG.tlf", result.detail


def test_real_pytuflow_complex_unordered_context_keeps_full_tlf_name(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow")
    path = tmp_path / "UON_04_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf"
    path.write_text("Log Folder == logs\nStart Time == 0\nEnd Time == 48\n", encoding="utf-8")
    supplied = {
        "e3": "TP01",
        "e2": "00540m",
        "e1": "01.0p",
        "e4": "EnsembleILCL",
        "e5": "CCNear",
        "s1": "EXG",
        "s2": "M3W",
        "s4": "16M",
    }
    before = supplied.copy()
    result = progress.RunProgressResolver().resolve(path, supplied)
    name = "UON_04_M3W_EXG_01.0p_00540m_EnsembleILCL_TP01_CCNear_16M.tlf"
    assert result.tlf_path == tmp_path / "logs" / name, result.detail
    assert result.start_hours == 0
    assert result.end_hours == 48
    assert supplied == before


def test_unavailable_parser_and_unresolved_path_are_advisory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "model.tcf"
    path.touch()

    def missing(_name: str) -> ModuleType:
        message = "PyTUFLOW is unavailable"
        raise ImportError(message)

    monkeypatch.setattr(progress.importlib, "import_module", missing)
    result = progress.RunProgressResolver().resolve(path, {})
    assert result.tlf_path is None
    assert "ImportError" in result.detail

    class Model:
        def context(self, _flags: dict[str, str], *, file_resolution: object) -> SimpleNamespace:
            assert file_resolution == "deferred"
            return SimpleNamespace(output_name=lambda: "model", log_folder_path=lambda: Path("<<UNKNOWN>>"))

    def factory(_path: Path) -> Model:
        return Model()

    def importer(_name: str) -> SimpleNamespace:
        return SimpleNamespace(TCF=factory, FileResolution=SimpleNamespace(DEFERRED="deferred"))

    monkeypatch.setattr(progress.importlib, "import_module", importer)
    result = progress.RunProgressResolver().resolve(path, {})
    assert result.tlf_path is None
    assert "resolve all variables" in result.detail


def test_resolver_caches_model_and_keeps_isodate_unknown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "model.tcf"
    path.touch()
    calls: list[Path] = []

    class Run:
        def output_name(self) -> str:
            return "model"

        def log_folder_path(self) -> Path:
            return tmp_path / "log"

        def find_input(self, *, lhs: str, regex: bool, regex_flags: int) -> list[SimpleNamespace]:
            del lhs, regex, regex_flags
            return [SimpleNamespace(value=datetime(2024, 12, 3))]

    class Model:
        def context(self, _flags: dict[str, str], *, file_resolution: object) -> Run:
            assert file_resolution == "deferred"
            return Run()

    def factory(path: Path) -> Model:
        calls.append(path)
        return Model()

    def importer(_name: str) -> SimpleNamespace:
        return SimpleNamespace(TCF=factory, FileResolution=SimpleNamespace(DEFERRED="deferred"))

    monkeypatch.setattr(progress.importlib, "import_module", importer)
    resolver = progress.RunProgressResolver()
    for event in ("a", "b"):
        result = resolver.resolve(path, {"e1": event})
        assert result.tlf_path == tmp_path / "log" / "model.tlf"
        assert result.start_hours is None
        assert result.end_hours is None
    assert calls == [path]


@pytest.mark.parametrize("old_header", [False, True])
def test_preexisting_completion_never_becomes_current_progress(tmp_path: Path, old_header: bool) -> None:
    path = tmp_path / "run.tlf"
    text = _main("3:00:00") + "SIMULATION SUMMARY\nFinal Cumulative ME: -0.25%\n"
    modified = _LAUNCH - 1
    if old_header:
        text = text.replace("2024-Dec-03", "2024-Dec-02")
        modified = _LAUNCH + 10  # A freshly copied old log is still old.
    _write(path, text, modified=modified)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH, end_hours=8)
    for _ in range(2):
        result = reader.poll()
        assert result.state == "unverified"
        assert result.model_hours is None
        assert result.end_hours == 8
        assert result.last_log_update is None
        assert result.phase is None
        assert result.timestep_seconds is None
        assert result.final_mass_error_percent is None


def test_coarse_mtime_requires_observed_post_launch_change(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    coarse = _LAUNCH - 0.5
    _write(path, _main(), modified=coarse)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().state == "unverified"
    _write(path, _main() + "SIM: 1:00:00\n", modified=coarse)
    result = reader.poll()
    assert result.state == "live"
    assert result.model_hours == 1


def test_missing_then_created_log_is_observed_even_with_coarse_mtime(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().state == "waiting"
    _write(path, _main("0:30:00"), modified=_LAUNCH - 0.5)
    assert reader.poll().model_hours == 0.5


def test_partial_rows_wait_for_newline_and_clock_cpu_are_not_model_time(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    text = _main("1:00:00") + "Writing HPC Output at: 0:59:00 Clock: 9:00:00 CPU: 8:00:00\nSIM: 2:00:00"
    _write(path, text)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 1
    _write(path, text + "\n", modified=_LAUNCH + 2)
    assert reader.poll().model_hours == 2


@pytest.mark.parametrize(
    ("start", "end", "clock", "expected"),
    [(-72, 3, "-0:00:57", -57 / 3600), (1, 3, "2:00:00", 2), (0, 72, "49:30:00", 49.5)],
)
def test_negative_restart_and_multiday_model_hours(
    tmp_path: Path, start: float, end: float, clock: str, expected: float
) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main(clock, start=start, end=end))
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.start_hours == start
    assert result.end_hours == end
    assert result.model_hours == pytest.approx(expected)


def test_effective_timing_wins_over_later_echo_and_cli_override_waits(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main("1:00:00", end=2) + "End Time (h) == 3.\n")
    assert progress.LogProgressReader(path, started_at=_LAUNCH).poll().end_hours == 2
    _write(path, _HEADER + "End Time (h) == 3.\nSIM: 1:00:00\n")
    reader = progress.LogProgressReader(path, started_at=_LAUNCH, allow_command_end=False)
    assert reader.poll().end_hours is None
    _write(path, _HEADER + "End Time (h) == 3.\nEnd Time (h): 2.\nSIM: 1:30:00\n")
    assert reader.poll().end_hours == 2


def test_main_and_fresh_hpc_use_furthest_time_but_ignore_old_companion(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("0:30:00"))
    _write(companion, _HPC_HEADER + _row("3:00:00"), modified=_LAUNCH - 1)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 0.5
    _write(companion, _HPC_HEADER + _row("1:00:00"), modified=_LAUNCH + 2)
    assert reader.poll().model_hours == 1
    _write(path, _main("3:00:00"), modified=_LAUNCH + 3)
    _write(companion, _HPC_HEADER + _row("2:59:37"), modified=_LAUNCH + 3)
    # The replaced main makes its companion uncertain until a later update;
    # either way the actual main's final time must remain visible.
    assert reader.poll().model_hours == 3


def test_hpc_partial_rows_and_truncation_do_not_retain_later_progress(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("0:15:00"))
    text = _HPC_HEADER + _row("2:00:00") + _row("2:30:00").rstrip("\n")
    _write(companion, text)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 2
    _write(companion, text + "\n", modified=_LAUNCH + 2)
    assert reader.poll().model_hours == 2.5
    _write(companion, _HPC_HEADER + _row("0:30:00"), modified=_LAUNCH + 3)
    assert reader.poll().model_hours == 0.5


@pytest.mark.parametrize("remove_first", [False, True])
def test_main_generation_change_invalidates_unchanged_companion(tmp_path: Path, remove_first: bool) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("1:00:00") + "padding\n" * 50)
    _write(companion, _HPC_HEADER + _row("2:00:00"))
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 2
    if remove_first:
        path.unlink()
        assert reader.poll().model_hours is None
    _write(path, _main(), modified=_LAUNCH + 3)
    assert reader.poll().model_hours == 0
    assert reader.poll().model_hours == 0
    _write(companion, _HPC_HEADER + _row("0:30:00"), modified=_LAUNCH + 4)
    assert reader.poll().model_hours == 0.5


def test_sharing_violation_skips_poll_and_retries_only_when_polled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main("1:00:00"))
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 1
    original = progress._open_shared_binary
    attempts = 0

    def locked(_candidate: Path) -> FileIO:
        nonlocal attempts
        attempts += 1
        message = "The writer currently denies read sharing"
        raise PermissionError(message)

    _write(path, _main("2:00:00"), modified=_LAUNCH + 2)
    monkeypatch.setattr(progress, "_open_shared_binary", locked)
    result = reader.poll()
    assert result.state == "unavailable"
    assert result.model_hours == 1  # Explicitly labelled last-known observation.
    assert attempts == 1
    monkeypatch.setattr(progress, "_open_shared_binary", original)
    assert reader.poll().model_hours == 2


def test_hpc_sharing_violation_retains_verified_sample_and_age(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("0:30:00", end=4))
    _write(companion, _HPC_HEADER + _row("3:00:00"), modified=_LAUNCH + 2)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    original = Path.stat
    first = reader.poll()
    assert first.model_hours == 3

    def denied(candidate: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if candidate == companion:
            message = "Sharing violation"
            raise PermissionError(message)
        return original(candidate, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", denied)
    result = reader.poll()
    assert result.model_hours == 3
    assert result.last_log_update == first.last_log_update
    assert result.state == "unavailable"
    assert "HPC log" in result.detail
    monkeypatch.setattr(Path, "stat", original)
    assert reader.poll().model_hours == 3
    assert reader.poll().state == "live"


@pytest.mark.parametrize("different_stat_metadata", [False, True])
def test_reads_are_bounded_closed_and_skipped_when_stat_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, different_stat_metadata: bool
) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main() + "unchanged model detail\n" * 100_000 + "SIM: 1:00:00\n")
    original = progress._open_shared_binary
    original_fstat = os.fstat
    requests: list[int] = []
    handles: list[FileIO] = []

    def handle_metadata(descriptor: int) -> os.stat_result:
        value = original_fstat(descriptor)
        # Windows stat(path) and fstat(fd) can disagree on ctime semantics.
        # Deliberately distinct identity representations also prove that the
        # cache compares observations obtained through the same API.
        return cast(
            os.stat_result,
            SimpleNamespace(
                st_dev=value.st_dev + 1,
                st_ino=value.st_ino + 1,
                st_size=value.st_size,
                st_mtime_ns=value.st_mtime_ns,
                st_ctime_ns=value.st_ctime_ns + 1_000_000_000,
            ),
        )

    if different_stat_metadata:
        monkeypatch.setattr(os, "fstat", handle_metadata)

    class CountedFile:
        def __init__(self, source: FileIO) -> None:
            self.source = source

        def __enter__(self) -> CountedFile:
            return self

        def __exit__(self, *_args: object) -> None:
            self.source.close()

        def fileno(self) -> int:
            return self.source.fileno()

        def read(self, size: int) -> bytes:
            requests.append(size)
            return self.source.read(size)

        def seek(self, offset: int) -> int:
            return self.source.seek(offset)

    def tracked(candidate: Path) -> FileIO:
        source = original(candidate)
        handles.append(source)
        return cast(FileIO, CountedFile(source))

    monkeypatch.setattr(progress, "_open_shared_binary", tracked)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().model_hours == 1
    assert requests == [64 * 1024, 64 * 1024]
    assert all(source.closed for source in handles)
    assert reader.poll().model_hours == 1
    assert requests == [64 * 1024, 64 * 1024]
    with path.open("a", encoding="utf-8") as writer:
        writer.write("SIM: 2:00:00\n")
    os.utime(path, (_LAUNCH + 2, _LAUNCH + 2))
    assert reader.poll().model_hours == 2
    assert requests == [64 * 1024] * 4
    assert all(source.closed for source in handles)
    assert reader.poll().model_hours == 2
    assert requests == [64 * 1024] * 4


def test_path_only_metadata_change_preserves_verified_coarse_timestamp_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "run.tlf"
    coarse = _LAUNCH - 0.5
    _write(path, _main(), modified=coarse)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().state == "unverified"
    _write(path, _main() + "SIM: 1:00:00\n", modified=coarse)
    verified = reader.poll()
    assert verified.state == "live"
    original_stat = Path.stat

    def path_metadata(candidate: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        value = original_stat(candidate, follow_symlinks=follow_symlinks)
        return cast(
            os.stat_result,
            SimpleNamespace(
                st_dev=value.st_dev,
                st_ino=value.st_ino,
                st_size=value.st_size,
                st_mtime_ns=value.st_mtime_ns,
                st_ctime_ns=value.st_ctime_ns + 1,
            ),
        )

    monkeypatch.setattr(Path, "stat", path_metadata)
    # A path observation can change independently of the handle sampled for the
    # read. Rereading that same verified generation must not revoke freshness.
    assert reader.poll() == verified
    assert reader.poll() == verified


def test_model_record_split_between_contiguous_windows_is_retained(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    prefix = _main()
    text = prefix + "x" * (64 * 1024 - len(prefix) - 5) + "\nSIM: 2:00:00\n"
    _write(path, text)
    assert progress.LogProgressReader(path, started_at=_LAUNCH).poll().model_hours == 2


@pytest.mark.parametrize(
    ("relative", "start", "end"),
    [
        ("tutorials/Module_01/runs/log/M01_5m_001.tlf", 0, 3),
        ("tutorials/Module_07/runs/log/M07_5m_001.tlf", 0, 3),
        ("TUFLOW_Example_Model_Dataset/log/EG06_004.tlf", 1, 3),
    ],
)
def test_real_fixture_replay(tmp_path: Path, relative: str, start: float, end: float) -> None:
    original = _FIXTURES / relative
    if not original.is_file():
        pytest.skip("The scoped TUFLOW test-data submodule is unavailable")
    path = tmp_path / original.name
    text = re.sub(r"Simulation Started:[^\n]*\n", _HEADER, original.read_text(encoding="utf-8"), count=1)
    _write(path, text)
    companion = original.with_suffix(".hpc.tlf")
    if companion.is_file():
        _write(path.with_suffix(".hpc.tlf"), companion.read_text(encoding="utf-8"))
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.state == "live"
    assert result.start_hours == start
    assert result.end_hours == end
    assert result.model_hours == end


def test_snapshot_new_fields_preserve_existing_positional_arguments() -> None:
    snapshot = progress.LogProgressSnapshot(0, 3, 1, _LAUNCH, "live", "Observed")
    assert snapshot.phase is None
    assert snapshot.timestep_seconds is None
    assert snapshot.final_mass_error_percent is None


def test_phases_require_markers_and_advance_within_a_generation(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    text = _HEADER
    _write(path, text)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().phase is None
    text += "Initialising TUFLOW Licence Settings...\n"
    _write(path, text)
    assert reader.poll().phase == "initialising"
    text += "SIM: 0:00:00\nWriting HPC Output at: 0:00:00 Clock: 1:00:00\n"
    _write(path, text)
    assert reader.poll().phase == "solving"
    _write(companion, "Initialising model ...\n")
    assert reader.poll().phase == "solving"
    text += "Finished HPC Solution.\n"
    _write(path, text)
    assert reader.poll().phase == "writing_results"
    _write(companion, _HPC_HEADER + _row("0:30:00"))
    assert reader.poll().phase == "writing_results"
    # These earlier-phase records can also appear in replayed windows.
    text += "Initialising model ...\nSIM: 0:15:00\n"
    _write(path, text)
    assert reader.poll().phase == "writing_results"


@pytest.mark.parametrize(
    "marker",
    [
        "Finished Quadtree Solution.",
        "Run Finished ..... Steps completed: 37",
        "Writing maximums...",
        "Closing map output files...",
        "SIMULATION SUMMARY",
        "Simulation FINISHED",
    ],
)
def test_explicit_final_results_markers_set_phase(tmp_path: Path, marker: str) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main("3:00:00") + marker + "\n")
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.phase == "writing_results"
    assert result.final_mass_error_percent is None


@pytest.mark.parametrize(
    ("header", "row", "expected"),
    [
        ("iStep time maxNu maxNc maxNd nWet vol dt Eff", "100 1:00:00 .1 .2 .3 6 .31 .721 99.4%", 0.721),
        ("iStep time dt maxNu maxNc maxNd nWet vol Eff", "100 1:00:00 1.25 .1 .2 .3 6 .31 99.4%", 1.25),
        ("iStep time maxNu dt maxNc maxNd nWet vol Eff CPU", "100 1:00:00 .1 2D-1 .2 .3 6 .31 99% 4", 0.2),
        ("ISTEP TIME MAXNU MAXNC MAXND NWET VOL DT EFF", "100 1:00:00 .1 .2 .3 6 .31 .25 99.4%", 0.25),
        ("iStep time maxNu maxNc maxNd nWet vol Eff", "100 1:00:00 .1 .2 .3 6 .31 99.4%", None),
        ("iStep time maxNu maxNc maxNd nWet vol dt dt", "100 1:00:00 .1 .2 .3 6 .31 .25 .5", None),
    ],
)
def test_hpc_timestep_uses_unique_labelled_column(
    tmp_path: Path, header: str, row: str, expected: float | None
) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main())
    _write(path.with_suffix(".hpc.tlf"), header + "\n" + row + "\n")
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.model_hours == 1
    assert result.phase == "solving"
    assert result.timestep_seconds == expected


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "1e999", "0", "-0.5"])
def test_hpc_nonfinite_or_nonpositive_timestep_is_unknown(tmp_path: Path, raw: str) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main())
    _write(path.with_suffix(".hpc.tlf"), _HPC_HEADER + _row("1:00:00").replace("0.721", raw))
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.model_hours == 1
    assert result.timestep_seconds is None


def test_hpc_timestep_waits_for_complete_row_and_uses_latest_step(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main())
    text = _row("1:00:00")
    _write(companion, text)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().timestep_seconds is None  # No observed table header.
    text += _HPC_HEADER + _row("1:00:00", step=200).replace("0.721", "0.125")
    _write(companion, text)
    assert reader.poll().timestep_seconds == 0.125
    text += _row("1:00:00", step=300).replace("0.721", "0.350").rstrip("\n")
    _write(companion, text)
    assert reader.poll().timestep_seconds == 0.125  # The writer has not ended this record.
    text += "\n"
    _write(companion, text)
    assert reader.poll().timestep_seconds == 0.350
    text += "400 1:00:01 .1 .2 .3 6 .31\n"  # A truncated row, even with a newline.
    _write(companion, text)
    assert reader.poll().timestep_seconds == 0.350
    text += _row("1:00:01", step=400).replace("0.721", "nan")
    _write(companion, text)
    assert reader.poll().timestep_seconds is None


def test_scrolled_out_phase_and_timestep_do_not_regress_on_replayed_records(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    text = _main() + "padding\n" * 10_000 + "Finished HPC Solution.\n"
    hpc_text = _HPC_HEADER + _row("0:01:00") + "padding\n" * 10_000
    hpc_text += _row("2:00:00", step=200).replace("0.721", "0.250")
    _write(path, text)
    _write(companion, hpc_text)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    first = reader.poll()
    assert first.phase == "writing_results"
    assert first.timestep_seconds == 0.250
    _write(path, text + "padding\n" * 30_000 + "SIM: 0:01:00\n", modified=_LAUNCH + 2)
    _write(companion, hpc_text + "padding\n" * 30_000 + _row("0:01:00"), modified=_LAUNCH + 2)
    result = reader.poll()
    assert result.phase == "writing_results"
    assert result.timestep_seconds == 0.250


@pytest.mark.parametrize(("raw", "expected"), [("-0.25", -0.25), ("+0.4", 0.4), ("0.00", 0.0), ("-2D-1", -0.2)])
def test_final_mass_error_keeps_explicit_signed_percent(tmp_path: Path, raw: str, expected: float) -> None:
    path = tmp_path / "run.tlf"
    _write(path, _main() + f"Final Cumulative ME:   {raw}%\n")
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.final_mass_error_percent == expected
    assert result.phase == "writing_results"


@pytest.mark.parametrize("raw", ["nan%", "inf%", "1e999%", "-0.25", "-0.25% extra"])
def test_ambiguous_or_nonfinite_final_mass_error_stays_unknown(tmp_path: Path, raw: str) -> None:
    path = tmp_path / "run.tlf"
    text = _main() + f"Final Cumulative ME: {raw}\n"
    text += "CE -0.3% -0.1% -0.2%\nCumulative ME: -0.25%\nVolume Error (m3): -23 or -0.3% of Volume In + Out\n"
    _write(path, text)
    _write(path.with_suffix(".hpc.tlf"), "Final Cumulative ME: -0.75%\n")
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.final_mass_error_percent is None
    assert result.phase == "solving"


@pytest.mark.parametrize("remove_first", [False, True])
def test_new_generation_clears_phase_timestep_and_final_mass_error(tmp_path: Path, remove_first: bool) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("3:00:00") + "SIMULATION SUMMARY\nFinal Cumulative ME: -0.25%\n")
    _write(companion, _HPC_HEADER + _row("2:59:59"))
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    first = reader.poll()
    assert first.phase == "writing_results"
    assert first.timestep_seconds == 0.721
    assert first.final_mass_error_percent == -0.25
    if remove_first:
        path.unlink()
        missing = reader.poll()
        assert missing.phase is None
        assert missing.timestep_seconds is None
        assert missing.final_mass_error_percent is None
    _write(path, _HEADER + "Initialising model ...\n", modified=_LAUNCH + 2)
    reset = reader.poll()
    assert reset.phase == "initialising"
    assert reset.timestep_seconds is None
    assert reset.final_mass_error_percent is None
    _write(companion, _HPC_HEADER + _row("0:01:00").replace("0.721", "0.125"), modified=_LAUNCH + 3)
    current = reader.poll()
    assert current.phase == "solving"
    assert current.timestep_seconds == 0.125
    assert current.final_mass_error_percent is None
    _write(companion, "Initialising model ...\n", modified=_LAUNCH + 4)
    truncated = reader.poll()
    assert truncated.timestep_seconds is None


@pytest.mark.parametrize("blocked_source", ["main", "hpc"])
def test_sharing_conflicts_retain_verified_telemetry_and_original_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked_source: str
) -> None:
    path = tmp_path / "run.tlf"
    companion = path.with_suffix(".hpc.tlf")
    _write(path, _main("3:00:00") + "SIMULATION SUMMARY\nFinal Cumulative ME: -0.25%\n")
    _write(companion, _HPC_HEADER + _row("2:59:59"), modified=_LAUNCH + 2)
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    first = reader.poll()
    original = Path.stat

    def denied(candidate: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if candidate == (path if blocked_source == "main" else companion):
            message = "The writer temporarily denies shared reading"
            raise PermissionError(message)
        return original(candidate, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", denied)
    result = reader.poll()
    assert result.state == "unavailable"
    assert result.phase == first.phase == "writing_results"
    assert result.timestep_seconds == first.timestep_seconds == 0.721
    assert result.final_mass_error_percent == first.final_mass_error_percent == -0.25
    assert result.last_log_update == first.last_log_update


@pytest.mark.parametrize(
    ("relative", "timestep", "mass_error"),
    [
        ("tutorials/Module_01/runs/log/M01_5m_001.tlf", 0.674, 0.0),
        ("tutorials/Module_07/runs/log/M07_5m_001.tlf", 0.682, 0.0),
        ("tutorials/Module_11/runs/log/M11_5m_001.tlf", 0.845, -0.25),
        ("tutorials/Module_11/runs/log/M11_5m_002.tlf", 0.845, -0.20),
    ],
)
def test_real_fixture_final_telemetry(tmp_path: Path, relative: str, timestep: float, mass_error: float) -> None:
    original = _FIXTURES / relative
    if not original.is_file():
        pytest.skip("The scoped TUFLOW test-data submodule is unavailable")
    path = tmp_path / original.name
    text = re.sub(r"Simulation Started:[^\n]*\n", _HEADER, original.read_text(encoding="utf-8"), count=1)
    _write(path, text)
    _write(path.with_suffix(".hpc.tlf"), original.with_suffix(".hpc.tlf").read_text(encoding="utf-8"))
    result = progress.LogProgressReader(path, started_at=_LAUNCH).poll()
    assert result.phase == "writing_results"
    assert result.timestep_seconds == pytest.approx(timestep)
    assert result.final_mass_error_percent == pytest.approx(mass_error)


def test_real_fixture_phase_transitions_precede_final_mass_error(tmp_path: Path) -> None:
    original = _FIXTURES / "tutorials/Module_11/runs/log/M11_5m_001.tlf"
    if not original.is_file():
        pytest.skip("The scoped TUFLOW test-data submodule is unavailable")
    text = re.sub(r"Simulation Started:[^\n]*\n", _HEADER, original.read_text(encoding="utf-8"), count=1)
    path = tmp_path / original.name
    first_sim = text.index("\nSIM:") + 1
    _write(path, text[:first_sim])
    reader = progress.LogProgressReader(path, started_at=_LAUNCH)
    assert reader.poll().phase == "initialising"
    first_sim_end = text.index("\n", first_sim) + 1
    _write(path, text[:first_sim_end], modified=_LAUNCH + 2)
    solving = reader.poll()
    assert solving.phase == "solving"
    assert solving.final_mass_error_percent is None
    final_marker = text.index("Finished HPC Solution.")
    final_marker_end = text.index("\n", final_marker) + 1
    _write(path, text[:final_marker_end], modified=_LAUNCH + 3)
    writing = reader.poll()
    assert writing.phase == "writing_results"
    assert writing.final_mass_error_percent is None
    _write(path, text, modified=_LAUNCH + 4)
    assert reader.poll().final_mass_error_percent == -0.25


@pytest.mark.skipif(sys.platform != "win32", reason="Requires native Windows handle sharing")
def test_windows_reader_allows_long_lived_and_reopened_writer(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    path.write_bytes(b"old\n")
    with path.open("ab", buffering=0) as writer:
        with progress._open_shared_binary(path) as reader:
            assert not os.get_inheritable(reader.fileno())
            writer.write(b"appended\n")
            assert reader.read() == b"old\nappended\n"
    with progress._open_shared_binary(path) as reader:
        with path.open("ab", buffering=0) as writer:
            writer.write(b"reopened\n")
        assert reader.read() == b"old\nappended\nreopened\n"


@pytest.mark.skipif(sys.platform != "win32", reason="Requires native Windows handle sharing")
def test_windows_reader_allows_source_rename_and_replacement_after_close(tmp_path: Path) -> None:
    path = tmp_path / "run.tlf"
    rotated = tmp_path / "rotated.tlf"
    replacement = tmp_path / "replacement.tlf"
    path.write_bytes(b"old\n")
    replacement.write_bytes(b"replacement\n")
    with progress._open_shared_binary(path) as reader:
        assert not os.get_inheritable(reader.fileno())
        # Delete sharing allows the open source to be renamed. Legacy Windows
        # MoveFileExW cannot replace an open destination, even with all share
        # flags, so place the replacement at the now-vacant original name.
        path.rename(rotated)
        replacement.replace(path)
        assert reader.read() == b"old\n"
    assert path.read_bytes() == b"replacement\n"
    assert rotated.read_bytes() == b"old\n"
    with progress._open_shared_binary(path) as reader:
        assert reader.read() == b"replacement\n"
    # A completed sample leaves no retained handle to obstruct replacement.
    rotated.replace(path)
    assert path.read_bytes() == b"old\n"


@pytest.mark.parametrize("failure", ["", "transfer", "file_object"])
def test_windows_open_flags_and_handle_ownership(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    path = tmp_path / "run.tlf"
    path.write_bytes(b"sample\n")
    descriptor = os.open(path, os.O_RDONLY)
    create_calls: list[tuple[object, ...]] = []
    closed_handles: list[int] = []
    transfer_flags: list[int] = []

    class Function:
        argtypes: object = None
        restype: object = None

        def __init__(self, *, close: bool = False) -> None:
            self.close = close

        def __call__(self, *args: object) -> int:
            if self.close:
                closed_handles.append(cast(int, args[0]))
                os.close(cast(int, args[0]))
                return 1
            create_calls.append(args)
            return descriptor

    def load_kernel(_name: str, *, use_last_error: bool) -> SimpleNamespace:
        assert use_last_error
        return SimpleNamespace(CreateFileW=Function(), CloseHandle=Function(close=True))

    def transfer(handle: int, flags: int) -> int:
        assert handle == descriptor
        transfer_flags.append(flags)
        if failure == "transfer":
            message = "Descriptor transfer failed"
            raise OSError(message)
        return descriptor

    def bad_file(*_args: object, **_kwargs: object) -> FileIO:
        message = "File object failed"
        raise OSError(message)

    module = ModuleType("msvcrt")
    monkeypatch.setattr(module, "open_osfhandle", transfer, raising=False)
    monkeypatch.setitem(sys.modules, "msvcrt", module)
    monkeypatch.setattr(ctypes, "WinDLL", load_kernel, raising=False)
    monkeypatch.setattr(progress.sys, "platform", "win32")
    monkeypatch.setattr(os, "O_BINARY", 0x8000, raising=False)
    monkeypatch.setattr(os, "O_NOINHERIT", 0x0080, raising=False)
    if failure == "file_object":
        monkeypatch.setattr(progress, "FileIO", bad_file)
    if failure:
        with pytest.raises(OSError):
            progress._open_shared_binary(path)
    else:
        with progress._open_shared_binary(path) as reader:
            assert reader.read() == b"sample\n"
    assert create_calls[0][1:3] == (0x80000000, 0x7)
    assert transfer_flags == [os.O_RDONLY | 0x8000 | 0x0080]
    assert closed_handles == ([descriptor] if failure == "transfer" else [])
    with pytest.raises(OSError):
        os.fstat(descriptor)
