"""Background file properties must never hold up the workflow coordinator."""

# pyright: reportPrivateUsage=false

from io import StringIO
from pathlib import Path
from threading import Event
from time import monotonic
from types import SimpleNamespace

import pytest

from ryan_library.functions.file_metadata import FileSizeProbe, format_file_size
from ryan_library.functions.live_dashboard import LiveWorkflowDashboard


def test_file_sizes_include_zero_groups_and_unknown_inputs(tmp_path: Path) -> None:
    empty = tmp_path / "empty.dat"
    empty.write_bytes(b"")
    first = tmp_path / "one.dat"
    first.write_bytes(b"a" * 1024)
    second = tmp_path / "two.dat"
    second.write_bytes(b"b" * 1024)
    probe = FileSizeProbe([(empty,), (first, second), (tmp_path / "missing",)])
    probe.start()
    try:
        probe._thread.join(timeout=2)
        assert not probe._thread.is_alive()
        assert probe.poll() == [(1, 0), (2, 2048), (3, None)]
        assert format_file_size(0) == "0 B"
        assert format_file_size(2048) == "2.0 KiB"
    finally:
        probe.cancel()


def test_blocked_network_stat_does_not_block_poll_or_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    entered = Event()
    release = Event()

    def blocked_stat(_path: Path) -> SimpleNamespace:
        entered.set()
        release.wait(timeout=5)
        return SimpleNamespace(st_size=123)

    monkeypatch.setattr(Path, "stat", blocked_stat)
    probe = FileSizeProbe([(Path("network-file"),)])
    probe.start()
    try:
        assert entered.wait(timeout=2)
        start = monotonic()
        assert probe.poll() == []
        probe.cancel()
        assert monotonic() - start < 1
        assert probe._thread.is_alive()
        assert not release.is_set()
    finally:
        release.set()
        probe._thread.join(timeout=2)
    assert not probe._thread.is_alive()
    assert probe.poll() == []  # A cancelled in-flight stat cannot publish late data.


def test_result_queue_is_bounded_and_cancellable(tmp_path: Path) -> None:
    source = tmp_path / "file"
    source.write_bytes(b"data")
    probe = FileSizeProbe([(source,)] * 100, max_pending=1)
    probe.start()
    try:
        # The queue can never accumulate the entire 100-file property scan.
        assert probe._results.maxsize == 1
        probe.cancel()
        probe._thread.join(timeout=2)
        assert not probe._thread.is_alive()
        assert len(probe.poll()) <= 1
    finally:
        probe.cancel()


def test_file_dashboard_consumes_sizes_on_coordinator_refresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "input.dat"
    source.write_bytes(b"x" * 2048)
    dashboard = LiveWorkflowDashboard(title="File properties")
    monkeypatch.setattr(dashboard.console, "_force_terminal", True)
    monkeypatch.setattr(dashboard.console, "_file", StringIO())
    dashboard.set_tasks(["input.dat"], files=[source])
    assert dashboard._file_sizes is None  # Preparing tasks never starts background I/O.
    with dashboard:
        probe = dashboard._file_sizes
        assert probe is not None
        probe._thread.join(timeout=2)
        assert not probe._thread.is_alive()
        dashboard.refresh()
        assert dashboard._tasks[1].metadata["size"] == "2.0 KiB"
        assert dashboard._tasks[1].metadata["size_bytes"] == "2048"
        assert any(column.header == "Size" for column in dashboard.columns)
        dashboard.set_tasks(["non-file work"])
        assert not any(column.header == "Size" for column in dashboard.columns)
    assert dashboard._file_sizes is None


@pytest.mark.parametrize(("enabled", "terminal"), [(False, True), (True, False)])
def test_no_property_reader_for_disabled_or_redirected_dashboards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool, terminal: bool
) -> None:
    dashboard = LiveWorkflowDashboard(title="No property reads", enabled=enabled)
    monkeypatch.setattr(dashboard.console, "_force_terminal", terminal)
    dashboard.set_tasks(["missing-file"], files=[tmp_path / "does-not-exist"])
    assert dashboard._file_sizes is None
