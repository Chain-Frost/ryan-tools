"""Advisory log observation never blocks or controls solver scheduling."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from pathlib import Path
from threading import Event, current_thread

import pytest

from ryan_library.functions.tuflow.command_plan import PlannedRun
from ryan_library.functions.tuflow.run_progress import LogProgressSnapshot, RunLogPrediction
from ryan_library.orchestrators.tuflow import run_progress as progress

type Resolve = Callable[[Path, Mapping[str, str]], RunLogPrediction]
type Poll = Callable[[Path], LogProgressSnapshot]
type ReadOptions = tuple[Path, float, float | None, float | None, bool]


def _run(base: Path, index: int = 1, *, switches: tuple[str, ...] = ()) -> PlannedRun:
    tcf = base / f"model_{index}.tcf"
    return PlannedRun(
        index=index,
        source=base / "runs.txt",
        line_start=index,
        line_end=index,
        tcf=tcf,
        working_directory=base,
        flag_pairs=(("e1", str(index)),),
        arguments=("-b", "-e1", str(index), *switches, str(tcf)),
        executable="TUFLOW.exe",
    )


def _prediction(path: Path, _variables: Mapping[str, str]) -> RunLogPrediction:
    return RunLogPrediction(tlf_path=path.with_suffix(".tlf"), start_hours=0.0, end_hours=3.0)


def _live(_path: Path) -> LogProgressSnapshot:
    return LogProgressSnapshot(start_hours=0.0, end_hours=3.0, model_hours=1.0, state="live")


def _install_io(
    monkeypatch: pytest.MonkeyPatch,
    *,
    resolve: Resolve = _prediction,
    poll: Poll = _live,
    opened: Callable[[ReadOptions], None] | None = None,
) -> None:
    class FakeResolver:
        def resolve(self, tcf_path: Path, run_variables: Mapping[str, str]) -> RunLogPrediction:
            return resolve(tcf_path, run_variables)

    class FakeReader:
        def __init__(
            self,
            tlf_path: Path,
            *,
            started_at: float,
            start_hours: float | None = None,
            end_hours: float | None = None,
            allow_command_end: bool = True,
        ) -> None:
            self.path = tlf_path
            if opened is not None:
                opened((tlf_path, started_at, start_hours, end_hours, allow_command_end))

        def poll(self) -> LogProgressSnapshot:
            return poll(self.path)

    monkeypatch.setattr(progress, "RunProgressResolver", FakeResolver)
    monkeypatch.setattr(progress, "LogProgressReader", FakeReader)


def _updates_until(
    observer: progress.AdvisoryRunProgress,
    predicate: Callable[[dict[int, progress.ProgressUpdate]], bool],
) -> dict[int, progress.ProgressUpdate]:
    received: dict[int, progress.ProgressUpdate] = {}
    deadline = time.monotonic() + 3.0
    wait = Event()
    while time.monotonic() < deadline:
        for update in observer.drain():
            received[update.run_index] = update
        if predicate(received):
            return received
        wait.wait(0.005)
    pytest.fail(f"Expected progress updates did not arrive: {received}")


@pytest.mark.parametrize("interval", [0.0, -1.0, float("inf"), float("nan"), True])
def test_interval_must_be_finite_and_positive(interval: float) -> None:
    with pytest.raises(ValueError, match="finite positive"):
        progress.AdvisoryRunProgress(interval)


def test_register_drain_discard_and_close_do_not_inspect_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def unexpected_resolve(_path: Path, _variables: Mapping[str, str]) -> RunLogPrediction:
        pytest.fail("Caller thread inspected a TCF")

    _install_io(monkeypatch, resolve=unexpected_resolve)
    observer = progress.AdvisoryRunProgress()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    assert observer.drain() == ()
    observer.discard(1)
    observer.close()
    observer.start()
    observer.register(_run(tmp_path), pid=20, started_at=200.0)
    assert observer.drain() == ()


@pytest.mark.parametrize("blocked_stage", ["resolve", "poll"])
def test_slow_io_does_not_block_scheduler_methods_or_publish_after_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, blocked_stage: str
) -> None:
    entered = Event()
    release = Event()
    returned = Event()
    worker_is_daemon: list[bool] = []

    def block() -> None:
        worker_is_daemon.append(current_thread().daemon)
        entered.set()
        release.wait(3.0)
        returned.set()

    def resolve(path: Path, variables: Mapping[str, str]) -> RunLogPrediction:
        if blocked_stage == "resolve":
            block()
        return _prediction(path, variables)

    def poll(path: Path) -> LogProgressSnapshot:
        if blocked_stage == "poll":
            block()
        return _live(path)

    _install_io(monkeypatch, resolve=resolve, poll=poll)
    observer = progress.AdvisoryRunProgress()
    observer.start()
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        assert entered.wait(2.0)
        began = time.monotonic()
        observer.register(_run(tmp_path, 2), pid=20, started_at=101.0)
        observer.discard(1)
        observer.drain()
        observer.close()
        assert time.monotonic() - began < 0.5
        assert not release.is_set()
    finally:
        observer.close()
        release.set()
    assert returned.wait(2.0)
    assert worker_is_daemon == [True]
    assert observer.drain() == ()


def test_replacement_ignores_in_flight_old_invocation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    resolving = Event()
    release = Event()
    resolved: list[Path] = []
    polled: list[Path] = []

    def resolve(path: Path, variables: Mapping[str, str]) -> RunLogPrediction:
        resolved.append(path)
        if len(resolved) == 1:
            resolving.set()
            release.wait(3.0)
        return _prediction(path, variables)

    def poll(path: Path) -> LogProgressSnapshot:
        polled.append(path)
        return _live(path)

    _install_io(monkeypatch, resolve=resolve, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=60.0)
    observer.start()
    old = _run(tmp_path / "old")
    new = _run(tmp_path / "new")
    observer.register(old, pid=10, started_at=100.0)
    try:
        assert resolving.wait(2.0)
        observer.register(new, pid=20, started_at=200.0)
        release.set()
        received = _updates_until(observer, lambda items: 1 in items and items[1].snapshot.state == "live")
        assert received[1].pid == 20
        assert resolved == [old.tcf, new.tcf]
        assert polled == [Path(str(new.tcf)).with_suffix(".tlf")]
    finally:
        observer.close()
        release.set()


def test_polling_runs_without_drain_and_keeps_only_latest_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    third_poll = Event()
    times: list[float] = []
    resolutions: list[Path] = []

    def resolve(path: Path, variables: Mapping[str, str]) -> RunLogPrediction:
        resolutions.append(path)
        return _prediction(path, variables)

    def poll(_path: Path) -> LogProgressSnapshot:
        times.append(time.monotonic())
        if len(times) >= 3:
            third_poll.set()
        return LogProgressSnapshot(model_hours=float(len(times)), state="live")

    _install_io(monkeypatch, resolve=resolve, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=0.04)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        # No drain or dashboard refresh is involved in these observations.
        assert third_poll.wait(2.0)
        received = _updates_until(
            observer,
            lambda items: (
                1 in items and items[1].snapshot.model_hours is not None and items[1].snapshot.model_hours >= 3
            ),
        )
        assert len(received) == 1
        assert len(resolutions) == 1
        assert times[1] - times[0] >= 0.035
        assert times[2] - times[1] >= 0.035
    finally:
        observer.close()


@pytest.mark.parametrize("interval", [60.0, 1e30])
def test_new_registration_and_frequent_drain_do_not_repoll_other_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, interval: float
) -> None:
    polled: list[Path] = []

    def poll(path: Path) -> LogProgressSnapshot:
        polled.append(path)
        return _live(path)

    _install_io(monkeypatch, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=interval)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        _updates_until(observer, lambda items: 1 in items and items[1].snapshot.state == "live")
        for _ in range(100):
            assert observer.drain() == ()
        observer.register(_run(tmp_path, 2), pid=20, started_at=101.0)
        _updates_until(observer, lambda items: 2 in items and items[2].snapshot.state == "live")
        assert polled == [tmp_path / "model_1.tlf", tmp_path / "model_2.tlf"]
    finally:
        observer.close()


def test_resolver_and_reader_failures_are_isolated_to_their_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolved: list[Path] = []

    def resolve(path: Path, variables: Mapping[str, str]) -> RunLogPrediction:
        resolved.append(path)
        if path.name == "model_1.tcf":
            message = "cannot parse control file"
            raise ValueError(message)
        return _prediction(path, variables)

    def poll(path: Path) -> LogProgressSnapshot:
        if path.name == "model_3.tlf":
            message = "network read failed"
            raise RuntimeError(message)
        return _live(path)

    _install_io(monkeypatch, resolve=resolve, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=60.0)
    for index in range(1, 4):
        observer.register(_run(tmp_path, index), pid=index * 10, started_at=100.0)
    observer.start()
    try:
        received = _updates_until(
            observer,
            lambda items: (
                len(items) == 3
                and items[1].snapshot.state == "unavailable"
                and items[2].snapshot.state == "live"
                and items[3].snapshot.state == "unavailable"
            ),
        )
        assert "cannot parse control file" in (received[1].warning or "")
        assert received[2].warning is None
        assert "network read failed" in (received[3].warning or "")
        assert resolved == [tmp_path / f"model_{index}.tcf" for index in range(1, 4)]
    finally:
        observer.close()


def test_unavailable_read_waits_for_its_interval_even_when_scheduler_wakes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    polled: list[Path] = []

    def poll(path: Path) -> LogProgressSnapshot:
        polled.append(path)
        if path.name == "model_1.tlf":
            return LogProgressSnapshot(state="unavailable", detail="writer temporarily denies access")
        return _live(path)

    _install_io(monkeypatch, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=60.0)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        _updates_until(observer, lambda items: 1 in items and items[1].snapshot.state == "unavailable")
        observer.register(_run(tmp_path, 2), pid=20, started_at=101.0)
        _updates_until(observer, lambda items: 2 in items and items[2].snapshot.state == "live")
        for _ in range(100):
            assert observer.drain() == ()
        assert polled == [tmp_path / "model_1.tlf", tmp_path / "model_2.tlf"]
    finally:
        observer.close()


def test_pending_warning_survives_recovery_before_main_thread_drains(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    third_started = Event()
    release_third = Event()
    calls = 0

    def poll(path: Path) -> LogProgressSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            message = "temporary sharing violation"
            raise PermissionError(message)
        if calls == 3:
            third_started.set()
            release_third.wait(3.0)
        return _live(path)

    _install_io(monkeypatch, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=0.02)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        assert third_started.wait(2.0)
        updates = observer.drain()
        assert len(updates) == 1
        assert updates[0].snapshot.state == "live"
        assert "temporary sharing violation" in (updates[0].warning or "")
        assert observer.drain() == ()
    finally:
        observer.close()
        release_third.set()


def test_warnings_are_deduplicated_until_recovery_then_reissued(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    entered = [Event() for _ in range(5)]
    release = [Event() for _ in range(5)]
    calls = 0

    def poll(path: Path) -> LogProgressSnapshot:
        nonlocal calls
        index = calls
        calls += 1
        if index >= len(entered):
            return _live(path)
        entered[index].set()
        release[index].wait(3.0)
        if index in {0, 1, 3}:
            return LogProgressSnapshot(state="unavailable", detail="network unavailable")
        return _live(path)

    _install_io(monkeypatch, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=0.01)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        assert entered[0].wait(2.0)
        observer.drain()
        for index, expected_warning in enumerate(("network unavailable", None, None, "network unavailable")):
            release[index].set()
            assert entered[index + 1].wait(2.0)
            updates = observer.drain()
            assert len(updates) == 1
            assert updates[0].warning == expected_warning
    finally:
        observer.close()
        for event in release:
            event.set()


def test_shared_log_paths_are_suppressed_for_all_overlapping_invocations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    polled = Event()

    def resolve(path: Path, _variables: Mapping[str, str]) -> RunLogPrediction:
        log_path = tmp_path / "logs" / "shared.tlf"
        if path.name == "model_2.tcf":
            log_path = tmp_path / "logs" / "nested" / ".." / "shared.tlf"
        return RunLogPrediction(log_path, start_hours=0.0, end_hours=3.0)

    def poll(path: Path) -> LogProgressSnapshot:
        polled.set()
        return _live(path)

    _install_io(monkeypatch, resolve=resolve, poll=poll)
    observer = progress.AdvisoryRunProgress(interval_seconds=0.01)
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    observer.register(_run(tmp_path, 2), pid=20, started_at=100.0)
    observer.start()
    try:
        received = _updates_until(
            observer,
            lambda items: len(items) == 2 and all(item.snapshot.state == "unverified" for item in items.values()),
        )
        assert sum(item.warning is not None for item in received.values()) == 1
        assert all(item.snapshot.model_hours is None for item in received.values())
        assert not polled.is_set()
        observer.discard(2)
        assert not polled.wait(0.04)
        observer.register(_run(tmp_path, 3), pid=30, started_at=101.0)
        third = _updates_until(observer, lambda items: 3 in items and items[3].snapshot.state == "unverified")
        assert third[3].warning is None
        assert not polled.is_set()
        observer.discard(1)
        observer.discard(3)
        observer.register(_run(tmp_path, 4), pid=40, started_at=102.0)
        fourth = _updates_until(observer, lambda items: 4 in items and items[4].snapshot.state == "live")
        assert fourth[4].warning is None
        assert polled.is_set()
    finally:
        observer.close()


@pytest.mark.parametrize(("switches", "expected_end", "expected_allow"), [((), 3.0, True), (("-ET60",), None, False)])
def test_end_time_override_waits_for_effective_logged_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    switches: tuple[str, ...],
    expected_end: float | None,
    expected_allow: bool,
) -> None:
    opened: list[ReadOptions] = []
    _install_io(monkeypatch, opened=opened.append)
    observer = progress.AdvisoryRunProgress(interval_seconds=60.0)
    observer.start()
    observer.register(_run(tmp_path, switches=switches), pid=10, started_at=100.0)
    try:
        _updates_until(observer, lambda items: 1 in items and items[1].snapshot.state == "live")
        assert opened == [(tmp_path / "model_1.tlf", 100.0, 0.0, expected_end, expected_allow)]
    finally:
        observer.close()


def test_unavailable_prediction_is_not_retried_on_dashboard_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolved: list[Path] = []

    def resolve(path: Path, _variables: Mapping[str, str]) -> RunLogPrediction:
        resolved.append(path)
        return RunLogPrediction(detail="PyTuflow capability is unavailable")

    _install_io(monkeypatch, resolve=resolve)
    observer = progress.AdvisoryRunProgress(interval_seconds=0.01)
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    try:
        received = _updates_until(observer, lambda items: 1 in items and items[1].snapshot.state == "unavailable")
        assert received[1].warning == "PyTuflow capability is unavailable"
        observer.register(_run(tmp_path, 2), pid=20, started_at=101.0)
        _updates_until(observer, lambda items: 2 in items and items[2].snapshot.state == "unavailable")
        for _ in range(100):
            assert observer.drain() == ()
        assert resolved == [tmp_path / "model_1.tcf", tmp_path / "model_2.tcf"]
    finally:
        observer.close()


def test_thread_start_failure_is_an_advisory_update(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class FailedThread:
        def __init__(self, *, target: Callable[[], None], name: str, daemon: bool) -> None:
            del target, name, daemon

        def start(self) -> None:
            message = "no thread available"
            raise RuntimeError(message)

    monkeypatch.setattr(progress, "Thread", FailedThread)
    observer = progress.AdvisoryRunProgress()
    observer.start()
    observer.register(_run(tmp_path), pid=10, started_at=100.0)
    updates = observer.drain()
    observer.close()
    assert len(updates) == 1
    assert updates[0].snapshot.state == "unavailable"
    assert "no thread available" in (updates[0].warning or "")
