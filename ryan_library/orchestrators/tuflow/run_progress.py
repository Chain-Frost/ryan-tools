"""Observe active TUFLOW logs without putting filesystem work on the scheduler.

One daemon resolves active runs and polls each log on its own interval. The
scheduler only exchanges immutable snapshots; a slow network read cannot delay
launching, process cleanup or shutdown. Observations never decide run outcomes.
"""

from __future__ import annotations

import math
import os
import re
import time
from dataclasses import dataclass, replace
from pathlib import Path, PurePath, PureWindowsPath
from threading import TIMEOUT_MAX, Event, Lock, Thread

from ...functions.tuflow.command_plan import PlannedRun, normalize_plan_path, partition_model_arguments
from ...functions.tuflow.run_progress import (
    LogProgressReader,
    LogProgressSnapshot,
    RunLogPrediction,
    RunProgressResolver,
)


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    """Latest observation and an optional warning for the caller's log route."""

    run_index: int
    pid: int
    snapshot: LogProgressSnapshot
    warning: str | None = None


@dataclass(slots=True)
class _ActiveRun:
    run: PlannedRun
    pid: int
    started_at: float
    prediction: RunLogPrediction | None = None
    resolved: bool = False
    log_key: PurePath | None = None
    reader: LogProgressReader | None = None
    next_poll: float = 0.0
    ambiguous: bool = False
    allow_command_end: bool = True
    warning_key: tuple[str, str] | None = None


class AdvisoryRunProgress:
    """A bounded, nonblocking bridge between live children and log observations.

    ``register`` accepts only launched children. ``drain`` returns at most one
    update per active run, independently of polling cadence. Callers present
    ``ProgressUpdate.warning`` on their normal logging thread.

    Closing or discarding a run does not wait for an in-flight filesystem call.
    Its result is ignored, and no further reads are scheduled for that entry.
    This observer does not join its daemon during interpreter shutdown.
    """

    def __init__(self, interval_seconds: float = 10.0) -> None:
        if isinstance(interval_seconds, bool) or not math.isfinite(interval_seconds) or interval_seconds <= 0:
            message = "Log progress interval must be a finite positive number"
            raise ValueError(message)
        self._interval = interval_seconds
        self._lock = Lock()
        self._wake = Event()
        self._stop = Event()
        self._thread: Thread | None = None
        self._startup_error: str | None = None
        self._resolver: RunProgressResolver | None = None
        self._active: dict[int, _ActiveRun] = {}
        self._pending: dict[int, ProgressUpdate] = {}
        self._warned_collisions: set[PurePath] = set()

    def start(self) -> None:
        """Start at most one daemon; repeat calls and calls after close do nothing."""
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                return
            thread = Thread(target=self._work, name="tuflow-runner-progress", daemon=True)
            self._thread = thread
        try:
            thread.start()
        except Exception as exc:  # noqa: BLE001 - optional telemetry must never prevent solver execution
            detail = f"Could not start log monitoring: {type(exc).__name__}: {exc}"
            with self._lock:
                self._startup_error = detail
                for entry in self._active.values():
                    self._publish_locked(entry, LogProgressSnapshot(state="unavailable", detail=detail))

    def register(self, run: PlannedRun, *, pid: int, started_at: float) -> None:
        """Register a launched invocation without opening or inspecting any path."""
        entry = _ActiveRun(run=run, pid=pid, started_at=started_at)
        with self._lock:
            if self._stop.is_set():
                return
            self._remove_locked(run.index)
            self._active[run.index] = entry
            if self._startup_error is not None:
                self._publish_locked(entry, LogProgressSnapshot(state="unavailable", detail=self._startup_error))
        self._wake.set()

    def discard(self, run_index: int) -> None:
        """Forget an exited invocation and discard any unconsumed observations."""
        with self._lock:
            self._remove_locked(run_index)
        self._wake.set()

    def drain(self) -> tuple[ProgressUpdate, ...]:
        """Consume latest snapshots; this never triggers reads or logging sinks."""
        with self._lock:
            updates = tuple(self._pending.values())
            self._pending.clear()
        return updates

    def close(self) -> None:
        """Stop scheduling immediately without joining a potentially blocked read."""
        self._stop.set()
        with self._lock:
            self._active.clear()
            self._pending.clear()
            self._warned_collisions.clear()
        self._wake.set()

    def _remove_locked(self, run_index: int) -> None:
        previous = self._active.pop(run_index, None)
        self._pending.pop(run_index, None)
        if previous is None or previous.log_key is None:
            return
        for entry in self._active.values():
            if entry.log_key == previous.log_key:
                return
        self._warned_collisions.discard(previous.log_key)

    def _is_active(self, entry: _ActiveRun) -> bool:
        with self._lock:
            return not self._stop.is_set() and self._active.get(entry.run.index) is entry and not entry.ambiguous

    def _next_entry_locked(self, now: float) -> tuple[_ActiveRun | None, float | None]:
        # Resolve all registered paths before reading any of them, so ordinary
        # simultaneous registrations reveal output collisions before polling.
        for entry in self._active.values():
            if not entry.resolved:
                return entry, None
        selected: _ActiveRun | None = None
        deadline = math.inf
        for entry in self._active.values():
            if not entry.ambiguous and entry.next_poll < deadline:
                selected = entry
                deadline = entry.next_poll
        if selected is None:
            return None, None
        if deadline <= now:
            return selected, None
        return None, min(deadline - now, TIMEOUT_MAX)

    def _work(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                if self._stop.is_set():
                    return
                self._wake.clear()
                entry, timeout = self._next_entry_locked(time.monotonic())
            if entry is None:
                self._wake.wait(timeout)
            elif self._is_active(entry):
                if entry.resolved:
                    self._poll(entry)
                else:
                    self._resolve(entry)

    def _resolve(self, entry: _ActiveRun) -> None:
        allow_command_end = True
        try:
            _, switches = partition_model_arguments(entry.run)
            allow_command_end = not any(re.fullmatch(r"-et\d+", option, re.IGNORECASE) for option in switches)
            if isinstance(entry.run.tcf, PureWindowsPath) != (os.name == "nt"):
                message = "Log monitoring requires the operating system matching the run's path format"
                raise ValueError(message)
            if self._resolver is None:
                self._resolver = RunProgressResolver()
            prediction = self._resolver.resolve(Path(str(entry.run.tcf)), entry.run.flags)
            if not allow_command_end:
                # -etN can override the TCF end time. Only the effective logged
                # Finish/End summary may provide this value for that invocation.
                prediction = replace(prediction, end_hours=None)
        except Exception as exc:  # noqa: BLE001 - parser/import failures are isolated to one optional observation
            prediction = RunLogPrediction(detail=f"Could not locate run log: {type(exc).__name__}: {exc}")
        with self._lock:
            if self._stop.is_set() or self._active.get(entry.run.index) is not entry:
                return
            entry.prediction = prediction
            entry.resolved = True
            entry.allow_command_end = allow_command_end
            if prediction.tlf_path is None:
                entry.next_poll = math.inf
                detail = prediction.detail or "The log filename could not be determined"
                self._publish_locked(entry, LogProgressSnapshot(state="unavailable", detail=detail))
                return
            # Compare lexical identities without dereferencing Windows mapped
            # drives, UNC aliases, symlinks or junctions on a network share.
            entry.log_key = normalize_plan_path(prediction.tlf_path)
            if not self._suppress_collisions_locked(entry):
                self._publish_locked(
                    entry,
                    LogProgressSnapshot(
                        start_hours=prediction.start_hours,
                        end_hours=prediction.end_hours,
                        detail=prediction.detail,
                    ),
                )

    def _suppress_collisions_locked(self, current: _ActiveRun) -> bool:
        peers: list[_ActiveRun] = []
        for entry in self._active.values():
            if entry.log_key == current.log_key:
                peers.append(entry)
        if len(peers) < 2 or current.log_key is None:
            return False
        indexes = ", ".join(str(entry.run.index) for entry in peers)
        detail = f"Runs {indexes} share log file {current.log_key}; their progress cannot be attributed safely"
        warn = current.log_key not in self._warned_collisions
        self._warned_collisions.add(current.log_key)
        for entry in peers:
            # Once writers overlap, ending one child cannot restore ownership
            # of the existing shared file. Suppress these invocations to exit.
            entry.ambiguous = True
            entry.reader = None
            entry.next_poll = math.inf
            prediction = entry.prediction
            self._publish_locked(
                entry,
                LogProgressSnapshot(
                    start_hours=prediction.start_hours if prediction is not None else None,
                    end_hours=prediction.end_hours if prediction is not None else None,
                    state="unverified",
                    detail=detail,
                ),
                warning=detail if entry is current and warn else None,
            )
        return True

    def _poll(self, entry: _ActiveRun) -> None:
        prediction = entry.prediction
        if prediction is None or prediction.tlf_path is None:
            return
        try:
            if entry.reader is None:
                entry.reader = LogProgressReader(
                    prediction.tlf_path,
                    started_at=entry.started_at,
                    start_hours=prediction.start_hours,
                    end_hours=prediction.end_hours,
                    allow_command_end=entry.allow_command_end,
                )
            if not self._is_active(entry):
                return
            snapshot = entry.reader.poll()
        except Exception as exc:  # noqa: BLE001 - a failed log read must not affect this or another solver process
            snapshot = LogProgressSnapshot(
                start_hours=prediction.start_hours,
                end_hours=prediction.end_hours,
                state="unavailable",
                detail=f"Could not read run log: {type(exc).__name__}: {exc}",
            )
        with self._lock:
            entry.next_poll = time.monotonic() + self._interval
            self._publish_locked(entry, snapshot)

    def _publish_locked(self, entry: _ActiveRun, snapshot: LogProgressSnapshot, *, warning: str | None = None) -> None:
        if self._stop.is_set() or self._active.get(entry.run.index) is not entry:
            return
        key = (snapshot.state, snapshot.detail) if snapshot.state == "unavailable" else None
        if key is not None and key != entry.warning_key and warning is None:
            warning = snapshot.detail or "Log monitoring is unavailable"
        entry.warning_key = key
        previous = self._pending.get(entry.run.index)
        if warning is None and previous is not None:
            # A recovered/updated snapshot must not swallow an as-yet unseen
            # warning just because the main thread has not drained it yet.
            warning = previous.warning
        self._pending[entry.run.index] = ProgressUpdate(entry.run.index, entry.pid, snapshot, warning)
