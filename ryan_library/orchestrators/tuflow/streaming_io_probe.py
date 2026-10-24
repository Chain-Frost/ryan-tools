"""Opt-in, non-authoritative read-ahead of complete PyTUFLOW batch projections.

This diagnostic overlaps literal-path stat calls with subsequent context
construction. The normal preflight inventory MUST still inspect every required
path, layer and database. No result here can establish model acceptance.
"""

from __future__ import annotations

import stat
import time
from collections import deque
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from .batch_preflight_adapter import BatchModelView


class _Reference(Protocol):
    path_pattern: Path
    base_directory: Path
    layer_pattern: str | None
    unresolved_variables: tuple[str, ...]
    requires_attribute_scan: bool
    requires_database_scan: bool
    requires_table_link_scan: bool
    requires_raster_metadata_scan: bool


class _Run(Protocol):
    error: str | None
    file_references: tuple[_Reference, ...]


@dataclass(frozen=True, slots=True)
class ReadAheadSummary:
    """Aggregate diagnostic counts only, never model identifiers or paths."""

    unique_paths: int
    duplicate_paths: int
    excluded_references: int
    present: int
    missing: int
    unresolved: int
    peak_outstanding: int
    wall_seconds: float
    worker_seconds: float
    overlapped_context_construction: bool


def _eligible_path(ref: _Reference) -> Path | None:
    """Conservatively choose plain literal file paths, never dynamic selectors."""
    if (
        ref.unresolved_variables
        or ref.layer_pattern
        or ref.requires_attribute_scan
        or ref.requires_database_scan
        or ref.requires_table_link_scan
        or ref.requires_raster_metadata_scan
    ):
        return None
    raw = str(ref.path_pattern)
    if not raw or raw == "." or "<<" in raw or ">>" in raw or "~" in raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = Path(ref.base_directory) / path
    # A literal basename is not safe if its inherited base contains selectors.
    combined = str(path)
    if any(c in combined for c in "*?[") or "<<" in combined or ">>" in combined or "~" in combined:
        return None
    return path.absolute()


def _stat_literal(path: Path) -> tuple[str, float]:
    """One read-only metadata check; errors never become accepted preflight."""
    start = time.perf_counter()
    try:
        category = "present" if stat.S_ISREG(path.stat().st_mode) else "missing"
    except FileNotFoundError:
        category = "missing"
    except OSError:
        category = "unresolved"
    return category, time.perf_counter() - start


class FileReadAheadProbe:
    """One-worker bounded diagnostic, isolated from authoritative inventory."""

    def __init__(self, *, max_pending: int = 8) -> None:
        if max_pending < 1:
            message = "max_pending must be positive"
            raise ValueError(message)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tuflow-read-ahead")
        self._pending: deque[Future[tuple[str, float]]] = deque()
        self._seen: set[Path] = set()
        self._started = time.perf_counter()
        self._first_worker_started: float | None = None
        self._producer_finished: float | None = None
        self._closed = False
        self._max_pending = max_pending
        self._duplicates = 0
        self._excluded = 0
        self._present = 0
        self._missing = 0
        self._unresolved = 0
        self._peak_outstanding = 0
        self._worker_seconds = 0.0

    def _read(self, path: Path) -> tuple[str, float]:
        if self._first_worker_started is None:
            self._first_worker_started = time.perf_counter()
        return _stat_literal(path)

    def _drain_one(self) -> None:
        category, elapsed = self._pending.popleft().result()
        if category == "present":
            self._present += 1
        elif category == "missing":
            self._missing += 1
        else:
            self._unresolved += 1
        self._worker_seconds += elapsed

    def submit_run(self, run: object) -> None:
        """Called synchronously after PyTUFLOW finishes one immutable run."""
        if self._closed:
            message = "Read-ahead producer is closed"
            raise RuntimeError(message)
        projection = cast(_Run, run)
        if projection.error is not None:
            return  # A failed/partial projection must never seed inspection.
        for ref in projection.file_references:
            path = _eligible_path(ref)
            if path is None:
                self._excluded += 1
                continue
            if path in self._seen:
                self._duplicates += 1
                continue
            self._seen.add(path)
            if len(self._pending) == self._max_pending:
                self._drain_one()  # Bounded backpressure, never unbounded futures.
            self._pending.append(self._executor.submit(self._read, path))
            self._peak_outstanding = max(self._peak_outstanding, len(self._pending))

    def mark_producer_finished(self) -> None:
        self._producer_finished = time.perf_counter()

    def finish(self) -> ReadAheadSummary:
        if self._closed:
            message = "Read-ahead probe has already finished"
            raise RuntimeError(message)
        try:
            while self._pending:
                self._drain_one()
        finally:
            self._closed = True
            self._executor.shutdown(wait=True)
        return ReadAheadSummary(
            unique_paths=len(self._seen),
            duplicate_paths=self._duplicates,
            excluded_references=self._excluded,
            present=self._present,
            missing=self._missing,
            unresolved=self._unresolved,
            peak_outstanding=self._peak_outstanding,
            wall_seconds=time.perf_counter() - self._started,
            worker_seconds=self._worker_seconds,
            overlapped_context_construction=(
                self._first_worker_started is not None
                and self._producer_finished is not None
                and self._first_worker_started < self._producer_finished
            ),
        )

    def __enter__(self) -> FileReadAheadProbe:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self._closed:
            return
        try:
            while self._pending:
                self._drain_one()
        except Exception:
            if exc_type is None:
                raise
        finally:
            self._closed = True
            self._executor.shutdown(wait=True)


def probe_batch_io_overlap(
    model: object, flags: Sequence[str], *, max_pending: int = 8
) -> tuple[BatchModelView, ReadAheadSummary]:
    """Pilot only: preserve full batch projection and final inventory semantics.

    Requires the compatible opt-in PyTUFLOW producer callback. Does not run the
    public preflight or alter its acceptance. The caller must perform normal
    validation separately; the returned counts are never dependency verdicts.
    """
    with FileReadAheadProbe(max_pending=max_pending) as probe:
        view = BatchModelView(model, flags, on_run=probe.submit_run)
        probe.mark_producer_finished()
        result = probe.finish()
    return view, result
