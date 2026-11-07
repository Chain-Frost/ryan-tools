"""Bounded, optional background file-property reads for workflow displays."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Thread

type TaskFiles = Path | Sequence[Path] | None


class FileSizeProbe:
    """Sample input sizes without blocking processing or waiting on shutdown.

    A single daemon thread performs stat calls. Only the coordinator consumes
    results and updates the display; a stalled network stat never holds up it.
    Cancellation cannot interrupt an OS stat already in progress, but prevents
    subsequent requests and publication once that call returns.
    """

    def __init__(self, file_groups: Sequence[Sequence[Path]], *, max_pending: int = 64) -> None:
        if max_pending < 1:
            msg = "max_pending must be positive"
            raise ValueError(msg)
        self._groups = tuple(tuple(group) for group in file_groups)
        self._results: Queue[tuple[int, int | None]] = Queue(maxsize=max_pending)
        self._stop = Event()
        self._thread = Thread(target=self._run, name="workflow-file-properties", daemon=True)

    def start(self) -> None:
        """Start the optional property reader; no files are read on this thread."""
        self._thread.start()

    def cancel(self) -> None:
        """Request cancellation without joining a potentially blocked OS call."""
        self._stop.set()

    def poll(self) -> list[tuple[int, int | None]]:
        """Return currently queued results without waiting for any stat call."""
        ready: list[tuple[int, int | None]] = []
        for _ in range(self._results.maxsize):
            try:
                ready.append(self._results.get_nowait())
            except Empty:
                break
        return ready

    def _run(self) -> None:
        for index, group in enumerate(self._groups, start=1):
            if self._stop.is_set():
                return
            if not group:
                continue
            size: int | None = 0
            for path in group:
                if self._stop.is_set():
                    return
                try:
                    size += path.stat().st_size
                except OSError:
                    size = None
                    break
            while not self._stop.is_set():
                try:
                    self._results.put((index, size), timeout=0.05)
                    break
                except Full:
                    continue


def format_file_size(size_bytes: int) -> str:
    """Format a measured file size with explicit binary units."""
    value = float(size_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{size_bytes} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"
