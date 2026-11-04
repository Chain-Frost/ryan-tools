"""Predict run logs with PyTUFLOW and sample bounded, current-invocation progress.

This module does no work on import and never decides whether a solver succeeded.
Callers must run prediction and polling outside their scheduling/UI thread: a
bounded read still cannot put a time limit on an unavailable network filesystem.
One reader belongs to one launched invocation, with one writer per output path.
"""

from __future__ import annotations

import importlib
import math
import os
import re
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from io import FileIO
from pathlib import Path
from typing import Literal, Protocol, cast

ProgressState = Literal["waiting", "live", "unverified", "unavailable"]

# A poll reads at most two windows from each of two files, regardless of file size.
_WINDOW_BYTES = 64 * 1024
_NUMBER = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eEdD][+-]?\d+)?"
_CLOCK = r"[+-]?\d+:[0-5]\d:[0-5]\d(?:\.\d+)?"
_PLACEHOLDER = re.compile(r"~([es]\d?)~", re.IGNORECASE)
_UNRESOLVED = re.compile(r"<<|>>|~[es]\d?~|[*?]", re.IGNORECASE)
_STARTED = re.compile(
    r"^\s*Simulation Started:\s*(\d{4})-([A-Za-z]{3})-(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$",
    re.MULTILINE,
)
_LIMIT = re.compile(
    rf"^\s*(Start|End|Finish) Time\s*\(h\)\s*(==|:)\s*(?:\.{{2,}}\s*)?({_NUMBER})(?=\s|$)",
    re.IGNORECASE,
)
_MAIN_TIME = re.compile(rf"^\s*(?:SIM:|Writing HPC Output at:)\s*({_CLOCK})(?=\s|$)", re.IGNORECASE)
_HPC_START = re.compile(rf"^\s*Start Time\s*={{1,2}}\s*({_CLOCK})(?=\s|$)", re.IGNORECASE)
_HPC_TABLE = re.compile(r"^\s*iStep\s+time\s+", re.IGNORECASE)
_HPC_ROW = re.compile(rf"^\s*\d+\s+({_CLOCK})\s+{_NUMBER}\s+{_NUMBER}\s+{_NUMBER}\s+")
_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")


@dataclass(frozen=True, slots=True)
class RunLogPrediction:
    """Best-effort metadata; an absent path means monitoring is unavailable."""

    tlf_path: Path | None = None
    start_hours: float | None = None
    end_hours: float | None = None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class LogProgressSnapshot:
    """Last observed model hours and log modification time, never an exit status."""

    start_hours: float | None = None
    end_hours: float | None = None
    model_hours: float | None = None
    last_log_update: float | None = None
    state: ProgressState = "waiting"
    detail: str = ""


class _TimeInput(Protocol):
    @property
    def value(self) -> object: ...


class _RunContext(Protocol):
    def output_name(self) -> str: ...

    def log_folder_path(self) -> Path: ...

    def find_input(self, *, lhs: str, regex: bool, regex_flags: int) -> list[_TimeInput]: ...


class _Model(Protocol):
    def context(self, run_context: dict[str, str], *, file_resolution: object) -> _RunContext: ...


class _ModelFactory(Protocol):
    def __call__(self, fpath: Path) -> _Model: ...


def _finite_number(value: object) -> float | None:
    # Datetimes (ISODATE), booleans and unresolved expressions are not model hours.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _ordered_context(tcf_name: str, supplied: Mapping[str, str]) -> dict[str, str]:
    """Adapt numbered flags to the accepted parser's filename-order requirement.

    The accepted PyTUFLOW pin removes used flags in reverse placeholder order,
    using their original argument positions. Ordering this metadata-only context
    accordingly avoids its IndexError without changing the actual solver argv.
    Mixed ~e~/~e1~ spellings are deliberately unsupported rather than guessed.
    """
    values: dict[str, str] = {}
    for supplied_key, value in supplied.items():
        key = supplied_key.lstrip("-").casefold()
        if re.fullmatch(r"[es][1-9]?", key) is None:
            message = f"Unsupported progress flag: {supplied_key}"
            raise ValueError(message)
        key = f"{key}1" if len(key) == 1 else key
        if key in values and values[key] != value:
            message = f"Conflicting progress flag: {key}"
            raise ValueError(message)
        values[key] = value

    context: dict[str, str] = {}
    aliases: dict[str, str] = {}
    for match in _PLACEHOLDER.finditer(tcf_name):
        token = match.group(1).casefold()
        canonical = f"{token}1" if len(token) == 1 else token
        if canonical not in values:
            message = f"No value for {match.group(0)} in the TCF filename"
            raise ValueError(message)
        if canonical in aliases and aliases[canonical] != token:
            message = f"Mixed bare and numbered {token[0]} placeholders cannot be predicted reliably"
            raise ValueError(message)
        aliases[canonical] = token
        context[token] = values[canonical]
    for key, value in values.items():
        if key not in aliases:
            context[key] = value
    return context


def _model_hours(run: _RunContext, label: str) -> float | None:
    inputs = run.find_input(lhs=rf"^{label} Time$", regex=True, regex_flags=re.IGNORECASE)
    if not inputs:
        return None
    # A resolved TCF can include active TEF/read-file commands. Ambiguous values
    # stay unknown; the actual log will supply the effective solver timing.
    result: float | None = None
    for command in inputs:
        value = _finite_number(command.value)
        if value is None or (result is not None and result != value):
            return None
        result = value
    return result


class RunProgressResolver:
    """Cache one parsed TCF per batch and use only public PyTUFLOW capabilities.

    A resolver is intended for one background worker and is not thread-safe.
    Live edits to control files require a new resolver; actual log timing takes
    precedence over these advisory start/end values.
    """

    def __init__(self) -> None:
        self._models: dict[Path, _Model] = {}
        self._factory: _ModelFactory | None = None
        self._deferred: object = None

    def resolve(self, tcf_path: Path, run_variables: Mapping[str, str]) -> RunLogPrediction:
        """Return a prediction or a readable failure without affecting a run."""
        try:
            path = tcf_path.absolute()
            context = _ordered_context(path.name, run_variables)
            if not path.is_file():
                message = f"TCF is unavailable: {path}"
                raise FileNotFoundError(message)
            if self._factory is None:
                module = importlib.import_module("pytuflow")
                factory: object = getattr(module, "TCF", None)
                resolution: object = getattr(module, "FileResolution", None)
                self._deferred = getattr(resolution, "DEFERRED", None)
                if not callable(factory) or self._deferred is None:
                    message = "The installed PyTUFLOW lacks the public deferred TCF API"
                    raise ValueError(message)
                self._factory = cast(_ModelFactory, factory)
            if path not in self._models:
                self._models[path] = self._factory(path)
            run = self._models[path].context(context, file_resolution=self._deferred)
            name = run.output_name()
            folder = Path(run.log_folder_path())
            if not name or name in {".", ".."} or any(char in name for char in ("/", "\\", ":", "\x00")):
                message = "PyTUFLOW returned an unsafe or empty log filename"
                raise ValueError(message)
            if _UNRESOLVED.search(name) or _UNRESOLVED.search(str(folder)):
                message = "PyTUFLOW could not resolve all variables in the log path"
                raise ValueError(message)
            if not folder.is_absolute():
                folder = path.parent / folder
            # Do not call tlf_path(): the accepted pin uses with_suffix on the
            # extension-free name and truncates dotted event values (e.g. 01.0p).
            tlf_path = folder / f"{name}.tlf"
            return RunLogPrediction(tlf_path, _model_hours(run, "Start"), _model_hours(run, "End"))
        except Exception as exc:  # noqa: BLE001 - optional third-party parsing must not stop a solver
            # Optional metadata from a third-party parser must never stop a
            # solver. The caller can surface this once as a monitoring warning.
            return RunLogPrediction(detail=f"Log prediction unavailable: {type(exc).__name__}: {exc}")


@dataclass(frozen=True, slots=True)
class _Stamp:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> _Stamp:
        return cls(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)

    @property
    def modified(self) -> float:
        return self.modified_ns / 1_000_000_000


@dataclass(frozen=True, slots=True)
class _Parsed:
    started: float | None = None
    start_hours: float | None = None
    end_hours: float | None = None
    model_hours: float | None = None
    start_priority: int = 0
    end_priority: int = 0
    hpc_table: bool = False


@dataclass(slots=True)
class _Source:
    path: Path
    path_stamp: _Stamp | None = None
    stamp: _Stamp | None = None
    head: bytes = b""
    parsed: _Parsed = _Parsed()
    fresh: bool = False
    missing_seen: bool = False
    require_change: bool = False


@dataclass(frozen=True, slots=True)
class _Observation:
    parsed: _Parsed = _Parsed()
    stamp: _Stamp | None = None
    state: ProgressState = "waiting"
    detail: str = ""


def _complete_lines(data: bytes, *, discard_first: bool = False) -> str:
    if discard_first:
        boundary = data.find(b"\n")
        data = data[boundary + 1 :] if boundary >= 0 else b""
    boundary = data.rfind(b"\n")
    return data[: boundary + 1].decode("utf-8", errors="replace") if boundary >= 0 else ""


def _read_windows(path: Path) -> tuple[_Stamp, bytes, tuple[str, ...]]:
    with _open_shared_binary(path) as source:
        stamp = _Stamp.from_stat(os.fstat(source.fileno()))
        head = source.read(min(stamp.size, _WINDOW_BYTES))
        sections = [_complete_lines(head)]
        if stamp.size > len(head):
            offset = max(len(head), stamp.size - _WINDOW_BYTES)
            source.seek(offset)
            tail = source.read(_WINDOW_BYTES)
            if offset == len(head):
                # Contiguous windows can complete the one record split between
                # them. For a skipped middle, discard the unknown leading row.
                prefix = head[head.rfind(b"\n") + 1 :]
                sections.append(_complete_lines(prefix + tail))
            else:
                sections.append(_complete_lines(tail, discard_first=True))
    return stamp, head, tuple(sections)


def _open_shared_binary(path: Path) -> FileIO:
    """Open read-only with permissive read/write/delete sharing.

    Windows CRT defaults do not guarantee delete sharing. Request all three
    share flags explicitly, then transfer HANDLE ownership to the file object.
    Sharing violations are ordinary OSErrors; callers skip this poll, not retry
    in a tight loop. POSIX opens are read-only and acquire no advisory lock.
    """
    if sys.platform != "win32":
        return FileIO(path, "rb")

    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    generic_read = 0x80000000
    share_read_write_delete = 0x00000001 | 0x00000002 | 0x00000004
    handle = create_file(str(path), generic_read, share_read_write_delete, None, 3, 0x80, None)
    if handle is None or handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT)
    except BaseException:
        close_handle(handle)
        raise
    try:
        return FileIO(descriptor, "rb", closefd=True)
    except BaseException:
        os.close(descriptor)
        raise


def _launch_time(text: str) -> float | None:
    match = _STARTED.search(text)
    if match is None:
        return None
    try:
        month = _MONTHS.index(match.group(2).casefold()) + 1
        value = datetime(
            int(match.group(1)),
            month,
            int(match.group(3)),
            int(match.group(4)),
            int(match.group(5)),
            int(match.group(6) or 0),
        )
        # TUFLOW and its runner use the same computer's local time; converting
        # here also accounts for the machine's timezone/DST rules.
        return value.timestamp()
    except (ValueError, OverflowError, OSError):
        return None


def _clock_hours(value: str) -> float | None:
    try:
        negative = value.startswith("-")
        parts = value.lstrip("+-").split(":")
        result = int(parts[0]) + int(parts[1]) / 60 + float(parts[2]) / 3600
        return _finite_number(-result if negative else result)
    except (ValueError, OverflowError):
        return None


def _parse_hpc_line(line: str, parsed: _Parsed) -> _Parsed:
    if _HPC_TABLE.match(line):
        parsed = replace(parsed, hpc_table=True)
    start = _HPC_START.match(line)
    if start:
        parsed = replace(parsed, start_hours=_clock_hours(start.group(1)))
    row = _HPC_ROW.match(line) if parsed.hpc_table else None
    if row:
        parsed = _advance_model_time(parsed, _clock_hours(row.group(1)))
    return parsed


def _parse_main_line(line: str, parsed: _Parsed, *, allow_command_end: bool) -> _Parsed:
    limit = _LIMIT.match(line)
    if limit:
        value = _finite_number(float(limit.group(3).replace("D", "E").replace("d", "e")))
        priority = 2 if limit.group(2) == ":" else 1
        if limit.group(1).casefold() == "start":
            if value is not None and priority >= parsed.start_priority:
                parsed = replace(parsed, start_hours=value, start_priority=priority)
        elif value is not None and priority >= parsed.end_priority and (priority == 2 or allow_command_end):
            parsed = replace(parsed, end_hours=value, end_priority=priority)
    progress = _MAIN_TIME.match(line)
    if progress:
        parsed = _advance_model_time(parsed, _clock_hours(progress.group(1)))
    return parsed


def _advance_model_time(parsed: _Parsed, value: float | None) -> _Parsed:
    if value is None:
        return parsed
    current = value if parsed.model_hours is None else max(value, parsed.model_hours)
    return replace(parsed, model_hours=current)


def _parse_sections(sections: tuple[str, ...], previous: _Parsed, *, hpc: bool, allow_command_end: bool) -> _Parsed:
    parsed = previous
    for section in sections:
        if not hpc:
            started = _launch_time(section)
            if started is not None:
                parsed = replace(parsed, started=started)
        for line in section.splitlines():
            if hpc:
                parsed = _parse_hpc_line(line, parsed)
            else:
                parsed = _parse_main_line(line, parsed, allow_command_end=allow_command_end)
    return parsed


class LogProgressReader:
    """Read bounded, complete TLF/HPC records belonging to one launched process.

    Main-log headers have minute precision, so a matching header alone is not
    evidence of a new invocation. Its mtime must also be at/after launch, or a
    change must have been observed after this reader's first post-launch poll.
    HPC logs have no launch date and require independent post-launch evidence
    after the main log is verified. Coarse timestamps therefore defer progress
    until a later observed update; stale logs are never accepted just for being
    within a tolerance window. Concurrent writers to the same output path must
    be suppressed by the caller because the log format cannot identify them.
    """

    def __init__(
        self,
        tlf_path: Path,
        *,
        started_at: float,
        start_hours: float | None = None,
        end_hours: float | None = None,
        allow_command_end: bool = True,
    ) -> None:
        self._main = _Source(tlf_path)
        self._hpc = _Source(tlf_path.with_suffix(".hpc.tlf"))
        self._started_at = started_at
        self._start_hours = _finite_number(start_hours)
        self._end_hours = _finite_number(end_hours)
        self._allow_command_end = allow_command_end
        self._snapshot = LogProgressSnapshot(self._start_hours, self._end_hours)

    def poll(self) -> LogProgressSnapshot:
        """Return last observed progress, tolerating missing or temporarily locked logs."""
        if not math.isfinite(self._started_at):
            return LogProgressSnapshot(state="unavailable", detail="The invocation start timestamp is unavailable")
        main = self._poll_source(self._main, hpc=False)
        if main.state != "live":
            if main.state == "unavailable" and self._snapshot.last_log_update is not None:
                self._snapshot = replace(self._snapshot, state=main.state, detail=main.detail)
            else:
                self._snapshot = LogProgressSnapshot(
                    self._start_hours, self._end_hours, state=main.state, detail=main.detail
                )
            return self._snapshot

        companion = self._poll_source(self._hpc, hpc=True)
        start = main.parsed.start_hours
        end = main.parsed.end_hours
        current = main.parsed.model_hours
        modified = main.stamp.modified if main.stamp else None
        if companion.stamp is not None and companion.state in {"live", "unavailable"}:
            if start is None:
                start = companion.parsed.start_hours
            if companion.parsed.model_hours is not None:
                current = (
                    companion.parsed.model_hours if current is None else max(current, companion.parsed.model_hours)
                )
            modified = max(modified, companion.stamp.modified) if modified is not None else companion.stamp.modified
        self._snapshot = LogProgressSnapshot(
            start if start is not None else self._start_hours,
            end if end is not None else self._end_hours,
            current,
            modified,
            "unavailable" if companion.state == "unavailable" else "live",
            companion.detail
            if companion.state == "unavailable"
            else ("Waiting for model-time output" if current is None else ""),
        )
        return self._snapshot

    def _poll_source(self, source: _Source, *, hpc: bool) -> _Observation:
        try:
            path_stamp = _Stamp.from_stat(source.path.stat())
            if path_stamp == source.path_stamp:
                return self._observation(source, hpc=hpc)
            stamp, head, sections = _read_windows(source.path)
        except FileNotFoundError:
            if not hpc and source.stamp is not None:
                self._hpc = _Source(self._hpc.path, require_change=True)
            source.path_stamp = None
            source.stamp = None
            source.head = b""
            source.parsed = _Parsed()
            source.fresh = False
            source.missing_seen = True
            return _Observation(detail="Waiting for TLF" if not hpc else "Waiting for HPC log")
        except OSError as exc:
            detail = f"{'HPC log' if hpc else 'Log'} temporarily unavailable: {exc}"
            if source.fresh:
                # A sharing conflict skips this observation. Previously verified
                # progress remains valid (and keeps its original modification
                # time); actual disappearance or replacement clears it instead.
                return _Observation(source.parsed, source.stamp, "unavailable", detail)
            return _Observation(state="unavailable", detail=detail)

        previous = source.stamp
        replaced = previous is not None and (
            (stamp.device, stamp.inode) != (previous.device, previous.inode)
            or stamp.size < previous.size
            or not head.startswith(source.head)
        )
        if replaced and not hpc:
            # A new main-log generation cannot inherit progress from a still
            # unchanged companion. Baseline that companion again, then wait for
            # its next observed change (it has no invocation date of its own).
            self._hpc = _Source(self._hpc.path, require_change=True)
        changed_after_observation = previous is not None and stamp != previous
        was_fresh = source.fresh
        source.fresh = (
            (was_fresh and not replaced)
            or stamp.modified >= self._started_at
            or source.missing_seen
            or changed_after_observation
        )
        if source.require_change:
            source.fresh = source.missing_seen or changed_after_observation
            if source.fresh:
                source.require_change = False
        # Compare path observations only with earlier path observations. On
        # Windows, stat() and fstat() can report different ctime meanings for
        # the same file (CPython issue #157671). The handle stamp remains the
        # authority for the bytes read and for generation comparisons.
        # Keep the pre-read path stamp: a change during reading must cause a
        # fresh poll rather than be hidden by caching a newer unread state.
        source.path_stamp = path_stamp
        source.stamp = stamp
        if not replaced and was_fresh and head == source.head:
            # Replaying an unchanged head could overwrite a newer effective
            # timing summary retained from a tail which has since scrolled out.
            sections = sections[1:]
        source.head = head
        source.parsed = _parse_sections(
            sections,
            _Parsed() if replaced or not was_fresh else source.parsed,
            hpc=hpc,
            allow_command_end=self._allow_command_end,
        )
        return self._observation(source, hpc=hpc)

    def _observation(self, source: _Source, *, hpc: bool) -> _Observation:
        verified = source.fresh
        if not hpc:
            started = source.parsed.started
            # Minute precision is intentionally paired with independent file
            # freshness, not a permissive mtime-minus-60-seconds comparison.
            earliest = math.floor(self._started_at / 60) * 60
            latest = time.time()
            verified = verified and started is not None and earliest <= started <= latest
        if not verified:
            return _Observation(state="unverified", detail="Waiting for a log from this invocation")
        return _Observation(source.parsed, source.stamp, "live")
