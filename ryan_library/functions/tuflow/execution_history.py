"""Single-writer, append-only CSV history for TUFLOW runner continuation.

The current CommandPlan remains authoritative: stored commands are diagnostics,
never replay input. No model files, outputs or external processes are inspected.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import ntpath
import os
import posixpath
import re
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from io import FileIO, StringIO
from pathlib import Path, PurePath, PureWindowsPath
from typing import Protocol
from uuid import uuid4

from .command_plan import CommandPlan, PlannedRun

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "1"
COLUMNS = (
    "schema_version",
    "event",
    "recorded_at",
    "run_key",
    "attempt_id",
    "outcome",
    "return_code",
    "tcf",
    "working_directory",
    "executable",
    "arguments",
    "pid",
    "error",
)
_EVENT_FLAGS = re.compile(r"-[es][1-9]", re.IGNORECASE)
_GPU = re.compile(r"-pu\d+(?:,\d+)*", re.IGNORECASE)
_ENGINE = frozenset({"-b", "-x", "-t", "-nlc"})
_OTHER_SWITCH = re.compile(r"-et\d+", re.IGNORECASE)
_HEX_SHA = re.compile(r"[0-9a-f]{64}")
_HEX_ATTEMPT = re.compile(r"[0-9a-f]{32}")


class ExecutionLogError(ValueError):
    """An execution log is inaccessible, incompatible, locked or corrupt."""


class ObservedOutcome(Protocol):
    """Read-only structural contract of the parent's frozen RunOutcome."""

    @property
    def run(self) -> PlannedRun: ...

    @property
    def return_code(self) -> int | None: ...

    @property
    def pid(self) -> int | None: ...

    @property
    def error(self) -> str | None: ...


@dataclass(frozen=True, slots=True)
class RunIdentity:
    """Stable logical occurrence and readable diagnostic context."""

    key: str
    executable: str
    tcf: str
    working_directory: str
    arguments: str


@dataclass(slots=True)
class _Attempt:
    run_key: str
    outcome: str | None = None


def _normal_path(value: str | PurePath) -> str:
    """Normalize spelling lexically, retaining distinct drive/UNC identities."""
    text = str(value)
    if isinstance(value, PureWindowsPath) or PureWindowsPath(text).drive or "\\" in text:
        return ntpath.normcase(ntpath.normpath(text))
    return posixpath.normpath(text)


def _effective_executable(run: PlannedRun, fallback: Path, base: Path) -> str:
    if run.executable is not None:
        return run.executable
    return str(fallback if fallback.is_absolute() else (base / fallback).absolute())


def _canonical_arguments(run: PlannedRun) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    """Canonicalize model arguments against the plan's effective TCF.

    Bare rows can omit the TCF token; full commands may place it before
    other switches. A scenario/event value ending in .tcf remains a value.
    """
    arguments = run.arguments
    flags: list[tuple[str, str]] = []
    remaining: list[str] = []
    position = 0
    while position < len(arguments):
        token = arguments[position]
        low = token.casefold()
        if _EVENT_FLAGS.fullmatch(token):
            if position + 1 >= len(arguments):
                msg = f"Run {run.index} has an event/scenario switch without a value"
                raise ExecutionLogError(msg)
            flags.append((low[1:], arguments[position + 1]))
            position += 2
            continue
        if not _GPU.fullmatch(token) and not (not token.startswith("-") and low.endswith(".tcf")):
            remaining.append(low if low in _ENGINE or _OTHER_SWITCH.fullmatch(token) else token)
        position += 1
    if dict(flags) != run.flags:
        msg = f"Run {run.index} has inconsistent event/scenario arguments"
        raise ExecutionLogError(msg)
    return tuple(sorted(flags)), tuple(remaining)


def identify_runs(plan: CommandPlan, *, fallback_executable: Path, base_directory: Path) -> dict[int, RunIdentity]:
    """Identify all logical occurrences *before* filtering for Resume.

    Same-config duplicates get a separate ordinal for that configuration.
    Display index, source line, scheduling priority and runtime GPU are excluded.
    """
    occurrences: dict[str, int] = {}
    identities: dict[int, RunIdentity] = {}
    for run in plan.runs:
        if run.index in identities:
            msg = f"Duplicate plan index {run.index}"
            raise ExecutionLogError(msg)
        executable = _effective_executable(run, fallback_executable, base_directory)
        executable_path = (
            PureWindowsPath(executable) if isinstance(run.working_directory, PureWindowsPath) else executable
        )
        flags, remaining = _canonical_arguments(run)
        payload = json.dumps(
            [
                "tuflow-logical-v1",
                _normal_path(run.tcf),
                _normal_path(run.working_directory),
                _normal_path(executable_path),
                flags,
                remaining,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        ordinal = occurrences.get(payload, 0) + 1
        occurrences[payload] = ordinal
        key = hashlib.sha256(json.dumps([payload, ordinal], ensure_ascii=False).encode("utf-8")).hexdigest()
        identities[run.index] = RunIdentity(
            key,
            executable,
            str(run.tcf),
            str(run.working_directory),
            subprocess.list2cmdline(list(run.arguments)),
        )
    return identities


def select_pending(
    plan: CommandPlan,
    identities: dict[int, RunIdentity],
    successes: set[str],
) -> CommandPlan:
    """Keep current-plan order and original indices, including duplicate runs."""
    return replace(plan, runs=tuple(run for run in plan.runs if identities[run.index].key not in successes))


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _parse_line(line: bytes, number: int) -> list[str]:
    try:
        decoded = line.decode("utf-8")
        values = list(csv.reader([decoded], strict=True))
    except (UnicodeError, csv.Error) as exc:
        msg = f"Invalid UTF-8/CSV at line {number}: {exc}"
        raise ExecutionLogError(msg) from exc
    if len(values) != 1 or len(values[0]) != len(COLUMNS):
        msg = f"Invalid CSV record shape at line {number}; expected {len(COLUMNS)} fields"
        raise ExecutionLogError(msg)
    return values[0]


class CsvExecutionLog:
    """Lock, read once and append durable records in one predictable CSV.

    Only a non-newline-terminated final fragment is repairable. Complete earlier
    records, including complete final corrupt records, are never silently skipped.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: FileIO | None = None
        self._attempts: dict[str, _Attempt] = {}
        self._latest: dict[str, str] = {}

    def __enter__(self) -> CsvExecutionLog:
        try:
            self._file = FileIO(self.path, "a+")
            self._lock()
            self._read_and_repair()
        except (OSError, ValueError) as exc:
            self.__exit__(None, None, None)
            if isinstance(exc, ExecutionLogError):
                raise
            msg = f"Cannot open execution log {self.path}: {exc}"
            raise ExecutionLogError(msg) from exc
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self._file is not None:
            self._file.close()  # Closing also releases the OS file lock.
            self._file = None

    def _stream(self) -> FileIO:
        if self._file is None:
            msg = "Execution log is not open"
            raise ExecutionLogError(msg)
        return self._file

    def _lock(self) -> None:
        file = self._stream()
        try:
            if os.name == "nt":
                import msvcrt

                # Windows byte-range locks are mandatory for readers too.
                # Reserve a far-beyond-EOF byte so operators can inspect the CSV
                # while the writer owns it, without locking actual CSV content.
                file.seek(1 << 60)
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            msg = f"Execution log {self.path} already has a writer or cannot be locked: {exc}"
            raise ExecutionLogError(msg) from exc

    def _read_and_repair(self) -> None:
        file = self._stream()
        file.seek(0)
        data = file.read()
        if not data:
            self._append_header()
            return
        end = len(data) if data.endswith(b"\n") else data.rfind(b"\n") + 1
        complete = data[:end]
        rows = complete.splitlines(keepends=True)
        if not rows and not csv_line(COLUMNS).startswith(data):
            msg = (
                f"Unsupported execution CSV schema or corrupt header in {self.path}; "
                "cannot safely repair a file without a valid header."
            )
            raise ExecutionLogError(msg)
        if rows:
            try:
                header = next(csv.reader([rows[0].decode("utf-8")], strict=True))
            except (UnicodeError, csv.Error) as exc:
                msg = f"Invalid CSV header in {self.path}: {exc}"
                raise ExecutionLogError(msg) from exc
            if tuple(header) != COLUMNS:
                msg = (
                    f"Unsupported execution CSV schema in {self.path}. Expected version {SCHEMA_VERSION} "
                    "START/FINISH/RESET history; an old completion-only report cannot be resumed. "
                    "Keep the old file separately and use a new log."
                )
                raise ExecutionLogError(msg)
        for number, line in enumerate(rows[1:], 2):
            record: dict[str, str] = dict(zip(COLUMNS, _parse_line(line, number), strict=True))
            self._apply(record, number)
        if end != len(data):
            logger.warning("Repairing incomplete trailing CSV fragment (%d bytes) in %s", len(data) - end, self.path)
            file.truncate(end)
            file.flush()
            os.fsync(file.fileno())
        if not rows:
            self._append_header()

    def _apply(self, record: dict[str, str], number: int) -> None:  # noqa: C901 - validate each transition together
        if record["schema_version"] != SCHEMA_VERSION:
            msg = f"Unknown CSV schema version at line {number}: {record['schema_version']!r}"
            raise ExecutionLogError(msg)
        try:
            stamp = datetime.fromisoformat(record["recorded_at"])
        except ValueError as exc:
            msg = f"Invalid timestamp at line {number}"
            raise ExecutionLogError(msg) from exc
        if stamp.utcoffset() is None:
            msg = f"Timestamp has no timezone at line {number}"
            raise ExecutionLogError(msg)
        event = record["event"]
        key = record["run_key"]
        attempt_id = record["attempt_id"]
        if event == "RESET":
            if any(record[column] for column in COLUMNS if column not in {"schema_version", "event", "recorded_at"}):
                msg = f"Invalid RESET fields at line {number}"
                raise ExecutionLogError(msg)
            self._latest.clear()
            return
        if event not in {"START", "FINISH"}:
            msg = f"Unknown CSV event at line {number}: {event!r}"
            raise ExecutionLogError(msg)
        if not _HEX_SHA.fullmatch(key) or not _HEX_ATTEMPT.fullmatch(attempt_id):
            msg = f"Invalid run key or attempt ID at line {number}"
            raise ExecutionLogError(msg)
        if not all(record[field] for field in ("tcf", "working_directory", "executable", "arguments")):
            msg = f"Missing run context at line {number}"
            raise ExecutionLogError(msg)
        if any("\r" in value or "\n" in value for value in record.values()):
            msg = f"Multiline CSV fields are prohibited at line {number}"
            raise ExecutionLogError(msg)
        if event == "START":
            if attempt_id in self._attempts or any(record[k] for k in ("outcome", "return_code", "pid", "error")):
                msg = f"Duplicate attempt ID or invalid START fields at line {number}"
                raise ExecutionLogError(msg)
            self._attempts[attempt_id] = _Attempt(key)
            self._latest[key] = attempt_id
            return
        attempt = self._attempts.get(attempt_id)
        if attempt is None or attempt.run_key != key or attempt.outcome is not None:
            msg = f"FINISH has no matching unfinished START at line {number}"
            raise ExecutionLogError(msg)
        outcome, code, error = record["outcome"], record["return_code"], record["error"]
        if outcome == "SUCCESS":
            valid = code == "0" and not error
        elif outcome == "FAILED":
            try:
                valid = int(code) != 0
            except ValueError:
                valid = False
        elif outcome == "LAUNCH_ERROR":
            valid = not code and bool(error) and not record["pid"]
        else:
            valid = False
        if not valid or (record["pid"] and not record["pid"].isdigit()):
            msg = f"Invalid FINISH outcome at line {number}"
            raise ExecutionLogError(msg)
        attempt.outcome = outcome

    def _append_header(self) -> None:
        self._write(csv_line(COLUMNS))

    def _write(self, content: bytes) -> None:
        try:
            file = self._stream()
            file.seek(0, os.SEEK_END)
            if file.write(content) != len(content):
                msg = "Short CSV write"
                raise OSError(msg)
            file.flush()
            os.fsync(file.fileno())
        except OSError as exc:
            msg = f"Cannot write execution log {self.path}: {exc}"
            raise ExecutionLogError(msg) from exc

    def _append(self, record: dict[str, str]) -> None:
        row = [record.get(column, "") for column in COLUMNS]
        if any("\n" in cell or "\r" in cell for cell in row):
            msg = "CSV context may not contain newline characters"
            raise ExecutionLogError(msg)
        self._write(csv_line(row))

    def successful_keys(self) -> set[str]:
        """Only latest STARTs finished successfully since the last RESET count."""
        return {key for key, attempt_id in self._latest.items() if self._attempts[attempt_id].outcome == "SUCCESS"}

    def reset(self) -> None:
        """Run all: persist a new baseline before any new attempt starts."""
        record = {"schema_version": SCHEMA_VERSION, "event": "RESET", "recorded_at": _timestamp()}
        self._append(record)
        self._latest.clear()

    def start(self, identity: RunIdentity) -> str:
        """Persist and fsync START before Popen; return its unique attempt ID."""
        attempt_id = uuid4().hex
        record = {
            "schema_version": SCHEMA_VERSION,
            "event": "START",
            "recorded_at": _timestamp(),
            "run_key": identity.key,
            "attempt_id": attempt_id,
            "tcf": identity.tcf,
            "working_directory": identity.working_directory,
            "executable": identity.executable,
            "arguments": identity.arguments,
        }
        self._append(record)
        self._attempts[attempt_id] = _Attempt(identity.key)
        self._latest[identity.key] = attempt_id
        return attempt_id

    def finish(self, identity: RunIdentity, attempt_id: str, result: ObservedOutcome) -> None:
        """Persist an observed return code or a definitive Popen failure."""
        if result.return_code is None:
            if not result.error:
                msg = "Cannot finish an attempt without an observed outcome"
                raise ExecutionLogError(msg)
            outcome = "LAUNCH_ERROR"
        else:
            outcome = "SUCCESS" if result.return_code == 0 and not result.error else "FAILED"
            if result.return_code == 0 and result.error:
                msg = "A return code of zero with an error cannot be recorded as success"
                raise ExecutionLogError(msg)
        record = {
            "schema_version": SCHEMA_VERSION,
            "event": "FINISH",
            "recorded_at": _timestamp(),
            "run_key": identity.key,
            "attempt_id": attempt_id,
            "outcome": outcome,
            "return_code": str(result.return_code) if result.return_code is not None else "",
            "tcf": identity.tcf,
            "working_directory": identity.working_directory,
            "executable": identity.executable,
            "arguments": identity.arguments,
            "pid": str(result.pid) if result.pid is not None else "",
            "error": result.error or "",
        }
        if record["error"]:
            record["error"] = record["error"].replace("\r", "\\r").replace("\n", "\\n")
        self._apply(record, 0)  # Validate linkage before the durable append.
        # _apply above only changes in-memory outcome; a failed write must abort execution.
        self._attempts[attempt_id].outcome = None
        self._append(record)
        self._attempts[attempt_id].outcome = outcome


def csv_line(values: tuple[str, ...] | list[str]) -> bytes:
    """One physical CSV line, including correct quoting and a final LF."""
    output = StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerow(values)
    return output.getvalue().encode("utf-8")


class CompletionSession:
    """Bind a frozen plan's identities to the runner's start/finish callbacks."""

    def __init__(self, history: CsvExecutionLog, identities: dict[int, RunIdentity]) -> None:
        self.history = history
        self.identities = identities
        self._active: dict[int, str] = {}

    def before_launch(self, run: PlannedRun) -> None:
        if run.index in self._active:
            msg = f"Run {run.index} was started twice in one invocation"
            raise ExecutionLogError(msg)
        self._active[run.index] = self.history.start(self.identities[run.index])

    def observed_completion(self, result: ObservedOutcome) -> None:
        attempt_id = self._active[result.run.index]
        self.history.finish(self.identities[result.run.index], attempt_id, result)
        del self._active[result.run.index]
