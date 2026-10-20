"""Coordinate recent TUFLOW activity, TLF inspection, and CodeMeter usage reporting."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from itertools import islice
from pathlib import Path
import re
from typing import Any

from ryan_library.functions.parse_tlf import get_log_lines, is_complete_tlf, search_for_completion, search_from_top
from ryan_library.functions.tuflow.codemeter import (
    CodeMeterAllocation,
    CodeMeterLicenseDenial,
    CodeMeterSnapshot,
    fetch_codemeter_snapshot,
)


MAX_RECENT_HOURS = 7 * 24
TIMESTAMP_FORMAT = "%Y-%b-%d %H:%M"
TIMESTAMP_PATTERN = re.compile(r"^(?P<timestamp>\d{4}-[A-Za-z]{3}-\d{2}\s+\d{2}:\d{2})")
LICENCE_PATTERN = re.compile(
    r"\bDon:\s+(?P<donation>\S+)\s+LOC\s+(?P<seat>\d+/\d+)\s+(?P<machine>\S+)",
    re.IGNORECASE,
)
NAMED_EVENT_PATTERN = re.compile(r"\b(?P<event>Started|Finished):\s+(?P<model>\S+)", re.IGNORECASE)
RUNTIME_PATTERN = re.compile(r"\bTot:\s+(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2})\s+CT\b")
QUOTED_TLF_PATTERN = re.compile(r'"(?P<path>[^"]+\.tlf)"\s*$', re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ActivityRecord:
    """One parsed TUFLOW global licence-log record."""

    timestamp: datetime
    event: str
    donation: str
    machine: str
    seat: str
    model: str
    runtime_seconds: int | None
    tlf_path: Path | None
    source: Path


@dataclass(frozen=True, slots=True)
class UserActivity:
    """Recent simulation activity for one user directory."""

    user: str
    recent_starts: tuple[ActivityRecord, ...] = ()
    recent_completions: tuple[ActivityRecord, ...] = ()
    error: str = ""


@dataclass(frozen=True, slots=True)
class TlfInspection:
    """Completion and hardware details read from one referenced TLF."""

    status: str
    gpu_device_ids: str = "-"
    runtime_seconds: int | None = None
    modified_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SimulationReportRow:
    """One simulation with independently inspected TLF state."""

    user: str
    record: ActivityRecord
    inspection: TlfInspection
    status: str


@dataclass(frozen=True, slots=True)
class CodeMeterProcessRow:
    """One active CodeMeter client process and its correlated simulation."""

    user: str
    process_id: int | None
    first_access: datetime | None
    last_access: datetime | None
    allocations: tuple[CodeMeterAllocation, ...]
    simulation: SimulationReportRow | None


@dataclass(frozen=True, slots=True)
class TuflowActivityReport:
    """Complete read-only TUFLOW and CodeMeter activity snapshot."""

    checked_at: datetime
    log_root: Path
    recent_hours: float
    starts: tuple[SimulationReportRow, ...]
    completions: tuple[SimulationReportRow, ...]
    codemeter: CodeMeterSnapshot | None
    codemeter_processes: tuple[CodeMeterProcessRow, ...]
    access_warnings: tuple[str, ...]
    workflow_warnings: tuple[str, ...]


def parse_activity_line(line: str, *, source: Path) -> ActivityRecord | None:
    """Parse one global TUFLOW licence-log line."""
    timestamp_match = TIMESTAMP_PATTERN.match(line)
    licence_match = LICENCE_PATTERN.search(line)
    if timestamp_match is None or licence_match is None:
        return None
    try:
        timestamp = datetime.strptime(timestamp_match.group("timestamp"), TIMESTAMP_FORMAT)
    except ValueError:
        return None

    named_event = NAMED_EVENT_PATTERN.search(line)
    if named_event is not None:
        event = named_event.group("event").upper()
        model = named_event.group("model")
    elif " Ini:" in line and " Tot:" in line:
        event = "FINISHED"
        model = "(not recorded by this TUFLOW build)"
    else:
        event = "ACTIVITY"
        model = "(not recorded)"

    runtime_match = RUNTIME_PATTERN.search(line)
    runtime_seconds = None
    if runtime_match is not None:
        runtime_seconds = (
            int(runtime_match.group("hours")) * 3600
            + int(runtime_match.group("minutes")) * 60
            + int(runtime_match.group("seconds"))
        )
    path_match = QUOTED_TLF_PATTERN.search(line)
    return ActivityRecord(
        timestamp=timestamp,
        event=event,
        donation=licence_match.group("donation"),
        machine=licence_match.group("machine"),
        seat=licence_match.group("seat"),
        model=model,
        runtime_seconds=runtime_seconds,
        tlf_path=Path(path_match.group("path")) if path_match is not None else None,
        source=source,
    )


def read_user_activity(user_directory: Path, *, activity_since: datetime) -> UserActivity:
    """Read recent starts and completions from one global-log user folder."""
    recent_starts: list[ActivityRecord] = []
    recent_completions: list[ActivityRecord] = []
    errors: list[str] = []
    try:
        log_files = sorted(user_directory.glob("*.log"), key=lambda path: path.name.casefold())
    except OSError as exc:
        return UserActivity(user=user_directory.name, error=str(exc))
    if not log_files:
        return UserActivity(user=user_directory.name, error="No .log files found")

    for log_file in log_files:
        active_by_seat: dict[tuple[str, str, str], ActivityRecord] = {}
        try:
            with log_file.open("r", encoding="utf-8", errors="replace") as stream:
                for line in stream:
                    record = parse_activity_line(line, source=log_file)
                    if record is None:
                        continue
                    seat_key = (record.donation, record.machine, record.seat)
                    if record.event == "STARTED":
                        active_by_seat[seat_key] = record
                        if record.timestamp >= activity_since:
                            recent_starts.append(record)
                    elif record.event == "FINISHED":
                        matching_start = active_by_seat.pop(seat_key, None)
                        if matching_start is not None:
                            record = replace(
                                record,
                                model=(
                                    matching_start.model
                                    if record.model.startswith("(not recorded")
                                    else record.model
                                ),
                                tlf_path=record.tlf_path or matching_start.tlf_path,
                            )
                        if record.timestamp >= activity_since:
                            recent_completions.append(record)
        except OSError as exc:
            errors.append(f"{log_file.name}: {exc}")

    return UserActivity(
        user=user_directory.name,
        recent_starts=tuple(sorted(recent_starts, key=lambda record: record.timestamp, reverse=True)),
        recent_completions=tuple(sorted(recent_completions, key=lambda record: record.timestamp, reverse=True)),
        error="; ".join(errors),
    )


def collect_user_activity(log_root: Path, *, activity_since: datetime) -> list[UserActivity]:
    """Collect recent activity from immediate user directories."""
    user_directories = sorted(
        (path for path in log_root.iterdir() if path.is_dir()),
        key=lambda path: path.name.casefold(),
    )
    return [read_user_activity(path, activity_since=activity_since) for path in user_directories]


def inspect_tlf(tlf_path: Path | None) -> TlfInspection:
    """Inspect one TLF with the parsing functions used by ``TLFProcessor``."""
    if tlf_path is None:
        return TlfInspection(status="NO TLF PATH")
    try:
        file_stat = tlf_path.stat()
    except FileNotFoundError:
        return TlfInspection(status="MISSING")
    except OSError:
        return TlfInspection(status="UNAVAILABLE")

    modified_at = datetime.fromtimestamp(file_stat.st_mtime)
    lines, last_lines = get_log_lines(
        logfile_path=tlf_path,
        is_large_file=file_stat.st_size > 10 * 1024 * 1024,
    )
    if not lines and not last_lines:
        return TlfInspection(status="EMPTY/UNREADABLE", modified_at=modified_at)

    data: dict[str, Any] = {}
    sim_complete = 0
    current_section: str | None = None
    for line in last_lines:
        data, sim_complete, current_section = search_for_completion(
            line=line,
            data_dict=data,
            sim_complete=sim_complete,
            current_section=current_section,
        )

    try:
        top_lines: Iterable[str]
        if lines:
            top_lines = lines[:5000]
            close_stream = None
        else:
            close_stream = tlf_path.open("r", encoding="utf-8", errors="replace")
            top_lines = islice(close_stream, 5000)
        try:
            success = 0
            spec_events = False
            spec_scen = False
            spec_var = False
            for line in top_lines:
                data, success, spec_events, spec_scen, spec_var = search_from_top(
                    line=line,
                    data_dict=data,
                    success=success,
                    spec_events=spec_events,
                    spec_scen=spec_scen,
                    spec_var=spec_var,
                )
                if "GPU_Device_IDs" in data:
                    break
        finally:
            if close_stream is not None:
                close_stream.close()
    except OSError:
        return TlfInspection(status="UNAVAILABLE", modified_at=modified_at)

    gpu_device_ids = str(data.get("GPU_Device_IDs", "-")).strip().rstrip(",").strip() or "-"
    final_runtime = data.get("Final_RunTime")
    runtime_seconds = round(float(final_runtime) * 3600) if isinstance(final_runtime, int | float) else None
    status = "COMPLETE" if is_complete_tlf(data_dict=data, sim_complete=sim_complete) else "INCOMPLETE"
    return TlfInspection(
        status=status,
        gpu_device_ids=gpu_device_ids,
        runtime_seconds=runtime_seconds,
        modified_at=modified_at,
    )


def tlf_status_for_record(record: ActivityRecord, inspection: TlfInspection) -> str:
    """Reject a completed TLF that predates its corresponding start record."""
    if (
        inspection.status == "COMPLETE"
        and inspection.modified_at is not None
        and inspection.modified_at < record.timestamp - timedelta(minutes=2)
    ):
        return "STALE TLF"
    return inspection.status


def simulation_status(record: ActivityRecord, inspection: TlfInspection) -> str:
    """Classify one simulation without considering another model's completion."""
    tlf_status = tlf_status_for_record(record, inspection)
    if tlf_status == "COMPLETE":
        return "COMPLETED"
    if tlf_status == "INCOMPLETE":
        return "POSSIBLY RUNNING"
    return "UNKNOWN"


def find_project_folder(tlf_path: Path | None) -> Path | None:
    """Return the nearest ancestor whose name begins with ``TUFLOW``."""
    if tlf_path is None:
        return None
    for parent in tlf_path.parents:
        if parent.name.casefold().startswith("tuflow"):
            return parent
    return tlf_path.parent


def _deduplicate_starts(entries: Iterable[tuple[str, ActivityRecord]]) -> list[tuple[str, ActivityRecord]]:
    newest: dict[tuple[str, ...], tuple[str, ActivityRecord]] = {}
    for user, record in entries:
        if record.tlf_path is not None:
            key = ("path", str(record.tlf_path).casefold())
        else:
            key = ("fallback", user.casefold(), record.machine.casefold(), record.seat, record.model.casefold())
        existing = newest.get(key)
        if existing is None or record.timestamp > existing[1].timestamp:
            newest[key] = (user, record)
    return sorted(newest.values(), key=lambda item: item[1].timestamp, reverse=True)


def _deduplicate_completions(entries: Iterable[tuple[str, ActivityRecord]]) -> list[tuple[str, ActivityRecord]]:
    unique: dict[tuple[str, ...], tuple[str, ActivityRecord]] = {}
    for user, record in entries:
        path_or_model = str(record.tlf_path).casefold() if record.tlf_path is not None else record.model.casefold()
        key = (user.casefold(), record.timestamp.isoformat(), record.machine.casefold(), record.seat, path_or_model)
        unique[key] = (user, record)
    return sorted(unique.values(), key=lambda item: item[1].timestamp, reverse=True)


def _normalise_user(value: str) -> str:
    return value.rsplit("\\", maxsplit=1)[-1].casefold()


def _start_distance_seconds(row: SimulationReportRow, timestamp: datetime) -> float:
    return abs((row.record.timestamp - timestamp).total_seconds())


def _group_codemeter_processes(
    allocations: Iterable[CodeMeterAllocation],
    *,
    starts: tuple[SimulationReportRow, ...],
) -> tuple[CodeMeterProcessRow, ...]:
    grouped: dict[tuple[str, int | None], list[CodeMeterAllocation]] = {}
    for allocation in allocations:
        grouped.setdefault((_normalise_user(allocation.user), allocation.process_id), []).append(allocation)

    rows: list[CodeMeterProcessRow] = []
    used_simulations: set[tuple[str, str]] = set()
    for (normalized_user, process_id), process_allocations in grouped.items():
        first_values = [item.first_access for item in process_allocations if item.first_access is not None]
        last_values = [item.last_access for item in process_allocations if item.last_access is not None]
        first_access = min(first_values) if first_values else None
        candidates = [row for row in starts if _normalise_user(row.user) == normalized_user]
        if first_access is not None:
            reference_time: datetime = first_access
            candidates.sort(key=lambda row: _start_distance_seconds(row, reference_time))
            candidates = [row for row in candidates if _start_distance_seconds(row, reference_time) <= 10 * 60]
        match = None
        for candidate in candidates:
            identity = (candidate.user.casefold(), str(candidate.record.tlf_path).casefold())
            if identity not in used_simulations:
                match = candidate
                used_simulations.add(identity)
                break
        rows.append(
            CodeMeterProcessRow(
                user=process_allocations[0].user,
                process_id=process_id,
                first_access=first_access,
                last_access=max(last_values) if last_values else None,
                allocations=tuple(sorted(process_allocations, key=lambda item: item.product_code)),
                simulation=match,
            )
        )
    return tuple(sorted(rows, key=lambda row: row.first_access or datetime.min, reverse=True))


def build_activity_report(
    *,
    log_root: Path,
    recent_hours: float,
    codemeter_url: str | None,
    codemeter_timeout_seconds: float = 5.0,
) -> TuflowActivityReport:
    """Build a read-only TUFLOW and CodeMeter activity snapshot."""
    if recent_hours <= 0 or recent_hours > MAX_RECENT_HOURS:
        msg = f"recent_hours must be greater than zero and no more than {MAX_RECENT_HOURS}"
        raise ValueError(msg)
    absolute_log_root = log_root.absolute()
    if not absolute_log_root.is_dir():
        msg = f"TUFLOW log folder does not exist: {absolute_log_root}"
        raise FileNotFoundError(msg)

    checked_at = datetime.now()
    activity_since = checked_at - timedelta(hours=recent_hours)
    activities = collect_user_activity(absolute_log_root, activity_since=activity_since)
    starts = _deduplicate_starts(
        (activity.user, record) for activity in activities for record in activity.recent_starts
    )
    completions = _deduplicate_completions(
        (activity.user, record) for activity in activities for record in activity.recent_completions
    )

    inspection_cache: dict[str, TlfInspection] = {}

    def inspection_for(record: ActivityRecord) -> TlfInspection:
        if record.tlf_path is None:
            return TlfInspection(status="NO TLF PATH")
        key = str(record.tlf_path).casefold()
        if key not in inspection_cache:
            inspection_cache[key] = inspect_tlf(record.tlf_path)
        return inspection_cache[key]

    start_rows = tuple(
        SimulationReportRow(
            user=user,
            record=record,
            inspection=inspection_for(record),
            status=simulation_status(record, inspection_for(record)),
        )
        for user, record in starts
    )
    completion_rows = tuple(
        SimulationReportRow(
            user=user,
            record=record,
            inspection=inspection_for(record),
            status="COMPLETED",
        )
        for user, record in completions
    )

    workflow_warnings: list[str] = []
    codemeter = None
    codemeter_processes: tuple[CodeMeterProcessRow, ...] = ()
    if codemeter_url:
        try:
            codemeter = fetch_codemeter_snapshot(
                base_url=codemeter_url,
                timeout_seconds=codemeter_timeout_seconds,
            )
            workflow_warnings.extend(codemeter.warnings)
            codemeter_processes = _group_codemeter_processes(codemeter.allocations, starts=start_rows)
        except OSError as exc:
            workflow_warnings.append(f"CodeMeter WebAdmin is unavailable at {codemeter_url}: {exc}")

    access_warnings = tuple(
        f"{activity.user}: {activity.error}" for activity in activities if activity.error
    )
    return TuflowActivityReport(
        checked_at=checked_at,
        log_root=absolute_log_root,
        recent_hours=recent_hours,
        starts=start_rows,
        completions=completion_rows,
        codemeter=codemeter,
        codemeter_processes=codemeter_processes,
        access_warnings=access_warnings,
        workflow_warnings=tuple(workflow_warnings),
    )


def _format_age(timestamp: datetime, *, now: datetime) -> str:
    minutes = max(0, int((now - timestamp).total_seconds())) // 60
    if minutes < 60:
        return f"{minutes}m"
    hours, remaining_minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h {remaining_minutes:02d}m"
    days, remaining_hours = divmod(hours, 24)
    return f"{days}d {remaining_hours:02d}h"


def _format_runtime(runtime_seconds: int | None) -> str:
    if runtime_seconds is None:
        return "-"
    hours, remainder = divmod(runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def _truncate(value: str, *, width: int = 48) -> str:
    return value if len(value) <= width else f"{value[: width - 3]}..."


def _table(rows: Iterable[tuple[str, ...]], *, headers: tuple[str, ...]) -> list[str]:
    materialized = list(rows)
    widths = [len(header) for header in headers]
    for row in materialized:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def formatted(row: tuple[str, ...]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip()

    return [
        formatted(headers),
        formatted(tuple("-" * width for width in widths)),
        *(formatted(row) for row in materialized),
    ]


def _local_naive(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.astimezone().replace(tzinfo=None)


def _recent_denials(
    denials: Iterable[CodeMeterLicenseDenial],
    *,
    since: datetime,
) -> list[CodeMeterLicenseDenial]:
    return [denial for denial in denials if _local_naive(denial.timestamp) >= since]


def render_activity_report(report: TuflowActivityReport) -> str:
    """Render a console report without mutating external state."""
    lines = [
        f"TUFLOW user activity from: {report.log_root}",
        f"Checked: {report.checked_at:%Y-%m-%d %H:%M} (local time)",
        (
            f"Window: last {report.recent_hours:g} hours (maximum 168). Each simulation is assessed independently; "
            "another model's completion does not affect it."
        ),
        "",
        "CODEMETER CURRENT LICENCE UTILISATION",
    ]

    if report.codemeter is None:
        lines.append("CodeMeter data unavailable or disabled.")
    elif report.codemeter.products:
        lines.extend(
            _table(
                (
                    (
                        str(product.product_code),
                        product.name,
                        str(product.used),
                        str(product.total),
                        str(product.available),
                    )
                    for product in report.codemeter.products
                ),
                headers=("CODE", "LICENCE", "USED", "TOTAL", "AVAILABLE"),
            )
        )
    else:
        lines.append("No CodeMeter product quantities were found.")

    lines.extend(("", "ACTIVE CODEMETER TUFLOW PROCESSES"))
    if report.codemeter_processes:
        process_rows: list[tuple[str, ...]] = []
        for process in report.codemeter_processes:
            simulation = process.simulation
            process_rows.append(
                (
                    process.user,
                    str(process.process_id or "-"),
                    process.first_access.strftime("%Y-%m-%d %H:%M:%S") if process.first_access else "-",
                    process.last_access.strftime("%Y-%m-%d %H:%M:%S") if process.last_access else "-",
                    simulation.inspection.gpu_device_ids if simulation else "-",
                    simulation.status if simulation else "UNMATCHED",
                    _truncate(simulation.record.model) if simulation else "(no matching recent start)",
                    ", ".join(f"{item.product_code}:H{item.handle_id}" for item in process.allocations),
                )
            )
        lines.extend(
            _table(
                process_rows,
                headers=(
                    "USER",
                    "PID",
                    "FIRST ACCESS",
                    "LAST ACCESS",
                    "GPU ID",
                    "TLF STATUS",
                    "MODEL",
                    "LICENCE:HANDLE",
                ),
            )
        )
    else:
        lines.append("No active CodeMeter TUFLOW allocations were found.")

    lines.extend(("", f"RECENT SIMULATION STARTS (LAST {report.recent_hours:g} HOURS)"))
    if report.starts:
        lines.extend(
            _table(
                (
                    (
                        row.status,
                        row.user,
                        row.record.timestamp.strftime("%Y-%m-%d %H:%M"),
                        _format_age(row.record.timestamp, now=report.checked_at),
                        row.record.machine,
                        row.record.seat,
                        row.inspection.gpu_device_ids,
                        tlf_status_for_record(row.record, row.inspection),
                        _truncate(row.record.model),
                    )
                    for row in report.starts
                ),
                headers=("STATUS", "USER", "STARTED", "AGE", "MACHINE", "SEAT", "GPU ID", "TLF", "MODEL"),
            )
        )
    else:
        lines.append("No simulation starts were recorded in this period.")

    lines.extend(("", f"RECENT COMPLETIONS (LAST {report.recent_hours:g} HOURS)"))
    if report.completions:
        lines.extend(
            _table(
                (
                    (
                        row.user,
                        row.record.timestamp.strftime("%Y-%m-%d %H:%M"),
                        _format_age(row.record.timestamp, now=report.checked_at),
                        _format_runtime(row.record.runtime_seconds or row.inspection.runtime_seconds),
                        row.record.machine,
                        row.record.seat,
                        row.inspection.gpu_device_ids,
                        tlf_status_for_record(row.record, row.inspection),
                        _truncate(row.record.model),
                    )
                    for row in report.completions
                ),
                headers=("USER", "FINISHED", "AGE", "RUNTIME", "MACHINE", "SEAT", "GPU ID", "TLF", "MODEL"),
            )
        )
    else:
        lines.append("No completions were recorded in this period.")

    if report.codemeter is not None:
        since = report.checked_at - timedelta(hours=report.recent_hours)
        denials = _recent_denials(report.codemeter.denials, since=since)
        lines.extend(("", f"RECENT CODEMETER LICENCE DENIALS (LAST {report.recent_hours:g} HOURS)"))
        if denials:
            product_names = {item.product_code: item.name for item in report.codemeter.products}
            lines.extend(
                _table(
                    (
                        (
                            _local_naive(denial.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                            denial.user,
                            str(denial.product_code),
                            product_names.get(denial.product_code, "UNKNOWN PRODUCT"),
                        )
                        for denial in denials
                    ),
                    headers=("TIME", "USER", "CODE", "LICENCE"),
                )
            )
        else:
            lines.append("No no-more-licences events were recorded in this period.")

    lines.extend(("", "PROJECT AND TUFLOW LOG PATHS"))
    path_rows = _deduplicate_starts(
        (row.user, row.record) for row in (*report.starts, *report.completions)
    )
    if path_rows:
        for index, (user, record) in enumerate(path_rows, start=1):
            project_folder = find_project_folder(record.tlf_path)
            lines.extend(
                (
                    f"[{index}] {user} — {record.model}",
                    f"    Project: {project_folder if project_folder is not None else '(not identified)'}",
                    f"    TLF:     {record.tlf_path if record.tlf_path is not None else '(not recorded)'}",
                )
            )
    else:
        lines.append("No referenced TUFLOW log paths were recorded in this period.")

    warnings = (*report.workflow_warnings, *report.access_warnings)
    if warnings:
        lines.extend(("", "WARNINGS"))
        lines.extend(f"WARNING: {warning}" for warning in warnings)
    lines.extend(("", "This report is read-only and is not proof that a TUFLOW process is still running."))
    return "\n".join(lines)
