"""Plan, export and execute TUFLOW batches without interpreting a shell.

The immutable PlannedRun from command_plan is the only logical run
description. GPU placement, process handles and observations are transient.
This module neither changes the process working directory nor exits/pauses.
"""

from __future__ import annotations

import itertools
import logging
import os
import re
import shlex
import subprocess
import sys
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from rich.live import Live

from ryan_library.functions.tuflow.command_plan import (
    CommandPlan,
    CommandPlanError,
    DirectoryRequirement,
    PlannedRun,
    parse_command_plan,
    read_command_plans,
)

logger = logging.getLogger(__name__)
_GPU = re.compile(r"-pu\d+(?:,\d+)*", re.IGNORECASE)
_FLAG = re.compile(r"[es][1-9]", re.IGNORECASE)
_PLACEHOLDER = re.compile(r"~([es][1-9])~", re.IGNORECASE)
_PRIORITY = frozenset({"LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"})
_ENGINE = frozenset({"-b", "-x", "-t", "-nlc"})
type RunMode = Literal["parameter_product", "textfiles", "both"]


def _empty_run_variables() -> dict[str, str | Sequence[str]]:
    return {}


@dataclass(frozen=True, slots=True)
class RunnerSettings:
    """Effective settings supplied by the human-editable Python wrapper.

    Relative paths are interpreted against working_directory, not cwd or the
    location of the library. A None TCF permits imported multi-model batches.
    Source -pu and START priority hints are preserved in the logical plan, but
    are replaced by the runner's runtime GPU/priority policy on launch.
    """

    wrapper_file: Path
    working_directory: Path
    executable: Path
    tcf: Path | None
    run_variables: Mapping[str, str | Sequence[str]] = field(default_factory=_empty_run_variables)
    input_files: tuple[Path, ...] = ()
    mode: RunMode = "parameter_product"
    batch_commands: str = "-b"
    priority_order: str | None = None
    gpu_devices: tuple[tuple[str, ...], ...] = ()
    computational_priority: str = "LOW"
    maximum_concurrent: int | None = None
    minimize_on_launch: bool = True
    wait_time_after_run: float = 2.0
    use_live_dashboard: bool = True
    live_refresh_per_second: float = 1.0
    check_gpkg_layers: bool = True


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """One definitive launch failure or an actually observed process exit."""

    run: PlannedRun
    return_code: int | None
    started_at: datetime | None
    finished_at: datetime
    pid: int | None
    error: str | None = None

    @property
    def successful(self) -> bool:
        return self.return_code == 0 and self.error is None


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    """Observed outcomes in completion order, not delayed console order."""

    selected: tuple[PlannedRun, ...]
    outcomes: tuple[RunOutcome, ...]
    interrupted: bool = False

    @property
    def successful(self) -> bool:
        return (
            not self.interrupted
            and len(self.outcomes) == len(self.selected)
            and all(result.successful for result in self.outcomes)
        )

    @property
    def exit_code(self) -> int:
        return 0 if self.successful else 1


# Scope 04 supplies the persistence implementation, not another plan class.
type BeforeLaunch = Callable[[PlannedRun], None]
type ObservedCompletion = Callable[[RunOutcome], None]


@dataclass(slots=True)
class _OwnedChild:
    run: PlannedRun
    process: subprocess.Popen[bytes]
    started_at: datetime
    slot: int | None


def _at_base(path: Path, base: Path) -> Path:
    return path if path.is_absolute() else (base / path).absolute()


def _validate_settings(settings: RunnerSettings) -> None:  # noqa: C901 - validate user policy in one place
    if settings.mode not in {"parameter_product", "textfiles", "both"}:
        message = f"Invalid runner mode: {settings.mode}"
        raise CommandPlanError(message)
    if settings.computational_priority.upper() not in _PRIORITY:
        message = f"Unsupported process priority: {settings.computational_priority}"
        raise CommandPlanError(message)
    if settings.maximum_concurrent is not None and settings.maximum_concurrent < 1:
        message = "maximum_concurrent must be positive"
        raise CommandPlanError(message)
    if settings.wait_time_after_run < 0 or settings.live_refresh_per_second <= 0:
        message = "Launch delay must be nonnegative and dashboard refresh must be positive"
        raise CommandPlanError(message)
    if not settings.working_directory.is_absolute():
        message = "The configured working directory must be absolute"
        raise CommandPlanError(message)
    for group in settings.gpu_devices:
        if not group or any(_GPU.fullmatch(flag) is None for flag in group):
            message = f"Invalid runner GPU group: {group!r}"
            raise CommandPlanError(message)
    try:
        switches = shlex.split(settings.batch_commands, posix=True)
    except ValueError as exc:
        message = f"Invalid batch_commands quoting: {exc}"
        raise CommandPlanError(message) from exc
    if any(token not in _ENGINE and not _GPU.fullmatch(token) for token in switches):
        message = "batch_commands supports only TUFLOW engine switches (-b, -x, -t, -nlc, -puN)"
        raise CommandPlanError(message)
    if settings.mode in {"textfiles", "both"} and not settings.input_files:
        message = "Input BAT/text files are required for textfiles/both mode"
        raise CommandPlanError(message)


def _flags(settings: RunnerSettings) -> tuple[tuple[str, tuple[str, ...]], ...]:
    result: list[tuple[str, tuple[str, ...]]] = []
    for key, raw in settings.run_variables.items():
        if _FLAG.fullmatch(key) is None:
            message = f"Invalid generated event/scenario key: {key!r}"
            raise CommandPlanError(message)
        values = tuple(raw.split() if isinstance(raw, str) else (str(value).strip() for value in raw))
        values = tuple(value for value in values if value)
        if values:
            result.append((key.casefold(), values))
    if settings.priority_order:
        order = settings.priority_order.split()
        result.sort(key=lambda pair: order.index(pair[0]) if pair[0] in order else len(order))
    return tuple(result)


def _generated_plan(settings: RunnerSettings) -> CommandPlan:
    if settings.tcf is None:
        message = "Generated parameter-product runs require a configured Python TCF"
        raise CommandPlanError(message)
    base = settings.working_directory
    tcf = _at_base(settings.tcf, base)
    exe = str(_at_base(settings.executable, base))
    variables = _flags(settings)
    required = {match.casefold() for match in _PLACEHOLDER.findall(tcf.name)}
    present = {key for key, _ in variables}
    if missing := required - present:
        message = f"TCF placeholders require generated flags: {sorted(missing)}"
        raise CommandPlanError(message)
    if any(key in required and not values for key, values in variables):
        message = "TCF placeholder has no generated values"
        raise CommandPlanError(message)
    try:
        batch = tuple(shlex.split(settings.batch_commands))
    except ValueError as exc:
        message = f"Invalid common engine switches: {exc}"
        raise CommandPlanError(message) from exc
    source = settings.wrapper_file.absolute()
    runs: list[PlannedRun] = []
    for number, values in enumerate(itertools.product(*(v for _, v in variables)), 1):
        pairs = tuple((key, value) for (key, _), value in zip(variables, values, strict=True))
        args = (*batch, *(token for key, value in pairs for token in (f"-{key}", value)), str(tcf))
        runs.append(PlannedRun(number, source, 0, 0, tcf, base, pairs, args, exe))
    return CommandPlan(source, tuple(runs), (DirectoryRequirement(base, 0, source),), 0, "commands", (source,))


def prepare_plan(settings: RunnerSettings) -> CommandPlan:
    """Finish every source and TCF consistency check before any side effects.

    Imported files are first, then generated combinations. Both remain one
    immutable ordered plan, including intentional duplicate occurrences.
    """
    _validate_settings(settings)
    plans: list[CommandPlan] = []
    if settings.mode in {"textfiles", "both"}:
        sources = tuple(_at_base(file, settings.working_directory) for file in settings.input_files)
        imported = read_command_plans(
            sources,
            initial_directory=settings.working_directory,
            configuration_directory=settings.working_directory,
            fallback_tcf=str(settings.tcf) if settings.tcf is not None else None,
        )
        plans.append(imported)
    if settings.mode in {"parameter_product", "both"}:
        plans.append(_generated_plan(settings))
    combined = tuple(replace(run, index=i) for i, run in enumerate((run for plan in plans for run in plan.runs), 1))
    if not combined:
        message = "No simulations were selected"
        raise CommandPlanError(message)
    return CommandPlan(
        source=plans[0].source,
        runs=combined,
        directories=tuple(requirement for plan in plans for requirement in plan.directories),
        ignored_statements=sum(plan.ignored_statements for plan in plans),
        syntax=plans[0].syntax if len(plans) == 1 else "mixed",
        source_files=tuple(source for plan in plans for source in plan.source_files),
    )


def _executable(run: PlannedRun, settings: RunnerSettings) -> str:
    return (
        run.executable if run.executable is not None else str(_at_base(settings.executable, settings.working_directory))
    )


def _model_switches(run: PlannedRun) -> tuple[str, ...]:
    """Remove an explicit TCF argument; the plan already owns the effective TCF.

    Bare flag rows legitimately have no TCF token. Do not confuse a scenario
    value ending in '.tcf' with a control-file argument.
    """
    result: list[str] = []
    expecting_flag_value = False
    for token in run.arguments:
        if expecting_flag_value:
            result.append(token)
            expecting_flag_value = False
        elif re.fullmatch(r"-[es][1-9]", token, re.IGNORECASE):
            result.append(token)
            expecting_flag_value = True
        elif not token.startswith("-") and token.casefold().endswith(".tcf"):
            continue
        else:
            result.append(token)
    return tuple(result)


def _model_arguments(run: PlannedRun) -> tuple[str, ...]:
    """Retain all accepted model switches except source GPU placement."""
    return tuple(token for token in _model_switches(run) if not _GPU.fullmatch(token))


def _command(run: PlannedRun, settings: RunnerSettings, group: tuple[str, ...]) -> list[str]:
    return [_executable(run, settings), *_model_arguments(run), *group, str(run.tcf)]


def _bat_quote(value: str, *, always: bool = False) -> str:
    if not value or any(char in value for char in '\r\n"%!^&|<>'):
        message = f"Cannot safely represent BAT argument {value!r}"
        raise CommandPlanError(message)
    if always or any(char.isspace() for char in value):
        return f'"{value}"'
    return value


def _logical_signature(run: PlannedRun, settings: RunnerSettings) -> tuple[object, ...]:
    # Source text can omit or reposition the TCF; compare effective model inputs.
    return (
        str(run.tcf),
        str(run.working_directory),
        _executable(run, settings),
        (*_model_switches(run), str(run.tcf)),
    )


def render_batch_commands(plan: CommandPlan, settings: RunnerSettings, *, destination: Path) -> str:
    """Produce a statically readable BAT file, then reparse and verify each run.

    A shared TUFLOW_EXE and BCMD are editable. TCF[_N] and additional executable
    variables retain different models and solvers rather than replacing them.
    START hints are deliberately regenerated from the runner's priority policy.
    """
    if not plan.runs:
        message = "Cannot export an empty batch"
        raise CommandPlanError(message)
    tcfs = tuple(dict.fromkeys(str(run.tcf) for run in plan.runs))
    exes = tuple(dict.fromkeys(_executable(run, settings) for run in plan.runs))
    tcf_names = {value: ("TCF" if len(tcfs) == 1 else f"TCF_{i}") for i, value in enumerate(tcfs, 1)}
    exe_names = {value: ("TUFLOW_EXE" if i == 1 else f"TUFLOW_EXE_{i}") for i, value in enumerate(exes, 1)}
    arguments = [_model_switches(run) for run in plan.runs]
    # Only factor common *leading engine switches*. Event/scenario values remain
    # visible on each START line even if they happen to match.
    common: list[str] = []
    if arguments:
        for column in zip(*arguments, strict=False):
            if len(set(column)) == 1 and (column[0].casefold() in _ENGINE or _GPU.fullmatch(column[0])):
                common.append(column[0])
            else:
                break
    priority = settings.computational_priority.upper()
    switch = f"/{priority} "
    minimized = "/MIN " if settings.minimize_on_launch else ""
    result = ["@echo off", "REM Edited values below apply to every matching START row."]
    for value, name in exe_names.items():
        result.append(f'set "{name}={value}"')
    result.append(f'set "BCMD={" ".join(common) if common else " "}"')
    for value, name in tcf_names.items():
        result.append(f'set "{name}={value}"')
    result.append("")
    for run, args in zip(plan.runs, arguments, strict=True):
        cwd = _bat_quote(str(run.working_directory), always=True)
        exe_var = exe_names[_executable(run, settings)]
        tcf_var = tcf_names[str(run.tcf)]
        remaining = " ".join(_bat_quote(token) for token in args[len(common) :])
        result.append(
            f'START /D {cwd} {switch}{minimized}/WAIT "" "%{exe_var}%" %BCMD% {remaining} "%{tcf_var}%"'.rstrip()
        )
    result.append("")
    result.append("REM Commands are intentionally ordered; duplicate rows are deliberate.")
    text = "\r\n".join(result) + "\r\n"
    exported = parse_command_plan(
        text,
        source=destination.absolute(),
        initial_directory=settings.working_directory,
        syntax="batch",
    )
    original = tuple(_logical_signature(run, settings) for run in plan.runs)
    roundtrip = tuple(_logical_signature(run, settings) for run in exported.runs)
    if original != roundtrip:
        message = "BAT export/reparse changed executable, TCF, cwd, model switches or occurrence order"
        raise CommandPlanError(message)
    return text


def export_batch_commands(plan: CommandPlan, settings: RunnerSettings, destination: Path) -> Path:
    """Exclusive-create output: never overwrite any imported source or prior export."""
    target = _at_base(destination, settings.working_directory)
    for source in plan.source_files:
        existing = Path(str(source))
        if os.path.normcase(str(target)) == os.path.normcase(str(existing)):
            message = f"Refusing to overwrite imported command source: {source}"
            raise CommandPlanError(message)
        if target.exists() and existing.exists() and target.samefile(existing):
            message = f"Refusing to overwrite imported command source alias: {source}"
            raise CommandPlanError(message)
    content = render_batch_commands(plan, settings, destination=target)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8", newline="") as output:
        output.write(content)
    return target


def _gpu_startup_message(groups: tuple[tuple[str, ...], ...]) -> None:
    """Exactly one external snapshot for an executing (nonempty) batch."""
    from ryan_library.functions.tuflow.gpu_occupancy import gpu_ids_from_flags, local_gpu_occupancy

    try:
        snapshot = local_gpu_occupancy()
    except Exception as exc:  # noqa: BLE001 - external inspection cannot gate launches
        logger.warning("Startup GPU inspection unavailable: %s; assignments are unknown.", exc)
        return
    if not snapshot.assigned and not snapshot.unknown_pids and not snapshot.inspection_incomplete:
        logger.info("No other running local TUFLOW models detected.")
    assigned = {identifier for process in snapshot.assigned for identifier in process.gpu_ids}
    capacity = [tuple(sorted(gpu_ids_from_flags(group))) for group in groups]
    configured_ids: set[int] = {identifier for group in capacity for identifier in group}
    if groups and capacity and all(group and set(group).intersection(assigned) for group in capacity):
        details = ", ".join(
            f"PID {process.pid}: GPUs {sorted(process.gpu_ids)}"
            for process in snapshot.assigned
            if process.gpu_ids.intersection(configured_ids)
        )
        logger.warning("All configured GPU groups appear occupied by external TUFLOW runs (%s).", details)
    elif snapshot.assigned and groups:
        logger.info("Configured GPU pool has capacity against confirmed external assignments.")
    elif snapshot.assigned and not groups:
        logger.warning("GPU assignment is automatic; external model GPU sharing cannot be assessed.")
    if snapshot.unknown_pids:
        logger.warning("External TUFLOW GPU assignments are unknown for PIDs %s.", snapshot.unknown_pids)
    if snapshot.inspection_incomplete:
        logger.warning("Local process inspection is incomplete; GPU capacity is uncertain.")
    if groups and any(not group for group in capacity):
        logger.warning("At least one configured GPU group has no explicit device identifier.")


class AdvisoryInputCheck:
    """At most one daemon validation of the frozen plan; never gates launching."""

    def __init__(self, plan: CommandPlan, *, check_gpkg_layers: bool = True) -> None:
        self.plan = plan
        self._layers = check_gpkg_layers
        self._messages: SimpleQueue[tuple[int, str]] = SimpleQueue()
        self._lock = Lock()
        self._cancel = Event()
        self.finished = Event()
        self._thread = Thread(target=self._work, name="tuflow-runner-preflight", daemon=True)

    def _work(self) -> None:
        try:
            from ryan_library.orchestrators.tuflow.command_preflight import check_command_plan

            report = check_command_plan(self.plan, check_gpkg_layers=self._layers)
            messages = [
                (
                    logging.WARNING,
                    (
                        f"Input check run {issue.run.index} {issue.run.source}:{issue.run.line_start}: "
                        f"[{issue.status}] {issue.origin}: {issue.reference} ({issue.detail})"
                    ),
                )
                for issue in report.issues
            ]
            messages.append(
                (
                    logging.INFO if report.passed else logging.WARNING,
                    (
                        f"Input check {'passed' if report.passed else 'failed or incomplete'} for "
                        f"{report.completed_runs}/{report.run_count} runs."
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 - advisory checks are never a launch prerequisite
            messages = [(logging.WARNING, f"Input checking unavailable or incomplete: {type(exc).__name__}: {exc}")]
        with self._lock:
            if not self._cancel.is_set():
                for message in messages:
                    self._messages.put(message)
            self.finished.set()

    def start(self) -> None:
        self._thread.start()

    def drain(self) -> None:
        while True:
            try:
                level, message = self._messages.get_nowait()
            except Empty:
                break
            logger.log(level, "%s", message)

    def close(self) -> None:
        self.drain()
        with self._lock:
            if not self.finished.is_set():
                self._cancel.set()
                logger.warning("Input checking incomplete at shutdown; no all-clear is implied.")
        self.drain()


def check_plan_only(plan: CommandPlan, *, check_gpkg_layers: bool = True) -> bool:
    """Blocking explicit preflight; failures and unavailable APIs are nonzero."""
    from ryan_library.orchestrators.tuflow.command_preflight import check_command_plan

    try:
        report = check_command_plan(plan, check_gpkg_layers=check_gpkg_layers)
    except Exception:
        logger.exception("Input checking failed or is unavailable.")
        return False
    for issue in report.issues:
        logger.warning(
            "Input check run %d %s:%d [%s] %s: %s (%s)",
            issue.run.index,
            issue.run.source,
            issue.run.line_start,
            issue.status,
            issue.origin,
            issue.reference,
            issue.detail,
        )
    if not report.passed:
        logger.error("Input checking failed or is incomplete: %d/%d completed", report.completed_runs, report.run_count)
    return report.passed


def _apply_priority(pid: int, name: str) -> None:
    """Set Windows child priority without assuming POSIX scheduling parity."""
    if sys.platform != "win32":
        return
    import psutil

    constants = {
        "LOW": psutil.IDLE_PRIORITY_CLASS,
        "BELOWNORMAL": psutil.BELOW_NORMAL_PRIORITY_CLASS,
        "NORMAL": psutil.NORMAL_PRIORITY_CLASS,
        "ABOVENORMAL": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        "HIGH": psutil.HIGH_PRIORITY_CLASS,
        "REALTIME": psutil.REALTIME_PRIORITY_CLASS,
    }
    psutil.Process(pid).nice(constants[name.upper()])


def _startupinfo(minimize: bool) -> subprocess.STARTUPINFO | None:
    if not minimize or sys.platform != "win32":
        return None
    info = subprocess.STARTUPINFO()
    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    info.wShowWindow = 7
    return info


def _stop_children(active: list[_OwnedChild]) -> None:
    """Request termination and observe exits; never invent successful completion."""
    for child in active:
        try:
            if child.process.poll() is None:
                child.process.terminate()
        except OSError as exc:
            logger.warning("Could not terminate TUFLOW PID %s: %s", child.process.pid, exc)
    for child in active:
        try:
            if child.process.poll() is None:
                child.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                child.process.kill()
                child.process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired) as exc:
                logger.warning("Could not confirm termination of PID %s: %s", child.process.pid, exc)
        except OSError as exc:
            logger.warning("Could not confirm termination of PID %s: %s", child.process.pid, exc)


def launch_plan(  # noqa: C901 - single owned-process lifecycle and observation boundary
    plan: CommandPlan,
    settings: RunnerSettings,
    *,
    on_before_launch: BeforeLaunch | None = None,
    on_observed_completion: ObservedCompletion | None = None,
    interrupted: Event | None = None,
    popen: Callable[..., subprocess.Popen[bytes]] | None = None,
    advisory: AdvisoryInputCheck | None = None,
) -> BatchOutcome:
    """Launch each immutable occurrence once, with its own cwd and argv.

    The prelaunch hook is called before Popen; the completion hook is called
    immediately when poll observes exit, *before* ordered console output.
    The hooks provide scope 04's append-only START/FINISH integration point.
    """
    if not plan.runs:
        message = "No selected simulations to launch"
        raise CommandPlanError(message)
    groups = settings.gpu_devices
    capacity = len(groups) if groups else 1
    if settings.maximum_concurrent is not None:
        capacity = min(capacity, settings.maximum_concurrent)
    if capacity < 1:
        message = "No runner slots configured"
        raise CommandPlanError(message)
    stop = interrupted if interrupted is not None else Event()
    factory: Callable[..., subprocess.Popen[bytes]] = (
        popen if popen is not None else cast(Callable[..., subprocess.Popen[bytes]], subprocess.Popen)
    )
    queue = deque(plan.runs)
    active: list[_OwnedChild] = []
    owned_slots: set[int] = set()
    outcomes: list[RunOutcome] = []
    ordered: dict[int, RunOutcome] = {}
    next_print = 0
    has_source_gpu = any(_GPU.fullmatch(token) for run in plan.runs for token in run.arguments)
    has_source_priority = any(run.start_options for run in plan.runs)
    if has_source_gpu:
        logger.warning("Imported -pu switches are retained for export but replaced by runner GPU settings at launch.")
    if has_source_priority:
        logger.info("Imported START priority/minimise/wait hints are replaced by runner scheduling settings.")
    if stop.is_set():
        return BatchOutcome(tuple(plan.runs), (), True)
    _gpu_startup_message(groups)

    dashboard: Live | None = None

    def redraw() -> None:
        if dashboard is None:
            return
        from rich.table import Table

        table = Table(title=f"TUFLOW batch ({len(outcomes)}/{len(plan.runs)} observed)")
        table.add_column("Run", justify="right")
        table.add_column("Model")
        table.add_column("Status")
        visible = plan.runs if len(plan.runs) <= 20 else (*plan.runs[:15], *plan.runs[-5:])
        for run in visible:
            completed = next((item for item in outcomes if item.run.index == run.index), None)
            state = (
                "OK"
                if completed and completed.successful
                else "FAIL"
                if completed
                else "RUNNING"
                if any(item.run.index == run.index for item in active)
                else "PENDING"
            )
            table.add_row(str(run.index), run.tcf.name, state)
        dashboard.update(table, refresh=True)

    def observed(result: RunOutcome) -> None:
        nonlocal next_print
        outcomes.append(result)
        ordered[result.run.index] = result
        if on_observed_completion is not None:
            on_observed_completion(result)
        # Presentation can be ordered, but the lifecycle callback above cannot.
        while next_print < len(plan.runs) and plan.runs[next_print].index in ordered:
            item = ordered.pop(plan.runs[next_print].index)
            logger.info(
                "Run %d: %s (exit %s)%s",
                item.run.index,
                "OK" if item.successful else "FAIL",
                item.return_code,
                f": {item.error}" if item.error else "",
            )
            next_print += 1
        redraw()

    def poll_children() -> bool:
        any_finished = False
        for child in active.copy():
            code = child.process.poll()
            if code is None:
                continue
            active.remove(child)
            if child.slot is not None:
                owned_slots.discard(child.slot)
            observed(RunOutcome(child.run, code, child.started_at, datetime.now(), child.process.pid))
            any_finished = True
        return any_finished

    if settings.use_live_dashboard:
        from rich.live import Live

        dashboard = Live(refresh_per_second=settings.live_refresh_per_second, transient=False)
        dashboard.start()
        redraw()
    try:
        while queue or active:
            if advisory is not None:
                advisory.drain()
            if stop.is_set():
                break
            poll_children()
            while queue and len(active) < capacity and not stop.is_set():
                slot = next((i for i in range(len(groups)) if i not in owned_slots), None) if groups else None
                if groups and slot is None:
                    break
                run = queue.popleft()
                group = groups[slot] if slot is not None else ()
                args = _command(run, settings, group)
                started = datetime.now()
                if on_before_launch is not None:
                    on_before_launch(run)
                try:
                    process = factory(
                        args=args,
                        cwd=str(run.working_directory),
                        creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
                        startupinfo=_startupinfo(settings.minimize_on_launch),
                    )
                except OSError as exc:
                    observed(RunOutcome(run, None, started, datetime.now(), None, f"Launch failed: {exc}"))
                    continue
                # Ownership *must* precede nice(), logging or any other fallible action.
                active.append(_OwnedChild(run, process, started, slot))
                if slot is not None:
                    owned_slots.add(slot)
                try:
                    _apply_priority(process.pid, settings.computational_priority)
                except Exception as exc:  # noqa: BLE001 - do not lose ownership after Popen succeeds
                    logger.warning("PID %s launched but process priority could not be set: %s", process.pid, exc)
                logger.info("Launched %d/%d PID %s: %s", run.index, len(plan.runs), process.pid, args)
                redraw()
                if queue and settings.wait_time_after_run:
                    time.sleep(settings.wait_time_after_run)
                poll_children()
            if active:
                time.sleep(0.1)
        if stop.is_set():
            logger.warning("Batch interrupted. Terminating owned child processes.")
            _stop_children(active)
            poll_children()
        if advisory is not None:
            advisory.drain()
    finally:
        try:
            if active:
                _stop_children(active)
                poll_children()
        finally:
            if dashboard is not None:
                dashboard.stop()
    return BatchOutcome(tuple(plan.runs), tuple(outcomes), stop.is_set())
