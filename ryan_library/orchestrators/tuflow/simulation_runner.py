"""Plan, export and execute TUFLOW batches without interpreting a shell.

The immutable PlannedRun from command_plan is the only logical run
description. GPU placement, process handles and observations are transient.
This module neither changes the process working directory nor exits/pauses.
"""

from __future__ import annotations

import itertools
import os
import re
import shlex
import subprocess
import sys
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path, PurePath
from queue import Empty, SimpleQueue
from tempfile import NamedTemporaryFile
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Literal, Protocol, cast

from loguru import logger

if TYPE_CHECKING:
    from ...functions.live_dashboard import LiveWorkflowDashboard

from ...functions.tuflow.command_plan import (
    CommandPlan,
    CommandPlanError,
    DirectoryRequirement,
    PlannedRun,
    deduplicate_plan,
    is_gpu_switch,
    model_arguments,
    normalize_plan_path,
    parse_command_plan,
    read_command_plans,
    validate_run_arguments,
)

_FLAG = re.compile(r"[es][1-9]", re.IGNORECASE)
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
    Python batch_commands replaces imported engine/GPU switches during planning.
    BAT/TXT input never controls executable, child cwd, process scheduling or GPU.
    Runtime GPU groups and process priority control the launched children.
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
    minimize_on_launch: bool = True
    wait_time_after_run: float = 2.0
    use_live_dashboard: bool = True
    live_refresh_per_second: float = 1.0


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


class _ProcessHandle(Protocol):
    """Process operations the scheduler actually uses, including during cleanup.

    Both real subprocess.Popen objects and deterministic test doubles satisfy
    this interface; tests no longer pretend a fake is a Popen instance.
    """

    pid: int

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def kill(self) -> None: ...


type _ProcessFactory = Callable[..., _ProcessHandle]


@dataclass(slots=True)
class _OwnedChild:
    run: PlannedRun
    process: _ProcessHandle
    started_at: datetime
    slot: int | None


def _at_base(path: Path, base: Path) -> Path:
    return path if path.is_absolute() else (base / path).absolute()


def _validate_settings(settings: RunnerSettings) -> tuple[str, ...]:
    if settings.mode not in {"parameter_product", "textfiles", "both"}:
        message = f"Invalid runner mode: {settings.mode}"
        raise CommandPlanError(message)
    if settings.computational_priority.upper() not in _PRIORITY:
        message = f"Unsupported process priority: {settings.computational_priority}"
        raise CommandPlanError(message)
    if settings.wait_time_after_run < 0 or settings.live_refresh_per_second <= 0:
        message = "Launch delay must be nonnegative and dashboard refresh must be positive"
        raise CommandPlanError(message)
    if not settings.working_directory.is_absolute():
        message = "The configured working directory must be absolute"
        raise CommandPlanError(message)
    assigned_gpu_ids: set[int] = set()
    for group in settings.gpu_devices:
        if not group or any(not isinstance(flag, str) or not is_gpu_switch(flag) for flag in group):
            message = (
                f"Invalid GPU group in GPU_DEVICES: {group!r}. Use '-pu0' or "
                "['-pu0', '-pu1'] for separate slots; [['-pu0', '-pu1']] for one multi-GPU slot."
            )
            raise CommandPlanError(message)
        group_ids = [int(flag[3:]) for flag in group]
        if len(set(group_ids)) != len(group_ids) or assigned_gpu_ids.intersection(group_ids):
            message = f"Duplicate GPU identifiers in GPU_DEVICES GPU groups: {group!r}"
            raise CommandPlanError(message)
        assigned_gpu_ids.update(group_ids)
    try:
        switches = shlex.split(settings.batch_commands, posix=True)
    except ValueError as exc:
        message = f"Invalid batch_commands quoting: {exc}"
        raise CommandPlanError(message) from exc
    if any(token not in _ENGINE and not is_gpu_switch(token) for token in switches):
        message = "batch_commands supports only TUFLOW engine switches (-b, -x, -t, -nlc, -puN)"
        raise CommandPlanError(message)
    if settings.mode in {"textfiles", "both"} and not settings.input_files:
        message = "Input BAT/text files are required for textfiles/both mode"
        raise CommandPlanError(message)
    return tuple(switches)


def _required_tcf_flags(tcf: PurePath) -> set[str]:
    """Validate TCF filename syntax, not physical file existence."""
    if tcf.suffix.casefold() != ".tcf":
        message = f"TCF must end in .tcf, got {tcf!s}"
        raise CommandPlanError(message)
    name = tcf.name
    if name.count("~") % 2:
        message = f"TCF filename has an unpaired ~ in {name!r}; use ~e1~ or ~s1~."
        raise CommandPlanError(message)
    placeholders = re.findall(r"~([^~]*)~", name)
    for flag in placeholders:
        if _FLAG.fullmatch(flag) is None:
            message = f"Invalid TCF filename placeholder ~{flag}~ in {name!r}; use ~e1~..~e9~ or ~s1~..~s9~."
            raise CommandPlanError(message)
    return {flag.casefold() for flag in placeholders}


def _check_tcf_flags(tcf: PurePath, pairs: tuple[tuple[str, str], ...]) -> None:
    """Ensure each TCF filename placeholder has a supplied -eN/-sN value."""
    missing = _required_tcf_flags(tcf) - {key.casefold() for key, _ in pairs}
    if missing:
        example = " ".join(f"-{key} VALUE" for key in sorted(missing))
        message = (
            f"TCF {tcf.name!r} requires missing event/scenario flags {sorted(missing)}. "
            f"Add {example} to RUN_VARIABLES_RAW or the imported BAT/TXT command."
        )
        raise CommandPlanError(message)


def _flags(settings: RunnerSettings) -> tuple[tuple[str, tuple[str, ...]], ...]:
    result: list[tuple[str, tuple[str, ...]]] = []
    for key, raw in settings.run_variables.items():
        if _FLAG.fullmatch(key) is None:
            message = f"Invalid generated event/scenario key: {key!r}"
            raise CommandPlanError(message)
        values = tuple(raw.split() if isinstance(raw, str) else (str(value).strip() for value in raw))
        values = tuple(value for value in values if value)
        if not values:
            message = f"RUN_VARIABLES_RAW[{key!r}] is empty. Remove this key instead of leaving a blank value."
            raise CommandPlanError(message)
        result.append((key.casefold(), values))
    if settings.priority_order:
        order = settings.priority_order.split()
        result.sort(key=lambda pair: order.index(pair[0]) if pair[0] in order else len(order))
    return tuple(result)


def _generated_plan(settings: RunnerSettings, batch: tuple[str, ...]) -> CommandPlan:
    if settings.tcf is None:
        message = "Generated parameter-product runs require a configured Python TCF"
        raise CommandPlanError(message)
    base = settings.working_directory
    tcf = _at_base(settings.tcf, base)
    exe = str(_at_base(settings.executable, base))
    variables = _flags(settings)
    required = _required_tcf_flags(tcf)
    present = {key for key, _ in variables}
    if missing := required - present:
        example = ", ".join(f"{key!r}: 'VALUE'" for key in sorted(missing))
        message = (
            f"TCF {tcf.name!r} requires missing generated flags {sorted(missing)}. "
            f"Add {example} to RUN_VARIABLES_RAW."
        )
        raise CommandPlanError(message)
    source = settings.wrapper_file.absolute()
    runs: list[PlannedRun] = []
    for number, values in enumerate(itertools.product(*(v for _, v in variables)), 1):
        pairs = tuple((key, value) for (key, _), value in zip(variables, values, strict=True))
        args = (*batch, *(token for key, value in pairs for token in (f"-{key}", value)), str(tcf))
        validate_run_arguments(args)
        runs.append(PlannedRun(number, source, 0, 0, tcf, base, pairs, args, exe))
    return CommandPlan(source, tuple(runs), (DirectoryRequirement(base, 0, source),), 0, "commands", (source,))


def prepare_plan(settings: RunnerSettings) -> CommandPlan:
    """Finish every source and TCF consistency check before any side effects.

    Imported files are first, then generated combinations. Both remain one
    immutable ordered plan, including intentional duplicate occurrences.
    """
    batch = _validate_settings(settings)
    plans: list[CommandPlan] = []
    if settings.mode in {"textfiles", "both"}:
        sources = tuple(_at_base(file, settings.working_directory) for file in settings.input_files)
        imported = read_command_plans(
            sources,
            initial_directory=settings.working_directory,
            configuration_directory=settings.working_directory,
            fallback_tcf=str(settings.tcf) if settings.tcf is not None else None,
            environment=dict(os.environ),
            # Every source extension is inert model-selection data for Python.
            # Its configured UNC cwd does not need CMD temporary drive mapping.
            syntax="commands",
        )
        runs: list[PlannedRun] = []
        for run in imported.runs:
            arguments = (*batch, *model_arguments(run, include_gpu=False, include_batch=False), str(run.tcf))
            validate_run_arguments(arguments)
            _check_tcf_flags(run.tcf, run.flag_pairs)
            # Source START, executable, and CD declarations are parsed as data,
            # never run or replayed. The Python settings own actual launches.
            runs.append(
                replace(
                    run,
                    arguments=arguments,
                    executable=str(_at_base(settings.executable, settings.working_directory)),
                    working_directory=settings.working_directory,
                )
            )
        plans.append(
            replace(
                imported,
                runs=tuple(runs),
                directories=(DirectoryRequirement(settings.working_directory, 0, settings.wrapper_file),),
            )
        )
        logger.info("Imported selections ignore source executable, launch directory, engine and GPU switches.")
    if settings.mode in {"parameter_product", "both"}:
        plans.append(_generated_plan(settings, batch))
    combined = tuple(replace(run, index=i) for i, run in enumerate((run for plan in plans for run in plan.runs), 1))
    if not combined:
        message = "No simulations were selected"
        raise CommandPlanError(message)
    result = deduplicate_plan(
        CommandPlan(
            source=plans[0].source,
            runs=combined,
            directories=(DirectoryRequirement(settings.working_directory, 0, settings.wrapper_file),),
            ignored_statements=sum(plan.ignored_statements for plan in plans),
            syntax=plans[0].syntax if len(plans) == 1 else "mixed",
            source_files=tuple(source for plan in plans for source in plan.source_files),
            duplicates_dropped=sum(plan.duplicates_dropped for plan in plans),
        )
    )
    if result.duplicates_dropped:
        logger.warning("Discarded {} duplicate TUFLOW selections, keeping the first.", result.duplicates_dropped)
    return result


def _executable(run: PlannedRun, settings: RunnerSettings) -> str:
    """Never use an executable path supplied by an imported BAT/TXT file."""
    del run
    return str(_at_base(settings.executable, settings.working_directory))


def validate_launch_executable(plan: CommandPlan, settings: RunnerSettings) -> None:
    """Verify the Python-selected solver before CSV RESET/START writes.

    Only actual execution invokes this. Export and explicit input checking
    remain usable without a solver. Refuse Windows batch/script launchers.
    """
    if not plan.runs:
        message = "No simulations were selected"
        raise CommandPlanError(message)
    executable = Path(_executable(plan.runs[0], settings))
    if os.name == "nt" and executable.suffix.casefold() != ".exe":
        message = f"Python TUFLOW_EXE must be an .exe, not a shell script: {executable}"
        raise CommandPlanError(message)
    if not executable.is_file():
        message = f"Configured Python TUFLOW_EXE does not exist: {executable}"
        raise CommandPlanError(message)


def _command(run: PlannedRun, settings: RunnerSettings, group: tuple[str, ...]) -> list[str]:
    return [_executable(run, settings), *model_arguments(run, include_gpu=False), *group, str(run.tcf)]


def _bat_quote(value: str, *, always: bool = False) -> str:
    # Double quotes protect CMD operators in data arguments. Percent/delayed
    # expansion, embedded quotes and caret expressions remain unsupported.
    if not value or any(char in value for char in '\r\n"%!^'):
        message = f"Cannot safely represent BAT argument {value!r}"
        raise CommandPlanError(message)
    if always or any(char.isspace() or char in "&|<>()" for char in value):
        return f'"{value}"'
    return value


def _logical_signature(run: PlannedRun, settings: RunnerSettings) -> tuple[object, ...]:
    # Source text can omit/reposition the TCF and normalize parent components.
    # Compare effective inputs without changing user-supplied launch spellings.
    return (
        normalize_plan_path(run.tcf),
        normalize_plan_path(run.working_directory),
        normalize_plan_path(_at_base(settings.executable, settings.working_directory)),
        model_arguments(run),
    )


def render_batch_commands(plan: CommandPlan, settings: RunnerSettings, *, destination: Path) -> str:
    """Render Windows batch syntax for a command TXT and verify every run.

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
    arguments = [model_arguments(run) for run in plan.runs]
    # Only factor common *leading engine switches*. Event/scenario values remain
    # visible on each START line even if they happen to match.
    common: list[str] = []
    if arguments:
        for column in zip(*arguments, strict=False):
            if len(set(column)) == 1 and (column[0].casefold() in _ENGINE or is_gpu_switch(column[0])):
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
    result.append("REM Commands are ordered; repeated equivalent model selections were removed.")
    text = "\r\n".join(result) + "\r\n"
    exported = parse_command_plan(
        text,
        source=destination.absolute(),
        initial_directory=settings.working_directory,
        # Match normal .txt import semantics: these are inert Python launch
        # selections, so a UNC cwd does not require a CMD drive mapping.
        syntax="commands",
    )
    original = tuple(_logical_signature(run, settings) for run in plan.runs)
    roundtrip = tuple(_logical_signature(run, settings) for run in exported.runs)
    if original != roundtrip:
        message = "BAT export/reparse changed executable, TCF, cwd, model switches or occurrence order"
        raise CommandPlanError(message)
    return text


def _protected_export_paths(plan: CommandPlan, settings: RunnerSettings) -> tuple[Path, ...]:
    wrapper = settings.wrapper_file.absolute()
    history = wrapper.with_name(f"{wrapper.stem}_runs.csv")
    return tuple(
        dict.fromkeys(
            (
                wrapper,
                history,
                *(Path(str(source)) for source in plan.source_files),
                *(_at_base(source, settings.working_directory) for source in settings.input_files),
                *(Path(str(run.tcf)) for run in plan.runs),
                *(_at_base(Path(_executable(run, settings)), Path(str(run.working_directory))) for run in plan.runs),
            )
        )
    )


def _check_export_target(target: Path, protected: tuple[Path, ...]) -> None:
    if target.is_symlink():
        message = f"Refusing to overwrite a symbolic-link command output: {target}"
        raise CommandPlanError(message)
    # Physical resolution is deliberate here: replacement must not follow a
    # parent junction/symlink onto a protected source, wrapper, model or CSV.
    physical_target = target.resolve()
    for source in protected:
        if physical_target == source.resolve() or (target.exists() and source.exists() and target.samefile(source)):
            message = f"Refusing to overwrite a protected input, wrapper or history file: {source}"
            raise CommandPlanError(message)
    if target.is_dir():
        message = f"Command output is a directory, not a TXT file: {target}"
        raise CommandPlanError(message)


def export_batch_commands(plan: CommandPlan, settings: RunnerSettings, destination: Path) -> Path:
    """Atomically refresh a .txt export without overwriting any known input.

    The full plan is rendered and reparsed before replacement. A failed render,
    write or replace leaves the previous export intact. Temporary siblings are
    removed on success and failure; no backup or output directory is created.
    """
    target = _at_base(destination, settings.working_directory)
    if target.suffix.casefold() != ".txt":
        message = "Command exports require a .txt filename to prevent accidental batch execution"
        raise CommandPlanError(message)
    protected = _protected_export_paths(plan, settings)
    _check_export_target(target, protected)
    content = render_batch_commands(plan, settings, destination=target)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        # Recheck immediately before replacement in case filesystem aliases
        # changed while the temporary export was being written.
        _check_export_target(target, protected)
        temporary.replace(target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return target


def _gpu_startup_message(groups: tuple[tuple[str, ...], ...]) -> None:
    """Exactly one external snapshot for an executing (nonempty) batch."""
    from ...functions.tuflow.gpu_occupancy import gpu_conflicts, gpu_ids_from_flags, local_gpu_occupancy

    try:
        snapshot = local_gpu_occupancy()
    except Exception as exc:  # noqa: BLE001 - external inspection cannot gate launches
        logger.warning("Startup GPU inspection unavailable: {}; assignments are unknown.", exc)
        return
    if not snapshot.assigned and not snapshot.unknown_pids and not snapshot.inspection_incomplete:
        logger.info("No other running local TUFLOW models detected.")
    assigned = {identifier for process in snapshot.assigned for identifier in process.gpu_ids}
    capacity = [tuple(sorted(gpu_ids_from_flags(group))) for group in groups]
    configured_ids: set[int] = {identifier for group in capacity for identifier in group}
    if groups and capacity and all(group and set(group).intersection(assigned) for group in capacity):
        details = ", ".join(
            f"PID {pid}: GPUs {sorted(identifiers)}"
            for pid, identifiers in gpu_conflicts(configured_ids, snapshot.assigned)
        )
        logger.warning("All configured GPU groups appear occupied by external TUFLOW runs ({}).", details)
    elif snapshot.assigned and groups:
        logger.info("Configured GPU pool has capacity against confirmed external assignments.")
    elif snapshot.assigned and not groups:
        logger.warning("GPU assignment is automatic; external model GPU sharing cannot be assessed.")
    if snapshot.unknown_pids:
        logger.warning("External TUFLOW GPU assignments are unknown for PIDs {}.", snapshot.unknown_pids)
    if snapshot.inspection_incomplete:
        logger.warning("Local process inspection is incomplete; GPU capacity is uncertain.")
    if groups and any(not group for group in capacity):
        logger.warning("At least one configured GPU group has no explicit device identifier.")


class AdvisoryInputCheck:
    """At most one daemon validation of the frozen plan; never gates launching."""

    def __init__(self, plan: CommandPlan, *, check_gpkg_layers: bool = True) -> None:
        self.plan = plan
        self._layers = check_gpkg_layers
        self._messages: SimpleQueue[tuple[str, str]] = SimpleQueue()
        self._lock = Lock()
        self._cancel = Event()
        self.finished = Event()
        self._thread = Thread(target=self._work, name="tuflow-runner-preflight", daemon=True)

    def _work(self) -> None:
        try:
            from .command_preflight import check_command_plan

            # Keep upstream inspection inside this daemon. Thread-pool workers
            # are joined at interpreter exit and could hold shutdown on slow I/O.
            report = check_command_plan(self.plan, check_gpkg_layers=self._layers, io_workers=1)
            messages = [
                (
                    "WARNING",
                    (
                        f"Input check run {issue.run.index} {issue.run.source}:{issue.run.line_start}: "
                        f"[{issue.finding.status}] {issue.finding.origin}: "
                        f"{issue.finding.reference} ({issue.finding.detail})"
                    ),
                )
                for issue in report.issues
            ]
            messages.append(
                (
                    "SUCCESS" if report.passed else "WARNING",
                    (
                        f"Input check {'passed' if report.passed else 'failed or incomplete'} for "
                        f"{report.completed_runs}/{report.run_count} runs."
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 - advisory checks are never a launch prerequisite
            messages = [("WARNING", f"Input checking unavailable or incomplete: {type(exc).__name__}: {exc}")]
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
            logger.log(level, "{}", message)

    def close(self) -> None:
        self.drain()
        with self._lock:
            if not self.finished.is_set():
                self._cancel.set()
                logger.warning("Input checking incomplete at shutdown; no all-clear is implied.")
        self.drain()


def check_plan_only(plan: CommandPlan, *, check_gpkg_layers: bool = True) -> bool:
    """Blocking explicit preflight; failures and unavailable APIs are nonzero."""
    from .command_preflight import check_command_plan

    try:
        report = check_command_plan(plan, check_gpkg_layers=check_gpkg_layers)
    except Exception:  # noqa: BLE001 - explicit diagnostic mode returns a failed check
        logger.exception("Input checking failed or is unavailable.")
        return False
    for issue in report.issues:
        logger.warning(
            "Input check run {} {}:{} [{}] {}: {} ({})",
            issue.run.index,
            issue.run.source,
            issue.run.line_start,
            issue.finding.status,
            issue.finding.origin,
            issue.finding.reference,
            issue.finding.detail,
        )
    if not report.passed:
        logger.error("Input checking failed or is incomplete: {}/{} completed", report.completed_runs, report.run_count)
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
            logger.warning("Could not terminate TUFLOW PID {}: {}", child.process.pid, exc)
    for child in active:
        try:
            if child.process.poll() is None:
                child.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                child.process.kill()
                child.process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired) as exc:
                logger.warning("Could not confirm termination of PID {}: {}", child.process.pid, exc)
        except OSError as exc:
            logger.warning("Could not confirm termination of PID {}: {}", child.process.pid, exc)


def launch_plan(  # noqa: C901 - single owned-process lifecycle and observation boundary
    plan: CommandPlan,
    settings: RunnerSettings,
    *,
    on_before_launch: BeforeLaunch | None = None,
    on_observed_completion: ObservedCompletion | None = None,
    interrupted: Event | None = None,
    popen: _ProcessFactory | None = None,
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
    # Each configured GPU group is one execution slot; automatic placement
    # without explicit groups provides a single slot.
    capacity = len(groups) if groups else 1
    if capacity < 1:
        message = "No runner slots configured"
        raise CommandPlanError(message)
    stop = interrupted if interrupted is not None else Event()
    # This cast applies to the genuine stdlib constructor, never to a fake.
    factory: _ProcessFactory = popen if popen is not None else cast(_ProcessFactory, subprocess.Popen)
    queue = deque(plan.runs)
    active: list[_OwnedChild] = []
    owned_slots: set[int] = set()
    outcomes: list[RunOutcome] = []
    ordered: dict[int, RunOutcome] = {}
    next_print = 0
    has_configured_gpu = any(is_gpu_switch(token) for run in plan.runs for token in run.arguments)
    has_source_priority = any(run.start_options for run in plan.runs)
    if has_configured_gpu:
        logger.warning("Python BATCH_COMMANDS -pu switches are exported but runner GPU groups control launches.")
    if has_source_priority:
        logger.info("Imported START priority/minimise/wait hints are replaced by runner scheduling settings.")
    if stop.is_set():
        return BatchOutcome(tuple(plan.runs), (), True)
    _gpu_startup_message(groups)

    dashboard: LiveWorkflowDashboard | None = None
    # Resume keeps the original logical indices. The renderer owns separate,
    # contiguous display slots and shows original indices through metadata.
    display_indices = {run.index: index for index, run in enumerate(plan.runs, 1)}

    def observed(result: RunOutcome) -> None:
        nonlocal next_print
        outcomes.append(result)
        ordered[result.run.index] = result
        if on_observed_completion is not None:
            on_observed_completion(result)
        # Presentation can be ordered, but the lifecycle callback above cannot.
        while next_print < len(plan.runs) and plan.runs[next_print].index in ordered:
            item = ordered.pop(plan.runs[next_print].index)
            logger.log(
                "SUCCESS" if item.successful else "ERROR",
                "Run {}: {} (exit {}){}",
                item.run.index,
                "OK" if item.successful else "FAIL",
                item.return_code,
                f": {item.error}" if item.error else "",
            )
            next_print += 1
        if dashboard is not None:
            dashboard.mark_finished(
                index=display_indices[result.run.index],
                status="OK" if result.successful else "FAIL",
                detail=result.error or f"Exit {result.return_code}",
                refresh=False,
            )
            dashboard.set_active_count(len(active), refresh=False)
            dashboard.refresh(force=True)

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
        from ...functions.live_dashboard import LiveWorkflowDashboard, WorkflowColumn

        dashboard = LiveWorkflowDashboard(
            title="TUFLOW simulations",
            refresh_per_second=settings.live_refresh_per_second,
            transient=False,
            columns=(
                WorkflowColumn("Run", "metadata", metadata_key="run_index", justify="right", no_wrap=True),
                WorkflowColumn("Model", "label"),
                WorkflowColumn("Status", "status", no_wrap=True),
                WorkflowColumn("Duration", "duration", no_wrap=True),
                WorkflowColumn("Detail", "detail"),
            ),
        )
        dashboard.set_tasks(
            labels=[run.tcf.name for run in plan.runs],
            metadata=[{"run_index": run.index} for run in plan.runs],
        )
    with dashboard if dashboard is not None else nullcontext():
        try:
            while queue or active:
                if advisory is not None:
                    advisory.drain()
                if dashboard is not None:
                    dashboard.refresh()
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
                            cwd=str(settings.working_directory),
                            creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
                            startupinfo=_startupinfo(settings.minimize_on_launch),
                        )
                    except OSError as exc:
                        observed(RunOutcome(run, None, started, datetime.now(), None, f"Launch failed: {exc}"))
                        continue
                    # Ownership must precede priority, presentation or another fallible operation.
                    active.append(_OwnedChild(run, process, started, slot))
                    if slot is not None:
                        owned_slots.add(slot)
                    try:
                        _apply_priority(process.pid, settings.computational_priority)
                    except Exception as exc:  # noqa: BLE001 - keep ownership after Popen succeeds
                        logger.warning("PID {} launched but process priority could not be set: {}", process.pid, exc)
                    logger.info(
                        "Launched run {} ({} selected) PID {}: {}", run.index, len(plan.runs), process.pid, args
                    )
                    if dashboard is not None:
                        dashboard.mark_running(
                            index=display_indices[run.index], detail=f"PID {process.pid}", refresh=False
                        )
                        dashboard.set_active_count(len(active), refresh=False)
                        dashboard.refresh(force=True)
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
            if active:
                _stop_children(active)
                poll_children()
    return BatchOutcome(tuple(plan.runs), tuple(outcomes), stop.is_set())
