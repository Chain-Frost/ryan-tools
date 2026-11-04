"""Plan, export and execute TUFLOW batches without interpreting a shell.

The immutable PlannedRun from command_plan is the only logical run
description. GPU placement, process handles and observations are transient.
This module neither changes the process working directory nor exits/pauses.
"""

from __future__ import annotations

import itertools
import math
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

from ...functions.loguru_helpers import log_settings

if TYPE_CHECKING:
    from ...functions.live_dashboard import LiveWorkflowDashboard, WorkflowAlert
    from ...functions.tuflow.run_progress import LogProgressSnapshot
    from .command_preflight import CommandPreflightIssue

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
from ...functions.tuflow.gpu_devices import gpu_device_label, gpu_group_label, local_cuda_device_names
from .run_progress import AdvisoryRunProgress, ProgressUpdate

_FLAG = re.compile(r"[es][1-9]", re.IGNORECASE)
_PRIORITY = frozenset({"LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"})
_ENGINE = frozenset({"-b", "-x", "-t", "-nlc"})
type RunMode = Literal["parameter_product", "textfiles", "both"]
type GpuAvailabilityIssueKind = Literal["occupied", "unknown_assignment", "inspection_incomplete", "query_failed"]


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
    wait_for_available_gpu: bool = True
    gpu_check_interval_seconds: float = 60.0
    computational_priority: str = "LOW"
    minimize_on_launch: bool = True
    wait_time_after_run: float = 2.0
    use_live_dashboard: bool = True
    live_refresh_per_second: float = 1.0
    live_max_rows: int = 10
    monitor_progress: bool = True
    progress_poll_interval_seconds: float = 10.0
    show_progress_diagnostics: bool = True


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


@dataclass(frozen=True, slots=True)
class GpuAvailabilityIssue:
    """One confirmed or uncertain reason configured GPU slots cannot run."""

    kind: GpuAvailabilityIssueKind
    slot_indexes: frozenset[int]
    gpu_ids: frozenset[int] = frozenset()
    pids: tuple[int, ...] = ()
    detail: str = ""


@dataclass(frozen=True, slots=True)
class GpuAvailability:
    """One local process-inspection result used by guarded scheduling."""

    blocked_slots: frozenset[int] = frozenset()
    issues: tuple[GpuAvailabilityIssue, ...] = ()


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


def _is_valid_gpu_flag(flag: object) -> bool:
    """Check untrusted runtime GPU values even if the caller is type-annotated."""
    return isinstance(flag, str) and is_gpu_switch(flag)


def _validate_gpu_groups(groups: tuple[tuple[str, ...], ...]) -> None:
    """Reject malformed or shared GPU assignments before any export/launch."""
    assigned_gpu_ids: set[int] = set()
    for group in groups:
        if not group or any(not _is_valid_gpu_flag(flag) for flag in group):
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


def _validate_gpu_wait_settings(settings: RunnerSettings) -> None:
    """Validate optional GPU-scheduling controls independently of plan complexity."""
    if type(settings.wait_for_available_gpu) is not bool:
        message = "WAIT_FOR_AVAILABLE_GPU must be True or False"
        raise CommandPlanError(message)
    if not math.isfinite(settings.gpu_check_interval_seconds) or settings.gpu_check_interval_seconds <= 0:
        message = "GPU_CHECK_INTERVAL_SECONDS must be a finite positive number"
        raise CommandPlanError(message)


def _validate_progress_settings(settings: RunnerSettings) -> None:
    """Reject invalid observation controls before any exports or launches."""
    if type(settings.monitor_progress) is not bool:
        message = "MONITOR_PROGRESS must be True or False"
        raise CommandPlanError(message)
    if type(settings.show_progress_diagnostics) is not bool:
        message = "SHOW_PROGRESS_DIAGNOSTICS must be True or False"
        raise CommandPlanError(message)
    if (
        type(settings.progress_poll_interval_seconds) not in (int, float)
        or not math.isfinite(settings.progress_poll_interval_seconds)
        or settings.progress_poll_interval_seconds <= 0
    ):
        message = "PROGRESS_POLL_INTERVAL_SECONDS must be a finite positive number"
        raise CommandPlanError(message)


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
    if type(settings.live_max_rows) is not int or settings.live_max_rows < 1:
        message = "Dashboard row limit must be a positive integer (>=1)"
        raise CommandPlanError(message)
    _validate_gpu_wait_settings(settings)
    _validate_progress_settings(settings)
    if not settings.working_directory.is_absolute():
        message = "The configured working directory must be absolute"
        raise CommandPlanError(message)
    _validate_gpu_groups(settings.gpu_devices)
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


def _warn_unusual_flag_values(plan: CommandPlan) -> None:
    """Warn about individual event/scenario values containing spaces or underscores."""
    reported: set[tuple[str, str, str]] = set()
    for run in plan.runs:
        for flag, value in run.flag_pairs:
            for character, description in ((" ", "a space (' ')"), ("_", "an underscore ('_')")):
                signature = (flag.casefold(), value, character)
                if character not in value or signature in reported:
                    continue
                logger.warning(
                    "Are you sure you want to include {} character in your variable names: -{}? Value: {!r}",
                    description,
                    flag,
                    value,
                )
                reported.add(signature)


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
            f"TCF {tcf.name!r} requires missing generated flags {sorted(missing)}. Add {example} to RUN_VARIABLES_RAW."
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
            ignored_exclamations=tuple(ignored for plan in plans for ignored in plan.ignored_exclamations),
        )
    )
    if result.duplicates_dropped:
        logger.warning("Discarded {} duplicate TUFLOW selections, keeping the first.", result.duplicates_dropped)
    if result.ignored_exclamations:
        count = sum(ignored.count for ignored in result.ignored_exclamations)
        logger.warning(
            "Ignored {} literal '!' character(s) across {} source line(s); planned runs and outputs may be wrong.",
            count,
            len(result.ignored_exclamations),
        )
        for ignored in result.ignored_exclamations:
            logger.warning(
                "Ignored {} '!' character(s) at {}:{}.",
                ignored.count,
                display_runner_path(ignored.source, settings.working_directory),
                ignored.line,
            )
    _warn_unusual_flag_values(result)
    _log_plan_preview(result, settings)
    return result


def _log_plan_preview(plan: CommandPlan, settings: RunnerSettings) -> None:
    """Restore the startup settings and complete selection list before execution."""
    values: tuple[tuple[str, object], ...] = (
        ("WRAPPER", settings.wrapper_file),
        ("WORKING_DIR", settings.working_directory),
        ("TCF", settings.tcf),
        ("TUFLOW_EXE", settings.executable),
        ("MODE", settings.mode),
        ("INPUT_FILES", settings.input_files),
        ("RUN_VARIABLES", settings.run_variables),
        ("BATCH_COMMANDS", settings.batch_commands),
        ("PRIORITY_ORDER", settings.priority_order),
        ("GPU_DEVICES", settings.gpu_devices or "automatic placement"),
        ("WAIT_FOR_AVAILABLE_GPU", settings.wait_for_available_gpu),
        ("GPU_CHECK_INTERVAL_SECONDS", settings.gpu_check_interval_seconds),
        ("CONCURRENT_SLOTS", len(settings.gpu_devices) or 1),
        ("COMPUTATIONAL_PRIORITY", settings.computational_priority),
        ("MINIMIZE_ON_LAUNCH", settings.minimize_on_launch),
        ("WAIT_TIME_AFTER_RUN (seconds)", settings.wait_time_after_run),
        ("USE_LIVE_DASHBOARD", settings.use_live_dashboard),
        ("LIVE_REFRESH_PER_SECOND", settings.live_refresh_per_second),
        ("LIVE_MAX_ROWS", settings.live_max_rows),
        ("MONITOR_PROGRESS", settings.monitor_progress),
        ("PROGRESS_POLL_INTERVAL_SECONDS", settings.progress_poll_interval_seconds),
        ("SHOW_PROGRESS_DIAGNOSTICS", settings.show_progress_diagnostics),
    )
    baseline = RunnerSettings(
        wrapper_file=settings.wrapper_file,
        working_directory=settings.working_directory,
        executable=settings.executable,
        tcf=settings.tcf,
    )
    defaults: dict[str, object] = {
        name: getattr(baseline, name.lower())
        for name in (
            "MODE",
            "INPUT_FILES",
            "RUN_VARIABLES",
            "BATCH_COMMANDS",
            "PRIORITY_ORDER",
            "COMPUTATIONAL_PRIORITY",
            "MINIMIZE_ON_LAUNCH",
            "USE_LIVE_DASHBOARD",
            "LIVE_REFRESH_PER_SECOND",
            "LIVE_MAX_ROWS",
            "WAIT_FOR_AVAILABLE_GPU",
            "GPU_CHECK_INTERVAL_SECONDS",
            "MONITOR_PROGRESS",
            "PROGRESS_POLL_INTERVAL_SECONDS",
            "SHOW_PROGRESS_DIAGNOSTICS",
        )
    }
    defaults.update(
        {
            "GPU_DEVICES": "automatic placement",
            "CONCURRENT_SLOTS": 1,
            "WAIT_TIME_AFTER_RUN (seconds)": baseline.wait_time_after_run,
        }
    )
    log_settings("Runner settings", dict(values), defaults)
    logger.info(
        "Simulation plan: {} unique runs, {} duplicates discarded (before Resume filtering; GPU slots assigned at launch).",
        len(plan.runs),
        plan.duplicates_dropped,
    )
    for run in plan.runs:
        source = display_runner_path(run.source, settings.working_directory)
        if run.line_start:
            source += f":{run.line_start}"
            if run.line_end != run.line_start:
                source += f"-{run.line_end}"
        command = subprocess.list2cmdline(
            [*model_arguments(run, include_gpu=False), display_runner_path(run.tcf, settings.working_directory)]
        )
        logger.info("Run {:03d}: {} | source {}", run.index, command, source)


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


def _blocked_external_gpu_slots(groups: tuple[tuple[str, ...], ...], *, exclude_pids: set[int]) -> GpuAvailability:
    """Block confirmed conflicts and report uncertain inspection without stalling work.

    CodeMeter reports licence allocations, but not a reliable GPU ID mapping.
    Explicit -puN assignments are comparable only in a verified shared CUDA
    selection environment; differing or unreadable environments remain unknown.
    Our own children are excluded; their slots are already tracked separately.
    """
    from ...functions.tuflow.gpu_occupancy import gpu_ids_from_flags, local_gpu_occupancy

    all_slots = frozenset(range(len(groups)))
    try:
        snapshot = local_gpu_occupancy(exclude_pids=exclude_pids)
    except Exception as exc:  # noqa: BLE001 - optional inspection must not stop computation
        return GpuAvailability(
            issues=(
                GpuAvailabilityIssue(
                    kind="query_failed",
                    slot_indexes=all_slots,
                    detail=f"Local TUFLOW process query failed: {exc}",
                ),
            ),
        )
    issues: list[GpuAvailabilityIssue] = []
    if snapshot.inspection_incomplete or snapshot.unknown_pids:
        if snapshot.unknown_pids:
            issues.append(
                GpuAvailabilityIssue(
                    kind="unknown_assignment",
                    slot_indexes=all_slots,
                    pids=snapshot.unknown_pids,
                    detail="TUFLOW is running but its physical GPU assignment could not be determined.",
                )
            )
        if snapshot.inspection_incomplete:
            issues.append(
                GpuAvailabilityIssue(
                    kind="inspection_incomplete",
                    slot_indexes=all_slots,
                    detail="Local process inspection was incomplete; configured GPUs cannot be declared free.",
                )
            )

    blocked: set[int] = set()
    group_ids = tuple(gpu_ids_from_flags(group) for group in groups)
    for process in snapshot.assigned:
        process_slots: set[int] = set()
        overlapping_ids: set[int] = set()
        for index, selected in enumerate(group_ids):
            overlap = selected.intersection(process.gpu_ids)
            if overlap:
                blocked.add(index)
                process_slots.add(index)
                overlapping_ids.update(overlap)
        if process_slots:
            issues.append(
                GpuAvailabilityIssue(
                    kind="occupied",
                    slot_indexes=frozenset(process_slots),
                    gpu_ids=frozenset(overlapping_ids),
                    pids=(process.pid,),
                    detail="Confirmed from explicit switches and matching CUDA selection environments.",
                )
            )
    return GpuAvailability(blocked_slots=frozenset(blocked), issues=tuple(issues))


def _gpu_availability_lines(
    availability: GpuAvailability,
    groups: tuple[tuple[str, ...], ...],
    *,
    interval_seconds: float,
    device_names: Mapping[int, str] | None = None,
) -> tuple[str, ...]:
    """Build shared terminal/dashboard detail for one unavailable state."""
    lines: list[str] = []
    for issue in availability.issues:
        if issue.kind == "occupied":
            gpu_text = ", ".join(f"-pu{identifier}" for identifier in sorted(issue.gpu_ids))
            pid_text = ", ".join(str(pid) for pid in issue.pids)
            lines.append(f"TUFLOW {gpu_text}: OCCUPIED - external TUFLOW PID {pid_text}")
        elif issue.kind == "unknown_assignment":
            pid_text = ", ".join(str(pid) for pid in issue.pids)
            lines.append(f"GPU assignment UNKNOWN for external TUFLOW PID {pid_text}")
        elif issue.kind == "inspection_incomplete":
            lines.append("GPU status UNKNOWN - local process inspection was incomplete")
        else:
            lines.append(f"GPU status UNKNOWN - {issue.detail}")

    blocked_labels = [gpu_group_label(groups[index], device_names) for index in sorted(availability.blocked_slots)]
    available_labels = [
        gpu_group_label(group, device_names)
        for index, group in enumerate(groups)
        if index not in availability.blocked_slots
    ]
    if blocked_labels:
        lines.append(f"Excluded from scheduling: {', '.join(blocked_labels)}")
    if any(issue.kind != "occupied" for issue in availability.issues):
        lines.append("GPU inspection uncertain - continuing without verified availability; GPU sharing is possible")
    if available_labels:
        lines.append(f"Eligible GPU slot(s) may continue: {', '.join(available_labels)}")
    else:
        lines.append("ALL CONFIGURED GPU SLOTS BLOCKED - queued batch is waiting")
    lines.append(f"Next availability check in {interval_seconds:g} seconds")
    return tuple(lines)


def _log_gpu_availability_transition(
    availability: GpuAvailability,
    previous: GpuAvailability,
    groups: tuple[tuple[str, ...], ...],
    *,
    interval_seconds: float,
    device_names: Mapping[int, str] | None = None,
) -> None:
    """Emit prominent errors only when the external restriction changes."""
    if availability == previous:
        if availability.blocked_slots:
            logger.info(
                "GPU availability restriction is unchanged; {} of {} configured slot(s) remain blocked. "
                "Next check in {}s.",
                len(availability.blocked_slots),
                len(groups),
                interval_seconds,
            )
        return
    if previous.blocked_slots and not availability.issues:
        logger.success("Confirmed GPU restrictions cleared; queued scheduling may resume.")
    if not availability.issues:
        if previous.issues:
            logger.success("GPU inspection recovered; no external GPU restrictions detected.")
        return
    details = "\n".join(
        _gpu_availability_lines(availability, groups, interval_seconds=interval_seconds, device_names=device_names)
    )
    if availability.blocked_slots:
        logger.error("GPU AVAILABILITY ERROR\n{}", details)
    else:
        logger.warning("GPU INSPECTION WARNING - CONTINUING\n{}", details)


def _gpu_dashboard_alert(
    availability: GpuAvailability,
    groups: tuple[tuple[str, ...], ...],
    *,
    interval_seconds: float,
    device_names: Mapping[int, str] | None = None,
) -> WorkflowAlert | None:
    """Translate scheduling state into the dashboard's reusable alert model."""
    if not availability.issues:
        return None
    from ...functions.live_dashboard import WorkflowAlert

    blocked = bool(availability.blocked_slots)
    return WorkflowAlert(
        title="GPU AVAILABILITY ERROR" if blocked else "GPU INSPECTION WARNING - CONTINUING",
        border_style="bright_red" if blocked else "yellow",
        title_style="bold bright_red" if blocked else "bold yellow",
        body_style="bold red" if blocked else "yellow",
        lines=_gpu_availability_lines(
            availability, groups, interval_seconds=interval_seconds, device_names=device_names
        ),
    )


def _gpu_startup_message(groups: tuple[tuple[str, ...], ...]) -> None:
    """Exactly one external snapshot for an executing (nonempty) batch."""
    from ...functions.tuflow.gpu_occupancy import gpu_conflicts, gpu_ids_from_flags, local_gpu_occupancy

    try:
        snapshot = local_gpu_occupancy()
    except Exception as exc:  # noqa: BLE001 - external inspection cannot gate launches
        logger.warning("Startup GPU inspection unavailable: {}; assignments are unknown.", exc)
        return
    if not snapshot.assigned and not snapshot.unknown_pids and not snapshot.inspection_incomplete:
        logger.success("No other running local TUFLOW models detected.")
    assigned = {identifier for process in snapshot.assigned for identifier in process.gpu_ids}
    capacity = [tuple(sorted(gpu_ids_from_flags(group))) for group in groups]
    configured_ids: set[int] = {identifier for group in capacity for identifier in group}
    if groups and capacity and all(group and set(group).intersection(assigned) for group in capacity):
        details = ", ".join(
            f"PID {pid}: TUFLOW switches {', '.join(f'-pu{identifier}' for identifier in sorted(identifiers))}"
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


def display_runner_path(value: str | PurePath, base: Path) -> str:
    """Show project-relative paths without dereferencing links or changing I/O paths."""
    filename, separator, layer = str(value).partition(" >> ")
    path = Path(filename)
    try:
        shown = str(path.relative_to(base.absolute())) if path.is_absolute() else str(path)
    except ValueError:
        shown = str(path)
    return f"{shown}{separator}{layer}"


def _input_issue_message(issue: CommandPreflightIssue, *, advisory: bool = False) -> str:
    """Describe an attributed error or advisory without changing launch policy."""
    run, finding = issue.run, issue.finding
    base = Path(run.working_directory).absolute()

    def display(value: str | PurePath) -> str:
        return display_runner_path(value, base)

    flags = " ".join(f"-{key} {value}" for key, value in run.flag_pairs)
    selection = f" {flags}" if flags else ""
    owner = (
        display(finding.owner_control_file) if finding.owner_control_file is not None else "Not provided by validator"
    )
    if finding.line_number is not None:
        owner = f"{owner}:{finding.line_number}"
    detail = finding.detail
    if finding.csv_columns:
        columns = ", ".join(repr(column) for column in finding.csv_columns)
        detail = f"{detail.rstrip('. ')}. Required CSV columns: {columns}."
    return (
        f"{'Input advisory' if advisory else 'Input check'} run {run.index}: "
        f"{'WARNING' if advisory else finding.status.upper()} — {display(run.tcf)}{selection} "
        f"({finding.kind or finding.origin or 'input'})\n"
        f"  Source      : {display(run.source)}:{run.line_start}"
        f"{f'-{run.line_end}' if run.line_end != run.line_start else ''}\n"
        f"  Control file: {owner}\n"
        f"  Command     : {finding.command_text or 'Full command not provided by validator'}\n"
        f"  Reference   : {display(finding.reference)}\n"
        f"  Detail      : {detail}"
    )


class AdvisoryInputCheck:
    """At most one daemon validation of the frozen plan; never gates launching."""

    def __init__(self, plan: CommandPlan, *, check_gpkg_layers: bool = True) -> None:
        self.plan = plan
        self._layers = check_gpkg_layers
        self._messages: SimpleQueue[tuple[str, str]] = SimpleQueue()
        self._lock = Lock()
        self._cancel = Event()
        self.finished = Event()
        self.status = "Running"
        self._thread = Thread(target=self._work, name="tuflow-runner-preflight", daemon=True)

    def _work(self) -> None:
        try:
            from .command_preflight import check_command_plan

            # Keep upstream inspection inside this daemon. Thread-pool workers
            # are joined at interpreter exit and could hold shutdown on slow I/O.
            report = check_command_plan(self.plan, check_gpkg_layers=self._layers, io_workers=1)
            status = "Passed" if report.passed else f"Issues: {len(report.issues)}"
            messages = [
                (
                    "WARNING",
                    _input_issue_message(issue),
                )
                for issue in report.issues
            ]
            messages.extend(("WARNING", _input_issue_message(warning, advisory=True)) for warning in report.warnings)
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
            status = "Incomplete"
            messages = [("WARNING", f"Input checking unavailable or incomplete: {type(exc).__name__}: {exc}")]
        with self._lock:
            if not self._cancel.is_set():
                self.status = status
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
                self.status = "Incomplete"
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
        logger.warning("{}", _input_issue_message(issue))
    for warning in report.warnings:
        logger.warning("{}", _input_issue_message(warning, advisory=True))
    if not report.passed:
        logger.error("Input checking failed or is incomplete: {}/{} completed", report.completed_runs, report.run_count)
    else:
        logger.success("Input check passed for {}/{} runs.", report.completed_runs, report.run_count)
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


def _dashboard_metadata(plan: CommandPlan, settings: RunnerSettings) -> list[dict[str, object]]:
    """Group by lexical TCF identity, disambiguating repeated basenames."""
    name_paths: dict[str, set[str]] = {}
    for run in plan.runs:
        key = os.path.normcase(str(normalize_plan_path(run.tcf)))
        name_paths.setdefault(run.tcf.name.casefold(), set()).add(key)
    rows: list[dict[str, object]] = []
    for run in plan.runs:
        label = run.tcf.name
        if len(name_paths[label.casefold()]) > 1:
            label = display_runner_path(run.tcf, settings.working_directory)
        rows.append(
            {
                "run_index": run.index,
                "tcf_key": os.path.normcase(str(normalize_plan_path(run.tcf))),
                "tcf_label": label,
                "gpu": "Pending",
                "end_hours": "--",
                "model_hours": "--",
                "progress": "--",
                **run.flags,
            }
        )
    return rows


def _progress_metadata(snapshot: LogProgressSnapshot) -> dict[str, str]:
    """Display hours and elapsed model fraction, never infer solver success."""
    values = {"end_hours": "--", "model_hours": "--", "progress": "--"}
    for key, value in (("end_hours", snapshot.end_hours), ("model_hours", snapshot.model_hours)):
        if value is not None and math.isfinite(value):
            values[key] = f"{value:.3f}"
    start, end, model = snapshot.start_hours, snapshot.end_hours, snapshot.model_hours
    if (
        start is not None
        and end is not None
        and model is not None
        and all(math.isfinite(value) for value in (start, end, model))
        and end > start
    ):
        fraction = min(100.0, max(0.0, 100.0 * (model - start) / (end - start)))
        values["progress"] = f"{fraction:.1f}"
    return values


def _progress_observation_detail(update: ProgressUpdate, *, stale_after: float) -> str | None:
    snapshot = update.snapshot
    state_labels = {"waiting": "waiting log", "unverified": "log unverified", "unavailable": "progress unavailable"}
    if snapshot.state in state_labels:
        return state_labels[snapshot.state]
    if snapshot.last_log_update is not None and math.isfinite(snapshot.last_log_update):
        age = max(0.0, time.time() - snapshot.last_log_update)
        if age >= stale_after:
            return f"log {int(age // 60)}m old"
    return "log warning" if update.warning else None


def _progress_diagnostic_detail(snapshot: LogProgressSnapshot) -> str | None:
    diagnostics: list[str] = []
    timestep = snapshot.timestep_seconds
    if timestep is not None and type(timestep) in (int, float) and math.isfinite(timestep) and timestep > 0:
        diagnostics.append(f"dt {timestep:.4g}s")
    mass_error = snapshot.final_mass_error_percent
    if mass_error is not None and type(mass_error) in (int, float) and math.isfinite(mass_error):
        diagnostics.append(f"ME {mass_error:.4g}%")
    return " / ".join(diagnostics) if diagnostics else None


def _progress_detail(
    update: ProgressUpdate,
    *,
    stale_after: float,
    show_diagnostics: bool = True,
    outcome_detail: str | None = None,
) -> str:
    """Describe observed work without turning a log phase into process status."""
    snapshot = update.snapshot
    phase_labels = {"initialising": "Initialising", "solving": "Solving", "writing_results": "Writing results"}
    parts: list[str] = []
    if outcome_detail is not None:
        parts.append(outcome_detail)
    elif snapshot.phase is not None and snapshot.phase in phase_labels:
        parts.append(phase_labels[snapshot.phase])
    observation = _progress_observation_detail(update, stale_after=stale_after)
    if observation:
        parts.append(observation)
    diagnostics = _progress_diagnostic_detail(snapshot) if show_diagnostics else None
    if diagnostics:
        parts.append(diagnostics)
    if outcome_detail is None:
        parts.append(f"PID {update.pid}")
    return "; ".join(parts)


def launch_plan(  # noqa: C901 - single owned-process lifecycle and observation boundary
    plan: CommandPlan,
    settings: RunnerSettings,
    *,
    on_before_launch: BeforeLaunch | None = None,
    on_observed_completion: ObservedCompletion | None = None,
    interrupted: Event | None = None,
    popen: _ProcessFactory | None = None,
    advisory: AdvisoryInputCheck | None = None,
    proposed_plan: CommandPlan | None = None,
) -> BatchOutcome:
    """Launch each immutable occurrence once, with its own cwd and argv.

    The prelaunch hook is called before Popen; the completion hook is called
    immediately when poll observes exit, *before* ordered console output.
    The hooks provide scope 04's append-only START/FINISH integration point.
    proposed_plan supplies the complete pre-Resume selection for dashboard columns;
    only plan is launched. Omit it when plan already contains all proposed runs.
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
    gpu_availability = GpuAvailability()
    next_gpu_check = 0.0
    gpu_status: str | None = "Checking" if settings.wait_for_available_gpu and groups else None
    has_configured_gpu = any(is_gpu_switch(token) for run in plan.runs for token in run.arguments)
    has_source_priority = any(run.start_options for run in plan.runs)
    if has_configured_gpu:
        logger.warning("Python BATCH_COMMANDS -pu switches are exported but runner GPU groups control launches.")
    if has_source_priority:
        logger.info("Imported START priority/minimise/wait hints are replaced by runner scheduling settings.")
    if stop.is_set():
        return BatchOutcome(tuple(plan.runs), (), True)
    try:
        device_names = local_cuda_device_names()
    except Exception as exc:  # noqa: BLE001 - optional display metadata must not prevent launches
        logger.warning("GPU name inspection failed; continuing with device IDs: {}", exc)
        device_names = {}
    if device_names:
        logger.success(
            "Local CUDA device mapping for this launch environment: {}",
            "; ".join(gpu_device_label(identifier, device_names) for identifier in sorted(device_names)),
        )
    else:
        logger.warning("CUDA device names unavailable; continuing with -puN identifiers.")
    logger.info(
        "-puN uses TUFLOW/CUDA order, not NVIDIA or Windows display indexes. "
        "These names describe this launch environment; external processes may use different CUDA ordering. "
        "For older TUFLOW builds, verify device names in the model's .hpc.tlf."
    )
    if gpu_status is not None:
        logger.info(
            "Waiting for external GPU availability is enabled for {} configured group(s); recheck interval {}s.",
            len(groups),
            settings.gpu_check_interval_seconds,
        )
    else:
        if settings.wait_for_available_gpu and not groups:
            logger.warning(
                "WAIT_FOR_AVAILABLE_GPU cannot protect automatic GPU placement. "
                "Configure explicit -puN GPU_DEVICES to enable guarded scheduling."
            )
        _gpu_startup_message(groups)

    dashboard: LiveWorkflowDashboard | None = None
    progress_monitor: AdvisoryRunProgress | None = None
    latest_progress: dict[int, ProgressUpdate] = {}
    # Resume keeps the original logical indices. The renderer owns separate,
    # contiguous display slots and shows original indices through metadata.
    display_indices = {run.index: index for index, run in enumerate(plan.runs, 1)}

    def close_progress() -> None:
        nonlocal progress_monitor
        monitor = progress_monitor
        progress_monitor = None
        latest_progress.clear()
        if monitor is not None:
            try:
                monitor.close()
            except Exception as exc:  # noqa: BLE001 - optional cleanup must not affect owned solver processes
                logger.warning("Run progress monitoring cleanup failed; continuing simulations: {}", exc)

    def disable_progress(error: Exception, *, after_exit: bool = False) -> None:
        if after_exit:
            logger.warning(
                "Run progress monitoring unavailable after process exit; recorded outcome unchanged: {}", error
            )
        else:
            logger.warning("Run progress monitoring unavailable; continuing simulations: {}", error)
        close_progress()
        if dashboard is not None:
            try:
                for child in active:
                    dashboard.update_task(
                        index=display_indices[child.run.index],
                        metadata={"end_hours": "--", "model_hours": "--", "progress": "--"},
                        detail=f"PID {child.process.pid}; progress unavailable",
                        refresh=False,
                    )
            except Exception as exc:  # noqa: BLE001 - a failed optional display must not terminate owned children
                logger.warning("Could not clear unavailable progress from the dashboard: {}", exc)

    def finish_progress(result: RunOutcome) -> ProgressUpdate | None:
        progress_update = latest_progress.get(result.run.index)
        if progress_update is not None and (result.pid is None or progress_update.pid != result.pid):
            progress_update = None
        if progress_monitor is not None:
            try:
                if result.pid is None:
                    progress_monitor.discard(result.run.index)
                else:
                    pending = progress_monitor.finish(result.run.index, pid=result.pid)
                    if pending is not None and pending.run_index == result.run.index and pending.pid == result.pid:
                        progress_update = pending
                        if pending.warning:
                            logger.warning("Run {} log observation: {}", pending.run_index, pending.warning)
            except Exception as exc:  # noqa: BLE001 - observation cannot change an observed exit or continuation
                disable_progress(exc, after_exit=result.pid is not None)
        latest_progress.pop(result.run.index, None)
        return progress_update

    def observed(result: RunOutcome) -> None:
        nonlocal next_print
        outcomes.append(result)
        ordered[result.run.index] = result
        if on_observed_completion is not None:
            on_observed_completion(result)
        progress_update = finish_progress(result)
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
            outcome_detail = result.error or f"Exit {result.return_code}"
            completion_detail = outcome_detail
            completion_metadata: dict[str, str] | None = {"gpu": "Not launched"} if result.pid is None else None
            if progress_update is not None:
                try:
                    metadata = _progress_metadata(progress_update.snapshot)
                    detail = _progress_detail(
                        progress_update,
                        stale_after=max(60.0, settings.progress_poll_interval_seconds * 3),
                        show_diagnostics=settings.show_progress_diagnostics,
                        outcome_detail=outcome_detail,
                    )
                    completion_metadata = metadata
                    completion_detail = detail
                except Exception as exc:  # noqa: BLE001 - optional terminal observations cannot change the exit
                    disable_progress(exc, after_exit=True)
            dashboard.mark_finished(
                index=display_indices[result.run.index],
                status="OK" if result.successful else "FAIL",
                detail=completion_detail,
                metadata=completion_metadata,
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

        flag_keys = sorted({key for run in (proposed_plan or plan).runs + plan.runs for key, _ in run.flag_pairs})
        dashboard = LiveWorkflowDashboard(
            title="TUFLOW simulations",
            subtitle="Batch: "
            + (" ".join(flag for flag in shlex.split(settings.batch_commands) if not is_gpu_switch(flag)) or "(none)"),
            refresh_per_second=settings.live_refresh_per_second,
            max_rows=settings.live_max_rows,
            transient=False,
            group_by="tcf_key",
            group_label_key="tcf_label",
            columns=(
                WorkflowColumn("Run", "metadata", metadata_key="run_index", justify="right", no_wrap=True, min_width=3),
                *(
                    WorkflowColumn(key, "metadata", metadata_key=key, no_wrap=True, overflow="ellipsis")
                    for key in flag_keys
                ),
                WorkflowColumn("Status", "status", no_wrap=True, min_width=7),
                WorkflowColumn("GPU", "metadata", metadata_key="gpu", no_wrap=True, min_width=4),
                WorkflowColumn("End (h)", "metadata", metadata_key="end_hours", justify="right", no_wrap=True),
                WorkflowColumn("Model (h)", "metadata", metadata_key="model_hours", justify="right", no_wrap=True),
                WorkflowColumn("%", "metadata", metadata_key="progress", justify="right", no_wrap=True),
                WorkflowColumn("Elapsed", "duration", no_wrap=True, min_width=8),
                WorkflowColumn("Detail", "detail", no_wrap=True, overflow="ellipsis"),
            ),
        )
        dashboard.set_tasks(
            labels=[" ".join(f"{key} {value}" for key, value in run.flag_pairs) or "(no flags)" for run in plan.runs],
            metadata=_dashboard_metadata(plan, settings),
        )
    with dashboard if dashboard is not None else nullcontext():
        try:
            if dashboard is not None and settings.monitor_progress:
                try:
                    progress_monitor = AdvisoryRunProgress(interval_seconds=settings.progress_poll_interval_seconds)
                    progress_monitor.start()
                except Exception as exc:  # noqa: BLE001 - optional monitoring must never prevent solver execution
                    disable_progress(exc)
            while queue or active:
                if advisory is not None:
                    advisory.drain()
                if dashboard is not None:
                    if progress_monitor is not None:
                        try:
                            for update in progress_monitor.drain():
                                latest_progress[update.run_index] = update
                                if update.warning:
                                    logger.warning(
                                        "Run {} progress unavailable: {}; solver continues.",
                                        update.run_index,
                                        update.warning,
                                    )
                            for child in active:
                                update = latest_progress.get(child.run.index)
                                if update is not None and update.pid == child.process.pid:
                                    dashboard.update_task(
                                        index=display_indices[child.run.index],
                                        metadata=_progress_metadata(update.snapshot),
                                        detail=_progress_detail(
                                            update,
                                            stale_after=max(60.0, settings.progress_poll_interval_seconds * 3),
                                            show_diagnostics=settings.show_progress_diagnostics,
                                        ),
                                        refresh=False,
                                    )
                        except Exception as exc:  # noqa: BLE001 - corrupt/failed observations cannot cancel computation
                            disable_progress(exc)
                    metrics: dict[str, object] = {}
                    if advisory is not None:
                        metrics["Input checks"] = advisory.status
                    if gpu_status is not None:
                        metrics["GPU slots"] = gpu_status
                    if metrics:
                        dashboard.set_extra_metrics(metrics)
                    dashboard.refresh()
                if stop.is_set():
                    break
                if poll_children() and gpu_status is not None:
                    next_gpu_check = 0.0
                while queue and len(active) < capacity and not stop.is_set():
                    if gpu_status is not None and time.monotonic() >= next_gpu_check:
                        inspected_availability = _blocked_external_gpu_slots(
                            groups, exclude_pids={child.process.pid for child in active}
                        )
                        _log_gpu_availability_transition(
                            inspected_availability,
                            gpu_availability,
                            groups,
                            interval_seconds=settings.gpu_check_interval_seconds,
                            device_names=device_names,
                        )
                        gpu_availability = inspected_availability
                        if dashboard is not None:
                            dashboard.set_alert(
                                _gpu_dashboard_alert(
                                    gpu_availability,
                                    groups,
                                    interval_seconds=settings.gpu_check_interval_seconds,
                                    device_names=device_names,
                                ),
                                refresh=False,
                            )
                        gpu_status = f"{len(groups) - len(gpu_availability.blocked_slots)}/{len(groups)} eligible"
                        if any(issue.kind != "occupied" for issue in gpu_availability.issues):
                            gpu_status += " (inspection uncertain)"
                        next_gpu_check = time.monotonic() + settings.gpu_check_interval_seconds
                    slot = (
                        next(
                            (
                                i
                                for i in range(len(groups))
                                if i not in owned_slots and i not in gpu_availability.blocked_slots
                            ),
                            None,
                        )
                        if groups
                        else None
                    )
                    if groups and slot is None:
                        break
                    if stop.is_set():
                        break
                    run = queue.popleft()
                    group = groups[slot] if slot is not None else ()
                    args = _command(run, settings, group)
                    started = datetime.now()
                    if on_before_launch is not None:
                        on_before_launch(run)
                    progress_started_at = time.time()
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
                    if progress_monitor is not None:
                        try:
                            progress_monitor.register(run, pid=process.pid, started_at=progress_started_at)
                        except Exception as exc:  # noqa: BLE001 - retain child ownership after optional observer failure
                            disable_progress(exc)
                    try:
                        _apply_priority(process.pid, settings.computational_priority)
                    except Exception as exc:  # noqa: BLE001 - keep ownership after Popen succeeds
                        logger.warning("PID {} launched but process priority could not be set: {}", process.pid, exc)
                    logger.info(
                        "Launched run {} ({} selected) PID {} [{}]: {}",
                        run.index,
                        len(plan.runs),
                        process.pid,
                        gpu_group_label(group, device_names),
                        subprocess.list2cmdline(
                            [
                                display_runner_path(args[0], settings.working_directory),
                                *args[1:-1],
                                display_runner_path(args[-1], settings.working_directory),
                            ]
                        ),
                    )
                    if dashboard is not None:
                        dashboard.mark_running(
                            index=display_indices[run.index],
                            detail=f"PID {process.pid}",
                            metadata={"gpu": " ".join(group) or "Auto"},
                            refresh=False,
                        )
                        dashboard.set_active_count(len(active), refresh=False)
                        dashboard.refresh(force=True)
                    if queue and settings.wait_time_after_run:
                        time.sleep(settings.wait_time_after_run)
                    if poll_children() and gpu_status is not None:
                        next_gpu_check = 0.0
                if active:
                    time.sleep(0.1)
                elif queue and gpu_status is not None:
                    # No owned children: wait interruptibly without rescanning or waking at child-poll frequency.
                    remaining = max(0.0, next_gpu_check - time.monotonic())
                    dashboard_interval = 1.0 / settings.live_refresh_per_second if dashboard is not None else remaining
                    stop.wait(min(dashboard_interval, remaining))
            if stop.is_set():
                logger.warning("Batch interrupted. Terminating owned child processes.")
                _stop_children(active)
                poll_children()
            if advisory is not None:
                advisory.drain()
                if dashboard is not None:
                    dashboard.set_extra_metrics(
                        {"Input checks": advisory.status if advisory.finished.is_set() else "Incomplete"}
                    )
        finally:
            close_progress()
            if active:
                _stop_children(active)
                poll_children()
    return BatchOutcome(tuple(plan.runs), tuple(outcomes), stop.is_set())
