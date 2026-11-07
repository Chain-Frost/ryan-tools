"""Coordinate a complete TUFLOW export, preflight or resumable run session.

The caller supplies normalized settings and the actual copied wrapper's path.
This module owns planning, command export, CSV history and advisory checking;
the existing simulation runner owns scheduling, progress and dashboard rendering.
Logging configuration, signal handlers, follow-up scripts and console interaction
remain with the caller. All session resources close before an outcome is returned.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from threading import Event

from loguru import logger

from ...functions.tuflow.command_plan import CommandPlan
from ...functions.tuflow.execution_history import CompletionSession, CsvExecutionLog, identify_runs, select_pending
from ...functions.tuflow.runner_settings import CONT, OP
from .simulation_runner import (
    AdvisoryInputCheck,
    BatchOutcome,
    RunnerSettings,
    check_plan_only,
    display_runner_path,
    export_batch_commands,
    launch_plan,
    prepare_plan,
    validate_launch_executable,
)


@dataclass(frozen=True, slots=True)
class WorkflowControls:
    """Validated operation choices, independent of CLI and editable defaults."""

    operation: OP = OP.RUN
    continuation: CONT = CONT.RESUME
    export_commands: bool = True
    check_inputs: bool = True

    def __post_init__(self) -> None:
        if type(self.operation) is not OP or type(self.continuation) is not CONT:
            message = "Workflow operation and continuation must use OP and CONT enum values."
            raise ValueError(message)
        if type(self.export_commands) is not bool or type(self.check_inputs) is not bool:
            message = "Workflow export_commands and check_inputs must be booleans."
            raise ValueError(message)

    @property
    def exporting(self) -> bool:
        """Export-only always refreshes the adjacent command TXT."""
        return self.operation is OP.EXPORT_ONLY or self.export_commands

    @property
    def checking_inputs(self) -> bool:
        """Strict preflight forces checks; export-only never starts them."""
        if self.operation is OP.PREFLIGHT_ONLY:
            return True
        return self.operation is OP.RUN and self.check_inputs


@dataclass(frozen=True, slots=True)
class WorkflowOutcome:
    """Completed session data after history release and advisory cancellation."""

    operation: OP
    export_path: Path | None = None
    preflight_passed: bool | None = None
    batch: BatchOutcome | None = None
    skipped: int = 0

    @property
    def exit_code(self) -> int:
        if self.operation is OP.RUN:
            return self.batch.exit_code if self.batch is not None else 1
        if self.operation is OP.PREFLIGHT_ONLY:
            return 0 if self.preflight_passed is True else 1
        return 0 if self.operation is OP.EXPORT_ONLY and self.export_path is not None else 1


def _exists_as_file(candidate: Path) -> bool:
    try:
        return candidate.is_file()
    except OSError:
        return False


def _warn_optional_file_paths(plan: CommandPlan, settings: RunnerSettings, follow_up: Path | None) -> None:
    """Report unavailable optional files without blocking command export.

    Missing TCFs remain a solver/preflight concern. Pending execution separately
    requires its solver. Keep user-supplied path aliases without dereferencing.
    """

    tcfs = sorted({Path(str(run.tcf)) for run in plan.runs}, key=str)
    for tcf in tcfs:
        if not _exists_as_file(tcf):
            logger.warning(
                "TCF not found or not readable: {}. Plan/export will continue; "
                "preflight or the solver may report missing model inputs.",
                display_runner_path(tcf, settings.working_directory),
            )

    if settings.mode == "parameter_product":
        for source in settings.input_files:
            optional = source if source.is_absolute() else (settings.working_directory / source).absolute()
            if not _exists_as_file(optional):
                logger.warning(
                    "INPUT_FILES source not found: {}. This is ignored because MODE is "
                    "'parameter_product'; switch MODE to params.SRC.TEXTFILES or params.SRC.BOTH to use it.",
                    display_runner_path(optional, settings.working_directory),
                )

    executable = (
        settings.executable
        if settings.executable.is_absolute()
        else (settings.working_directory / settings.executable).absolute()
    )
    if not _exists_as_file(executable):
        logger.warning(
            "TUFLOW_EXE not found or not readable: {}. This is allowed without "
            "a solver launch; any pending execution will require a valid executable.",
            display_runner_path(executable, settings.working_directory),
        )

    if follow_up is not None:
        target = follow_up if follow_up.is_absolute() else (settings.working_directory / follow_up).absolute()
        if not _exists_as_file(target):
            logger.warning(
                "NEXT_RUN_FILE not found or not readable: {}. The follow-up will "
                "fail if it is still missing after batch completion.",
                display_runner_path(target, settings.working_directory),
            )


def run_simulation_workflow(
    settings: RunnerSettings,
    controls: WorkflowControls,
    *,
    follow_up: Path | None = None,
    interruption: Callable[[], AbstractContextManager[Event]] | None = None,
) -> WorkflowOutcome:
    """Plan and complete one operation, preserving the full plan for Resume.

    TXT and CSV paths derive only from ``settings.wrapper_file``, never this
    module's location or the model working directory. ``follow_up`` is used only
    for optional path diagnostics; executing it remains the caller's decision.

    ``interruption`` supplies a launch-only context yielding a stop event. The
    wrapper can install and restore its SIGINT handler there without suppressing
    KeyboardInterrupt during planning, export or strict preflight. Library callers
    may omit it or supply their own event context; no signal handlers are installed
    here. Preparation and execution exceptions propagate after resource cleanup.
    """
    if not settings.working_directory.is_dir():
        message = f"Working directory does not exist: {settings.working_directory}"
        raise ValueError(message)
    # Finish every source and TCF conflict check before writing, checking inputs,
    # inspecting GPUs or launching any solver.
    plan = prepare_plan(settings)
    _warn_optional_file_paths(plan, settings, follow_up)
    logger.info(
        "Selected {} unique simulations ({} duplicates discarded; {} source files).",
        len(plan.runs),
        plan.duplicates_dropped,
        len(plan.source_files),
    )
    wrapper_path = settings.wrapper_file.absolute()
    destination = None
    if controls.exporting:
        destination = export_batch_commands(plan, settings, wrapper_path.with_suffix(".txt"))
        logger.success("Editable commands exported to {}", display_runner_path(destination, settings.working_directory))
    if controls.operation is OP.EXPORT_ONLY:
        logger.success("Export-only: no solver processes, GPU scan or input checking.")
        return WorkflowOutcome(controls.operation, export_path=destination)
    if controls.operation is OP.PREFLIGHT_ONLY:
        return WorkflowOutcome(controls.operation, export_path=destination, preflight_passed=check_plan_only(plan))
    result, skipped = _run_with_history(plan, settings, controls, interruption=interruption)
    return WorkflowOutcome(controls.operation, export_path=destination, batch=result, skipped=skipped)


def _run_with_history(
    plan: CommandPlan,
    settings: RunnerSettings,
    controls: WorkflowControls,
    *,
    interruption: Callable[[], AbstractContextManager[Event]] | None,
) -> tuple[BatchOutcome, int]:
    """Keep completion history open through launch and advisory cleanup."""

    # Run all must verify the solver before resetting completed runs. A no-work
    # Resume may inspect existing history without an installed solver.
    if controls.continuation is CONT.ALL:
        validate_launch_executable(plan, settings)
    wrapper_path = settings.wrapper_file.absolute()
    log_path = wrapper_path.with_name(f"{wrapper_path.stem}_runs.csv")
    if controls.continuation is CONT.RESUME and not log_path.exists():
        validate_launch_executable(plan, settings)
    identities = identify_runs(plan, fallback_executable=settings.executable, base_directory=settings.working_directory)
    with CsvExecutionLog(log_path) as history:
        if controls.continuation is CONT.ALL:
            history.reset()
            logger.info(
                "Run all: new CSV completion baseline recorded in {}.",
                display_runner_path(log_path, settings.working_directory),
            )
        pending = select_pending(plan, identities, history.successful_keys())
        skipped = len(plan.runs) - len(pending.runs)
        if not pending.runs:
            logger.success(
                "Resume complete: all {} runs have observed successful finishes; no GPU scan or launch.", skipped
            )
            result = BatchOutcome((), ())
        else:
            if controls.continuation is CONT.RESUME:
                validate_launch_executable(pending, settings)
            logger.info(
                "{}: {} pending, {} confirmed successful runs skipped. History: {}",
                controls.continuation.value.capitalize(),
                len(pending.runs),
                skipped,
                display_runner_path(log_path, settings.working_directory),
            )
            session = CompletionSession(history, identities)
            advisory = AdvisoryInputCheck(pending) if controls.checking_inputs else None
            try:
                if advisory is not None:
                    try:
                        advisory.start()
                    except RuntimeError as exc:
                        logger.warning("Advisory input checking could not start: {}", exc)
                        advisory = None
                with interruption() if interruption is not None else nullcontext(Event()) as stopped:
                    result = launch_plan(
                        pending,
                        settings,
                        proposed_plan=plan,
                        interrupted=stopped,
                        advisory=advisory,
                        on_before_launch=session.before_launch,
                        on_observed_completion=session.observed_completion,
                    )
            finally:
                if advisory is not None:
                    advisory.close()
    return result, skipped
