r"""Run ordered TUFLOW simulations from editable parameters or BAT/text files.

Inputs: parameter products, one or more static .bat/.cmd/.txt command lists.
Outputs: optional editable command TXT, advisory input findings, process status,
append-only CSV execution history beside this wrapper.
The TXT contains Windows batch syntax and is refreshed on each export. Its
extension prevents accidental double-click execution; it is never run here.
Copy this wrapper into a model folder with the matching ryan-tools wheel
installed; no repository checkout or PYTHONPATH adjustment is required.

Examples:
    python run_tuflow_simulations.py --no-pause
    python run_tuflow_simulations.py --input-file runs.bat --mode textfiles --export-only --no-pause
    python run_tuflow_simulations.py --preflight-only --no-pause
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal


class OP(StrEnum):
    """Operation: launch, export command text, or inspect input files."""

    RUN = "run"
    EXPORT_ONLY = "export_only"
    PREFLIGHT_ONLY = "preflight_only"


class SRC(StrEnum):
    """Simulation selection source."""

    PARAMETER_PRODUCT = "parameter_product"
    TEXTFILES = "textfiles"
    BOTH = "both"


class CONT(StrEnum):
    """CSV continuation strategy."""

    RESUME = "resume"
    ALL = "all"


class PRIO(StrEnum):
    """Windows child process scheduling priority."""

    LOW = "LOW"
    BELOWNORMAL = "BELOWNORMAL"
    NORMAL = "NORMAL"
    ABOVENORMAL = "ABOVENORMAL"
    HIGH = "HIGH"
    REALTIME = "REALTIME"


type OperationSetting = OP | Literal["run", "export_only", "preflight_only"]
type SourceSetting = SRC | Literal["parameter_product", "textfiles", "both"]
type ContinuationSetting = CONT | Literal["resume", "all"]
type PrioritySetting = PRIO | Literal["LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"]
type PathSetting = str | Path
type InputFilesSetting = PathSetting | list[PathSetting] | tuple[PathSetting, ...] | None
type GpuGroupSetting = str | list[str] | tuple[str, ...]
type GpuDevicesSetting = str | list[GpuGroupSetting] | tuple[GpuGroupSetting, ...] | None

WRAPPER_VERSION = "2026-11-01.3"

# ------------------------ USER-EDITABLE DEFAULTS --------------------------
# CLI options override these defaults. Relative paths use WORKING_DIR,
# unless stated otherwise. A copied run wrapper owns its adjacent CSV/TXT.

# Model directory: None uses the copied Python wrapper's directory, not cwd.
# Accepts str or Path. Example: r"Q:\Project\TUFLOW" or
# r"\\server\share\TUFLOW". Relative paths use the invoking cwd.
WORKING_DIR: PathSetting | None = None

# Generated TCF, also a consistency constraint/fallback for imported rows.
# None allows distinct TCFs in "textfiles" mode; generated runs require a TCF.
# Accepts str or Path; relative to WORKING_DIR.
# Example: r"runs\model_~e1~_~s1~.tcf".
TCF: PathSetting | None = r".\runs\PROJECT_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf"

# Solver path used for every launch. Imported BAT/TXT executables are ignored.
# Accepts str or Path; relative to WORKING_DIR.
# Example: r"C:\TUFLOW\TUFLOW_iSP_w64.exe".
TUFLOW_EXE: PathSetting = r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe"

# TUFLOW engine flags, e.g. "-b" or "-b -t -nlc". "-t" performs a
# test-only run. Imported engine switches do not control Python launches.
# Prefer GPU_DEVICES rather than supplying a -puN flag here.
BATCH_COMMANDS = "-b"

# Generated event/scenario values: whitespace-delimited text or explicit
# lists for values with spaces. Example: {"e1": ["Dry", "Wet Season"]}.
# A single value containing " " or "_" produces a warning, not an error.
# Spaces separating values in a raw string are not part of those values.
RUN_VARIABLES_RAW: dict[str, str | list[str]] = {
    "e1": "01.0p 20.0p 05.0p 02.0p 10.0p 50.0p",
    "e2": "00720m 01080m 01440m 01800m 02160m 02880m 04320m",
    "e3": "TP01 TP02 TP03 TP04 TP05 TP06 TP07 TP08 TP09 TP10",
    "e4": "Ensembles",
    "s1": "EXG",
    "s2": "FullCatchment",
    "s4": "32M",
}

# Generated flag order, e.g. "e3 e2 e1". None preserves dictionary order.
PRIORITY_ORDER: str | None = "e3 e2 e1"

# "parameter_product": generate combinations; "textfiles": import inert
# BAT/CMD/TXT selections; "both": imported first, then generated. Equivalent
# selections are deduplicated. Prefer SRC.TEXTFILES; raw "textfiles" works.
MODE: SourceSetting = SRC.PARAMETER_PRODUCT

# Ordered BAT/TXT sources for "textfiles"/"both"; relative to WORKING_DIR.
# One path string/Path, a list/tuple of paths, or None for no sources.
# Example: ["runs1.bat", "runs2.txt"] or "runs1.bat".
# Never import the same TXT that export will replace.
INPUT_FILES: InputFilesSetting = ()

# ONE concurrent simulation per GPU slot; strings must match "-puN".
# GPU_DEVICES = "-pu1"                        # 1 run: GPU 1
# GPU_DEVICES = ["-pu0", "-pu1"]              # 2 runs: GPU 0, GPU 1
# GPU_DEVICES = [["-pu0", "-pu1"], "-pu2"]    # 2 runs: GPUs 0+1, GPU 2
# Legacy nested tuples also work, e.g. (("-pu0",), ("-pu1",)).
# None, [] or () gives one automatic-placement slot; GPU IDs cannot repeat.
# Imported BAT/TXT GPU switches are ignored.
GPU_DEVICES: GpuDevicesSetting = "-pu0"

# Windows child priority: LOW, BELOWNORMAL, NORMAL, ABOVENORMAL, HIGH, REALTIME.
# Prefer PRIO.LOW; raw "LOW" also works.
COMPUTATIONAL_PRIORITY: PrioritySetting = PRIO.LOW

# True opens solver windows minimized; False shows them normally.
MINIMIZE_ON_LAUNCH = True

# Seconds between launches (>=0). Example: 0.5. Does not change GPU slots.
WAIT_TIME_AFTER_RUN = 2.0

# "export_only" creates editable TXT but never launches TUFLOW (safe default).
# "run" launches the licensed solver and may overwrite its output files.
# "preflight_only" waits for strict input checks and never launches TUFLOW.
# Prefer OP.RUN; raw "run" or CLI --run-simulations also works.
OPERATION: OperationSetting = OP.EXPORT_ONLY

# "resume": skip observed successes in this wrapper's adjacent CSV;
# "all": reset completion baseline and rerun everything. Use a different
# filename for each TCF version so its run wrapper gets a separate CSV.
# Prefer CONT.RESUME; raw "resume" also works.
CONTINUATION_MODE: ContinuationSetting = CONT.RESUME

# True refreshes <wrapper_name>.txt (editable batch TEXT, not executable BAT).
# Existing TXT is deliberately replaced, but protected inputs cannot be.
# False omits optional export during "run" or "preflight_only".
EXPORT_COMMANDS = True

# Enable advisory BACKGROUND model-input checks while TUFLOW runs.
# Warnings do not block launches; "preflight_only" is strict regardless.
CHECK_INPUTS = True

# True shows the Rich live dashboard; False uses ordinary console logging.
USE_LIVE_DASHBOARD = True

# Dashboard refresh rate, >0 refreshes/second; e.g. 0.5 for slow terminals.
LIVE_REFRESH_PER_SECOND = 1.0

# Console level, e.g. "DEBUG", "INFO", "SUCCESS", "WARNING".
CONSOLE_LOG_LEVEL: Literal["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"] = "INFO"

# Optional Python follow-up after a normally completed batch (even when
# children failed). None disables. Accepts str or Path, relative to WORKING_DIR.
# Example: "next_batch.py"; not launched after an interruption.
NEXT_RUN_FILE: PathSetting | None = None

# Pause interactively on finish; --no-pause forces False, --pause forces True.
PAUSE_ON_FINISH = True
# --------------------------------------------------------------------------

import argparse
import signal
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from threading import Event

from loguru import logger

from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.tuflow import runner_user_inputs
from ryan_library.functions.tuflow.command_plan import CommandPlan
from ryan_library.functions.tuflow.execution_history import (
    CompletionSession,
    CsvExecutionLog,
    identify_runs,
    select_pending,
)
from ryan_library.functions.wrapper_utils import add_execution_cli_arguments, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.simulation_runner import (
    AdvisoryInputCheck,
    BatchOutcome,
    RunnerSettings,
    check_plan_only,
    export_batch_commands,
    launch_plan,
    prepare_plan,
    validate_launch_executable,
)


def _settings(arguments: argparse.Namespace) -> RunnerSettings:
    """Normalize validated editable inputs to the library's strict types."""
    base = arguments.working_directory if arguments.working_directory is not None else WORKING_DIR
    directory = (
        runner_user_inputs.path("WORKING_DIR", base).absolute()
        if base is not None
        else Path(__file__).absolute().parent
    )
    configured_tcf = None if arguments.no_tcf else (arguments.tcf if arguments.tcf is not None else TCF)
    selected_inputs = tuple(arguments.input_file) if arguments.input_file is not None else INPUT_FILES
    selected_mode = runner_user_inputs.choice("MODE", arguments.mode if arguments.mode is not None else MODE, SRC)
    selected_priority = runner_user_inputs.choice("COMPUTATIONAL_PRIORITY", COMPUTATIONAL_PRIORITY, PRIO)
    selected_batch_commands = runner_user_inputs.batch_commands(BATCH_COMMANDS)
    selected_executable = runner_user_inputs.path(
        "TUFLOW_EXE", arguments.solver_exe if arguments.solver_exe is not None else TUFLOW_EXE
    )
    if selected_executable.suffix.casefold() != ".exe":
        message = f"TUFLOW_EXE must end in .exe, got {selected_executable!s}."
        raise ValueError(message)
    selected_tcf = runner_user_inputs.path("TCF", configured_tcf) if configured_tcf is not None else None
    if selected_tcf is not None and selected_tcf.suffix.casefold() != ".tcf":
        message = f"TCF must end in .tcf, got {selected_tcf!s}."
        raise ValueError(message)
    follow_up = runner_user_inputs.path("NEXT_RUN_FILE", NEXT_RUN_FILE) if NEXT_RUN_FILE is not None else None
    if follow_up is not None and follow_up.suffix.casefold() != ".py":
        message = f"NEXT_RUN_FILE must name a .py script, got {follow_up!s}."
        raise ValueError(message)
    runner_user_inputs.boolean("EXPORT_COMMANDS", EXPORT_COMMANDS)
    runner_user_inputs.boolean("CHECK_INPUTS", CHECK_INPUTS)
    runner_user_inputs.boolean("PAUSE_ON_FINISH", PAUSE_ON_FINISH)
    runner_user_inputs.boolean("MINIMIZE_ON_LAUNCH", MINIMIZE_ON_LAUNCH)
    runner_user_inputs.boolean("USE_LIVE_DASHBOARD", USE_LIVE_DASHBOARD)
    return RunnerSettings(
        wrapper_file=Path(__file__).absolute(),
        working_directory=directory,
        tcf=selected_tcf,
        executable=selected_executable,
        run_variables=runner_user_inputs.run_variables(RUN_VARIABLES_RAW),
        input_files=runner_user_inputs.input_paths(selected_inputs),
        mode=selected_mode.value,
        batch_commands=selected_batch_commands,
        priority_order=runner_user_inputs.priority_order(PRIORITY_ORDER),
        gpu_devices=runner_user_inputs.gpu_groups(GPU_DEVICES),
        computational_priority=selected_priority.value,
        minimize_on_launch=MINIMIZE_ON_LAUNCH,
        wait_time_after_run=runner_user_inputs.finite_number(
            "WAIT_TIME_AFTER_RUN",
            arguments.launch_delay if arguments.launch_delay is not None else WAIT_TIME_AFTER_RUN,
            positive=False,
        ),
        use_live_dashboard=(arguments.live_dashboard if arguments.live_dashboard is not None else USE_LIVE_DASHBOARD),
        live_refresh_per_second=runner_user_inputs.finite_number(
            "LIVE_REFRESH_PER_SECOND",
            arguments.live_refresh_per_second
            if arguments.live_refresh_per_second is not None
            else LIVE_REFRESH_PER_SECOND,
            positive=True,
        ),
    )


@contextmanager
def _interruption_flag() -> Generator[Event]:
    """Process-boundary signal handling; always restore the caller's handler."""
    flag = Event()
    previous = signal.getsignal(signal.SIGINT)

    def on_interrupt(_number: int, _frame: object) -> None:
        flag.set()

    signal.signal(signal.SIGINT, on_interrupt)
    try:
        yield flag
    finally:
        signal.signal(signal.SIGINT, previous)


def _warn_optional_file_paths(plan: CommandPlan, settings: RunnerSettings) -> None:
    """Warn about unavailable referenced files without blocking command export.

    Missing imported or configured TCFs remain a solver/preflight concern.
    The selected TUFLOW_EXE is independently required only when execution
    needs a solver. A configured follow-up script may be optional until the
    batch completes. Never change or dereference user-supplied path aliases.
    """

    def exists_as_file(candidate: Path) -> bool:
        try:
            return candidate.is_file()
        except OSError:
            return False

    tcfs = sorted({Path(str(run.tcf)) for run in plan.runs}, key=str)
    for tcf in tcfs:
        if not exists_as_file(tcf):
            logger.warning(
                "TCF not found or not readable: {}. Plan/export will continue; "
                "preflight or the solver may report missing model inputs.",
                tcf,
            )

    if settings.mode == "parameter_product":
        for source in settings.input_files:
            optional = source if source.is_absolute() else (settings.working_directory / source).absolute()
            if not exists_as_file(optional):
                logger.warning(
                    "INPUT_FILES source not found: {}. This is ignored because MODE is "
                    "'parameter_product'; switch MODE to SRC.TEXTFILES or SRC.BOTH to use it.",
                    optional,
                )

    executable = (
        settings.executable
        if settings.executable.is_absolute()
        else (settings.working_directory / settings.executable).absolute()
    )
    if not exists_as_file(executable):
        logger.warning(
            "TUFLOW_EXE not found or not readable: {}. This is allowed without "
            "a solver launch; any pending execution will require a valid executable.",
            executable,
        )

    if NEXT_RUN_FILE is not None:
        follow_up = runner_user_inputs.path("NEXT_RUN_FILE", NEXT_RUN_FILE)
        target = follow_up if follow_up.is_absolute() else (settings.working_directory / follow_up).absolute()
        if not exists_as_file(target):
            logger.warning(
                "NEXT_RUN_FILE not found or not readable: {}. The follow-up will "
                "fail if it is still missing after batch completion.",
                target,
            )


def _run_follow_up(path: Path, working_directory: Path) -> bool:
    target = path if path.is_absolute() else (working_directory / path).absolute()
    if not target.is_file():
        logger.error("Configured follow-up not found: {}", target)
        return False
    try:
        result = subprocess.run([sys.executable, str(target)], cwd=str(working_directory), check=False)
    except OSError:
        logger.exception("Could not launch configured Python follow-up {}", target)
        return False
    if result.returncode != 0:
        logger.error("Follow-up {} returned {}", target, result.returncode)
    return result.returncode == 0


def main(argv: list[str] | None = None) -> int:
    """Return 0 on success, 1 on planning/validation/processing failure."""
    arguments = _arguments(argv)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        log_level = arguments.console_log_level if arguments.console_log_level is not None else CONSOLE_LOG_LEVEL
        if not isinstance(log_level, str) or log_level.upper() not in {
            "TRACE",
            "DEBUG",
            "INFO",
            "SUCCESS",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }:
            message = (
                f"CONSOLE_LOG_LEVEL={log_level!r} is invalid; use DEBUG, INFO, SUCCESS, WARNING, ERROR or CRITICAL."
            )
            raise ValueError(message)
        configure_serial_logging(console_log_level=log_level.upper())
        settings = _settings(arguments)
        # Validate finite-choice inputs before export, history or simulation launch.
        if arguments.run_simulations:
            selected_operation = OP.RUN
        elif arguments.preflight_only:
            selected_operation = OP.PREFLIGHT_ONLY
        elif arguments.export_only:
            selected_operation = OP.EXPORT_ONLY
        else:
            selected_operation = runner_user_inputs.choice("OPERATION", OPERATION, OP)
        selected_continuation = runner_user_inputs.choice(
            "CONTINUATION_MODE",
            arguments.run_mode if arguments.run_mode is not None else CONTINUATION_MODE,
            CONT,
        )
        if not settings.working_directory.is_dir():
            logger.error("Working directory does not exist: {}", settings.working_directory)
            return 1
        # All input sources and TCF conflicts must be checked before export,
        # validation, runtime GPU inspection, process launch or scope-04 writes.
        plan = prepare_plan(settings)
        _warn_optional_file_paths(plan, settings)
        requested_mode = selected_continuation.value
        logger.info(
            "Selected {} unique simulations ({} duplicates discarded; {} source files).",
            len(plan.runs),
            plan.duplicates_dropped,
            len(plan.source_files),
        )
        operation = selected_operation.value
        exporting = runner_user_inputs.boolean(
            "EXPORT_COMMANDS", EXPORT_COMMANDS if arguments.export is None else arguments.export
        )
        if operation == "export_only":
            exporting = True
        if exporting:
            # Fixed TXT beside the copied wrapper, not an executable BAT file.
            destination = export_batch_commands(plan, settings, Path(__file__).absolute().with_suffix(".txt"))
            logger.success("Editable commands exported to {}", destination)
        if operation == "export_only":
            logger.success("Export-only: no solver processes, GPU scan or input checking.")
            return 0
        if operation == "preflight_only":
            return 0 if check_plan_only(plan) else 1
        # Run all must verify the solver before resetting completed runs.
        # A no-work Resume must not need an installed solver.
        if requested_mode == "all":
            validate_launch_executable(plan, settings)
        # The supplying wrapper, not the installed orchestrator or model cwd,
        # owns the single persistent history location.
        wrapper_path = Path(__file__).absolute()
        log_path = wrapper_path.with_name(f"{wrapper_path.stem}_runs.csv")
        # With no history, every run is pending. Check the solver before
        # creating a new CSV. An existing all-complete Resume still needs
        # no solver installation, and may inspect its history first.
        if requested_mode == "resume" and not log_path.exists():
            validate_launch_executable(plan, settings)
        identities = identify_runs(
            plan, fallback_executable=settings.executable, base_directory=settings.working_directory
        )
        with CsvExecutionLog(log_path) as history:
            if requested_mode == "all":
                history.reset()
                logger.info("Run all: new CSV completion baseline recorded in {}.", log_path)
            pending = select_pending(plan, identities, history.successful_keys())
            skipped = len(plan.runs) - len(pending.runs)
            if not pending.runs:
                logger.success(
                    "Resume complete: all {} runs have observed successful finishes; no GPU scan or launch.", skipped
                )
                result = BatchOutcome((), ())
            else:
                if requested_mode == "resume":
                    validate_launch_executable(pending, settings)
                logger.info(
                    "{}: {} pending, {} confirmed successful runs skipped. History: {}",
                    requested_mode.capitalize(),
                    len(pending.runs),
                    skipped,
                    log_path,
                )
                session = CompletionSession(history, identities)
                advisory = None
                checks = runner_user_inputs.boolean(
                    "CHECK_INPUTS", CHECK_INPUTS if arguments.preflight is None else arguments.preflight
                )
                if checks:
                    advisory = AdvisoryInputCheck(pending)
                    try:
                        advisory.start()
                    except RuntimeError as exc:
                        logger.warning("Advisory input checking could not start: {}", exc)
                        advisory = None
                try:
                    with _interruption_flag() as stopped:
                        result = launch_plan(
                            pending,
                            settings,
                            interrupted=stopped,
                            advisory=advisory,
                            on_before_launch=session.before_launch,
                            on_observed_completion=session.observed_completion,
                        )
                finally:
                    if advisory is not None:
                        advisory.close()
        if result.interrupted:
            logger.error("Interrupted batch; configured follow-up is not started.")
            return 1
        logger.log(
            "SUCCESS" if result.successful else "ERROR",
            "Batch observed {}/{} pending completions; {} previously successful skipped; {} failures.",
            len(result.outcomes),
            len(result.selected),
            skipped,
            sum(not item.successful for item in result.outcomes),
        )
        outcome_code = result.exit_code
        # Preserve established chaining on normal batch termination, even when
        # one or more children failed; never hide their nonzero exit status.
        if NEXT_RUN_FILE is not None and not _run_follow_up(
            runner_user_inputs.path("NEXT_RUN_FILE", NEXT_RUN_FILE), settings.working_directory
        ):
            outcome_code = 1
        return outcome_code
    except (OSError, ValueError):
        logger.exception("Runner preparation or execution failed.")
        return 1
    except KeyboardInterrupt:
        logger.exception("Runner interrupted before batch completion.")
        return 1
    except Exception:  # noqa: BLE001 - wrapper owns the process failure exit code
        logger.exception("Unexpected runner failure.")
        return 1
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
        if (arguments.pause or PAUSE_ON_FINISH is True) and not arguments.no_pause:
            pause_console()


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan, check or run TUFLOW simulations. CLI options override editable constants.",
        epilog=r"Example: python run_tuflow_simulations.py --mode textfiles --no-tcf --input-file runs.bat --run-simulations --no-pause",
    )
    add_execution_cli_arguments(parser)
    parser.add_argument("--mode", choices=("parameter_product", "textfiles", "both"), help="Override MODE.")
    parser.add_argument("--input-file", type=Path, action="append", help="Supply BAT/TXT; repeat for multiple files.")
    model = parser.add_mutually_exclusive_group()
    model.add_argument("--tcf", type=Path, help="Constrain imported runs to this TCF.")
    model.add_argument("--no-tcf", action="store_true", help="Allow multiple imported TCFs.")
    parser.add_argument("--solver-exe", type=Path, help="Override TUFLOW_EXE.")
    parser.add_argument("--run-mode", choices=("resume", "all"), help="Resume successful runs or start a new baseline.")
    parser.add_argument(
        "--preflight",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable advisory background input checking during launches.",
    )
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument(
        "--run-simulations", action="store_true", help="Launch solver processes (licence/output risk)."
    )
    operation.add_argument("--preflight-only", action="store_true", help="Check inputs strictly; never launch.")
    operation.add_argument("--export-only", action="store_true", help="Write TXT commands without launching.")
    parser.add_argument(
        "--export",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable command TXT export during run/preflight.",
    )
    parser.add_argument(
        "--live-dashboard",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable the live progress display.",
    )
    parser.add_argument("--live-refresh-per-second", type=float, help="Override dashboard refresh rate (>0).")
    parser.add_argument("--launch-delay", type=float, help="Seconds between launches (>=0).")
    parser.add_argument("--pause", action="store_true", help="Pause interactively at the end.")
    parsed = parser.parse_args(argv)
    if parsed.pause and parsed.no_pause:
        parser.error("--pause and --no-pause cannot be combined")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
