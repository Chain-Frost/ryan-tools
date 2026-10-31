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

from pathlib import Path

WRAPPER_VERSION = "2026-10-31.4"

# ----------------------- EDITABLE RUNNER SETTINGS -------------------------
# Relative paths below are based on WORKING_DIR (the supplying script's folder
# by default), not on the current terminal directory or imported BAT locations.
# None uses this copied wrapper's folder; e.g. Path(r"P:\Project\Model").
WORKING_DIR: Path | None = None
# A Python TCF constrains every imported run and supplies missing bare-row TCFs.
# Set None for imported lists containing distinct TCFs; generated runs need a TCF.
TCF: Path | None = Path(r".\runs\PROJECT_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf")
# Sole executable for all launches; imported BAT/TXT executables are ignored.
TUFLOW_EXE = Path(r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe")
# Engine switches for EVERY run: -b suppresses completion prompts; -t tests inputs.
# For example, "-b -t -nlc" tests inputs without a licence check. Imported engine
# and GPU switches are replaced by this setting before export and Resume identity.
BATCH_COMMANDS = "-b"
# Optional generated-flag order. Unlisted flags retain RUN_VARIABLES_RAW order.
PRIORITY_ORDER: str | None = "e3 e2 e1"

# Generated parameter product; whitespace-delimited values. An explicit list
# may be used for individual values containing spaces.
RUN_VARIABLES_RAW: dict[str, str | list[str]] = {
    "e1": "01.0p 20.0p 05.0p 02.0p 10.0p 50.0p",
    "e2": "00720m 01080m 01440m 01800m 02160m 02880m 04320m",
    "e3": "TP01 TP02 TP03 TP04 TP05 TP06 TP07 TP08 TP09 TP10",
    "e4": "Ensembles",
    "s1": "EXG",
    "s2": "FullCatchment",
    "s4": "32M",
}

# parameter_product generates combinations; textfiles reads BAT/TXT model selections
# as inert data; both puts imported selections first. Duplicates are discarded.
# Imported START, CD and executable commands cannot control process launching.
MODE = "parameter_product"
# Sources are read in this order; e.g. (Path("runs.bat"), Path("additional.txt")).
# Do not use this wrapper's default export TXT as input while exporting to it.
INPUT_FILES: tuple[Path, ...] = ()
# One tuple per concurrent GPU slot. For two slots: (("-pu0",), ("-pu1",)).
# One multi-GPU slot: (("-pu0", "-pu1"),). Empty tuple uses automatic placement.
# These groups control launches. Imported GPU switches are ignored. A -puN in
# BATCH_COMMANDS is exported as written but these runtime groups take precedence.
GPU_DEVICES: tuple[tuple[str, ...], ...] = (("-pu0",),)
# None allows one child per GPU group, or one child with automatic placement.
MAXIMUM_CONCURRENT: int | None = None
# Windows process priority: LOW, BELOWNORMAL, NORMAL, ABOVENORMAL, HIGH, REALTIME.
COMPUTATIONAL_PRIORITY = "LOW"
# Open each solver console minimized; False opens it normally.
MINIMIZE_ON_LAUNCH = True
# Seconds between launch attempts. Zero starts available slots without a delay.
WAIT_TIME_AFTER_RUN = 2.0
# False selects export-only behavior; explicit --preflight-only still checks inputs.
RUN_SIMULATIONS = True
# resume skips latest observed successes; all resets the baseline and runs everything.
CONTINUATION_MODE = "resume"
# CSV: <this_wrapper_stem>_runs.csv beside this file, even with model-specific working directories.
# Stop the previous batch before Resume. Reexecuted runs use current model output locations.
# Refresh the complete current command plan even when Resume has no pending runs.
EXPORT_COMMANDS = True
# None uses this wrapper's basename with .txt beside it, e.g. my_batch.py -> my_batch.txt.
# An explicit .txt path is based on WORKING_DIR. Existing export content is replaced
# only after validation; imported sources, the wrapper and history are protected.
EXPORT_PATH: Path | None = None
# Background input checks are advisory and never block launches. --preflight-only waits.
CHECK_INPUTS = False
# Include GeoPackage layer checks when input checking is selected.
CHECK_GPKG_LAYERS = True
# Show the shared live task display; False keeps ordinary console messages.
USE_LIVE_DASHBOARD = True
# Maximum routine dashboard refreshes per second; must be greater than zero.
LIVE_REFRESH_PER_SECOND = 1.0
# INFO shows ordinary progress; SUCCESS keeps completions, warnings and errors.
# No extra log file is created; the execution CSV remains the persistent history.
CONSOLE_LOG_LEVEL = "INFO"
# Optional Python script after normal completion, including failed children and
# all-complete Resume. None disables chaining; e.g. Path("next_batch.py").
NEXT_RUN_FILE: Path | None = None
# Pause at the process boundary for interactive use; --no-pause overrides this.
PAUSE_ON_FINISH = True
# ------------------------------------------------------------------------

import argparse
import signal
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from threading import Event

from loguru import logger

from ryan_library.functions.loguru_helpers import configure_serial_logging
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


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an immutable, ordered TUFLOW plan. CLI options override editable Python defaults.",
        epilog=r"Example: python run_tuflow_simulations.py --mode textfiles --no-tcf --input-file runs.bat --no-pause",
    )
    add_execution_cli_arguments(parser)
    parser.add_argument("--mode", choices=("parameter_product", "textfiles", "both"), help="Override MODE.")
    parser.add_argument(
        "--input-file", type=Path, action="append", help="Supply BAT/text input; repeat to preserve order."
    )
    model = parser.add_mutually_exclusive_group()
    model.add_argument("--tcf", type=Path, help="Constrain imported runs to this TCF.")
    model.add_argument("--no-tcf", action="store_true", help="Allow multiple TCFs in imported commands.")
    parser.add_argument("--solver-exe", type=Path, help="Override the fallback TUFLOW executable.")
    parser.add_argument(
        "--run-mode",
        choices=("resume", "all"),
        help="Resume (default) skips latest observed successes; all resets the CSV completion baseline.",
    )
    parser.add_argument(
        "--preflight",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run advisory background input checking while launching.",
    )
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--preflight-only", action="store_true", help="Wait for strict checks; never launch.")
    operation.add_argument("--export-only", action="store_true", help="Export the plan without solver/GPU checks.")
    parser.add_argument(
        "--export",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable command TXT export; defaults to editable EXPORT_COMMANDS.",
    )
    parser.add_argument(
        "--export-path", type=Path, help="Replaceable .txt command output; defaults to this wrapper's basename."
    )
    parser.add_argument("--live-dashboard", action=argparse.BooleanOptionalAction, default=None)
    return parser.parse_args(argv)


def _settings(arguments: argparse.Namespace) -> RunnerSettings:
    base = arguments.working_directory if arguments.working_directory is not None else WORKING_DIR
    directory = (base if base is not None else Path(__file__).absolute().parent).absolute()
    tcf = None if arguments.no_tcf else (arguments.tcf if arguments.tcf is not None else TCF)
    return RunnerSettings(
        wrapper_file=Path(__file__).absolute(),
        working_directory=directory,
        tcf=tcf,
        executable=arguments.solver_exe if arguments.solver_exe is not None else TUFLOW_EXE,
        run_variables=RUN_VARIABLES_RAW,
        input_files=tuple(arguments.input_file) if arguments.input_file is not None else INPUT_FILES,
        mode=arguments.mode if arguments.mode is not None else MODE,  # type: ignore[arg-type]  # argparse choices
        batch_commands=BATCH_COMMANDS,
        priority_order=PRIORITY_ORDER,
        gpu_devices=GPU_DEVICES,
        maximum_concurrent=MAXIMUM_CONCURRENT,
        computational_priority=COMPUTATIONAL_PRIORITY,
        minimize_on_launch=MINIMIZE_ON_LAUNCH,
        wait_time_after_run=WAIT_TIME_AFTER_RUN,
        use_live_dashboard=USE_LIVE_DASHBOARD if arguments.live_dashboard is None else arguments.live_dashboard,
        live_refresh_per_second=LIVE_REFRESH_PER_SECOND,
        check_gpkg_layers=CHECK_GPKG_LAYERS,
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
        configure_serial_logging(console_log_level=arguments.console_log_level or CONSOLE_LOG_LEVEL)
        settings = _settings(arguments)
        if not settings.working_directory.is_dir():
            logger.error("Working directory does not exist: {}", settings.working_directory)
            return 1
        # All input sources and TCF conflicts must be checked before export,
        # validation, runtime GPU inspection, process launch or scope-04 writes.
        plan = prepare_plan(settings)
        requested_mode = arguments.run_mode if arguments.run_mode is not None else CONTINUATION_MODE
        if requested_mode not in {"resume", "all"}:
            msg = f"Unsupported continuation mode: {requested_mode}"
            raise ValueError(msg)
        logger.info(
            "Selected {} unique simulations ({} duplicates discarded; {} source files).",
            len(plan.runs),
            plan.duplicates_dropped,
            len(plan.source_files),
        )
        exporting = EXPORT_COMMANDS if arguments.export is None else arguments.export
        if arguments.export_only:
            exporting = True
        if exporting:
            path = arguments.export_path or EXPORT_PATH or Path(__file__).absolute().with_suffix(".txt")
            destination = export_batch_commands(plan, settings, path)
            logger.success("Editable commands exported to {}", destination)
        if arguments.export_only or (not RUN_SIMULATIONS and not arguments.preflight_only):
            logger.success("Export-only: no solver processes, GPU scan or input checking.")
            return 0
        if arguments.preflight_only:
            return 0 if check_plan_only(plan, check_gpkg_layers=settings.check_gpkg_layers) else 1
        # Run all must verify the solver before resetting completed runs.
        # A no-work Resume must not need an installed solver.
        if requested_mode == "all":
            validate_launch_executable(plan, settings)
        # The supplying wrapper, not the installed orchestrator or model cwd,
        # owns the single persistent history location.
        wrapper_path = Path(__file__).absolute()
        log_path = wrapper_path.with_name(f"{wrapper_path.stem}_runs.csv")
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
                checks = CHECK_INPUTS if arguments.preflight is None else arguments.preflight
                if checks:
                    advisory = AdvisoryInputCheck(pending, check_gpkg_layers=settings.check_gpkg_layers)
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
        if NEXT_RUN_FILE is not None and not _run_follow_up(NEXT_RUN_FILE, settings.working_directory):
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
        if not arguments.no_pause and PAUSE_ON_FINISH:
            pause_console()


if __name__ == "__main__":
    raise SystemExit(main())
