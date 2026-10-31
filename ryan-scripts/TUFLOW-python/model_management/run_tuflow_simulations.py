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
from typing import Literal

WRAPPER_VERSION = "2026-11-01.2"

# ------------------------ USER-EDITABLE DEFAULTS --------------------------
# CLI options override these defaults. Relative paths use WORKING_DIR,
# unless stated otherwise. A copied run wrapper owns its adjacent CSV/TXT.

# Model directory: None uses the copied Python wrapper's directory, not cwd.
# Example: Path(r"Q:\Project\TUFLOW") or Path(r"\\server\share\TUFLOW").
WORKING_DIR: Path | None = None

# Generated TCF, also a consistency constraint/fallback for imported rows.
# None allows distinct TCFs in "textfiles" mode; generated runs require a TCF.
# Relative to WORKING_DIR; e.g. Path(r"runs\model_~e1~_~s1~.tcf").
TCF: Path | None = Path(r".\runs\PROJECT_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf")

# Solver path used for every launch. Imported BAT/TXT executables are ignored.
# Relative to WORKING_DIR. Example: Path(r"C:\TUFLOW\TUFLOW_iSP_w64.exe").
TUFLOW_EXE = Path(r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe")

# TUFLOW engine flags, e.g. "-b" or "-b -t -nlc". "-t" performs a
# test-only run. Imported engine switches do not control Python launches.
# Prefer GPU_DEVICES rather than supplying a -puN flag here.
BATCH_COMMANDS = "-b"

# Generated event/scenario values: whitespace-delimited text or explicit
# lists for values with spaces. Example: {"e1": ["Dry", "Wet Season"]}.
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
# selections are deduplicated. Example: "textfiles".
MODE: Literal["parameter_product", "textfiles", "both"] = "parameter_product"

# Ordered BAT/TXT sources, used for "textfiles"/"both" only. Relative to
# WORKING_DIR. Example: (Path("runs1.bat"), Path("runs2.txt")).
# Never import the same TXT that export will replace.
INPUT_FILES: tuple[Path, ...] = ()

# ONE concurrent simulation per GPU group. Three copy-ready examples:
# GPU_DEVICES = (("-pu0", "-pu1"), ("-pu2",))  # 2 runs: GPU 0+1, GPU 2
# GPU_DEVICES = (("-pu0",), ("-pu1",))         # 2 runs: GPU 0, GPU 1
# GPU_DEVICES = (("-pu0",),)                  # 1 run: GPU 0
# () gives one automatic-placement slot; imported GPU flags are ignored.
GPU_DEVICES: tuple[tuple[str, ...], ...] = (("-pu0",),)

# Windows child priority: LOW, BELOWNORMAL, NORMAL, ABOVENORMAL, HIGH, REALTIME.
COMPUTATIONAL_PRIORITY = "LOW"

# True opens solver windows minimized; False shows them normally.
MINIMIZE_ON_LAUNCH = True

# Seconds between launches (>=0). Example: 0.5. Does not change GPU slots.
WAIT_TIME_AFTER_RUN = 2.0

# "export_only" creates editable TXT but never launches TUFLOW (safe default).
# "run" launches the licensed solver and may overwrite its output files.
# "preflight_only" waits for strict input checks and never launches TUFLOW.
# Example: "run" or use CLI --run-simulations.
OPERATION: Literal["run", "export_only", "preflight_only"] = "export_only"

# "resume": skip observed successes in this wrapper's adjacent CSV;
# "all": reset completion baseline and rerun everything. Use a different
# filename for each TCF version so its run wrapper gets a separate CSV.
CONTINUATION_MODE: Literal["resume", "all"] = "resume"

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
CONSOLE_LOG_LEVEL = "INFO"

# Optional Python follow-up after a normally completed batch (even when
# children failed). None disables. Relative to WORKING_DIR.
# Example: Path("next_batch.py"); not launched after an interruption.
NEXT_RUN_FILE: Path | None = None

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
        computational_priority=COMPUTATIONAL_PRIORITY,
        minimize_on_launch=MINIMIZE_ON_LAUNCH,
        wait_time_after_run=(arguments.launch_delay if arguments.launch_delay is not None else WAIT_TIME_AFTER_RUN),
        use_live_dashboard=(arguments.live_dashboard if arguments.live_dashboard is not None else USE_LIVE_DASHBOARD),
        live_refresh_per_second=(
            arguments.live_refresh_per_second
            if arguments.live_refresh_per_second is not None
            else LIVE_REFRESH_PER_SECOND
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
        configure_serial_logging(console_log_level=log_level)
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
        operation = OPERATION
        if arguments.run_simulations:
            operation = "run"
        elif arguments.preflight_only:
            operation = "preflight_only"
        elif arguments.export_only:
            operation = "export_only"
        if operation not in {"run", "export_only", "preflight_only"}:
            msg = f"Unsupported operation: {operation}"
            raise ValueError(msg)
        exporting = EXPORT_COMMANDS if arguments.export is None else arguments.export
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
        if (arguments.pause or PAUSE_ON_FINISH) and not arguments.no_pause:
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
