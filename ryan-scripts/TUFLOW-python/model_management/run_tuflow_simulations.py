r"""Run ordered TUFLOW simulations from editable parameters or BAT/text files.

Inputs: parameter products, one or more static .bat/.cmd/.txt command lists.
Outputs: optional editable BAT export, advisory input findings, process status.
Copy this wrapper into a model folder with the matching ryan-tools wheel
installed; no repository checkout or PYTHONPATH adjustment is required.

Examples:
    python run_tuflow_simulations.py --no-pause
    python run_tuflow_simulations.py --input-file runs.bat --mode textfiles --export-only --no-pause
    python run_tuflow_simulations.py --preflight-only --no-pause
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-31.1"

# ----------------------- EDITABLE RUNNER SETTINGS -------------------------
# Relative paths below are based on WORKING_DIR (the supplying script's folder
# by default), not on the current terminal directory or imported BAT locations.
WORKING_DIR: Path | None = None
TCF: Path | None = Path(r".\runs\PROJECT_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf")
TUFLOW_EXE = Path(r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe")
BATCH_COMMANDS = "-b"
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

MODE = "parameter_product"  # parameter_product, textfiles, both
INPUT_FILES: tuple[Path, ...] = ()  # e.g. (Path("runs.bat"), Path("additional.txt"))
# Python TCF is a hard constraint on every imported run. Set TCF=None to allow
# distinct TCFs in the same imported command list. Unused SET/REM are ignored.
# Imported model arguments and cwd survive; source START priority and -pu
# switches do NOT control scheduling. Runner GPU groups take precedence.
GPU_DEVICES: tuple[tuple[str, ...], ...] = (("-pu0",),)
MAXIMUM_CONCURRENT: int | None = None  # defaults to one child per GPU group
COMPUTATIONAL_PRIORITY = "LOW"
MINIMIZE_ON_LAUNCH = True
WAIT_TIME_AFTER_RUN = 2.0
RUN_SIMULATIONS = True  # False: render/export only; no solver/GPU dependency
EXPORT_COMMANDS = True
EXPORT_PATH: Path | None = None  # None: non-overwriting timestamped .bat beside wrapper
CHECK_INPUTS = False  # Advisory background check; never blocks launches
CHECK_GPKG_LAYERS = True
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 1.0
NEXT_RUN_FILE: Path | None = None  # Python script after normal batch termination, even failed children
PAUSE_ON_FINISH = True
# ------------------------------------------------------------------------

import argparse
import logging
import signal
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from threading import Event

from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.simulation_runner import (
    AdvisoryInputCheck,
    RunnerSettings,
    check_plan_only,
    export_batch_commands,
    launch_plan,
    prepare_plan,
)

logger = logging.getLogger(__name__)


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an immutable, ordered TUFLOW plan. CLI options override editable Python defaults.",
        epilog=r"Example: python run_tuflow_simulations.py --mode textfiles --no-tcf --input-file runs.bat --no-pause",
    )
    parser.add_argument("--working-directory", type=Path, help="Override WORKING_DIR (defaults to script folder).")
    parser.add_argument("--mode", choices=("parameter_product", "textfiles", "both"), help="Override MODE.")
    parser.add_argument("--input-file", type=Path, action="append", help="Supply BAT/text input; repeat to preserve order.")
    model = parser.add_mutually_exclusive_group()
    model.add_argument("--tcf", type=Path, help="Constrain imported runs to this TCF.")
    model.add_argument("--no-tcf", action="store_true", help="Allow multiple TCFs in imported commands.")
    parser.add_argument("--solver-exe", type=Path, help="Override the fallback TUFLOW executable.")
    parser.add_argument("--preflight", action=argparse.BooleanOptionalAction, default=None,
                        help="Run advisory background input checking while launching.")
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--preflight-only", action="store_true", help="Wait for strict checks; never launch.")
    operation.add_argument("--export-only", action="store_true", help="Export the plan without solver/GPU checks.")
    parser.add_argument("--export", action=argparse.BooleanOptionalAction, default=None,
                        help="Enable/disable BAT export; defaults to editable EXPORT_COMMANDS.")
    parser.add_argument("--export-path", type=Path, help="Non-overwriting output BAT path.")
    parser.add_argument("--live-dashboard", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--no-pause", action="store_true", help="Suppress interactive console pause.")
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
def _interruption_flag() -> Iterator[Event]:
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
        logger.error("Configured follow-up not found: %s", target)
        return False
    try:
        result = subprocess.run([sys.executable, str(target)], cwd=str(working_directory), check=False)
    except OSError:
        logger.exception("Could not launch configured Python follow-up %s", target)
        return False
    if result.returncode != 0:
        logger.error("Follow-up %s returned %d", target, result.returncode)
    return result.returncode == 0


def main(argv: list[str] | None = None) -> int:
    """Return 0 on success, 1 on planning/validation/processing failure."""
    arguments = _arguments(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        settings = _settings(arguments)
        if not settings.working_directory.is_dir():
            logger.error("Working directory does not exist: %s", settings.working_directory)
            return 1
        # All input sources and TCF conflicts must be checked before export,
        # validation, runtime GPU inspection, process launch or scope-04 writes.
        plan = prepare_plan(settings)
        logger.info("Selected %d ordered simulations (%d source files).",
                    len(plan.runs), len(plan.source_files))
        exporting = EXPORT_COMMANDS if arguments.export is None else arguments.export
        if arguments.export_only:
            exporting = True
        if exporting:
            name = (f"{Path(__file__).stem}_commands_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.bat")
            path = arguments.export_path or EXPORT_PATH or Path(__file__).absolute().parent / name
            destination = export_batch_commands(plan, settings, path)
            logger.info("Editable commands exported to %s", destination)
        if arguments.export_only or (not RUN_SIMULATIONS and not arguments.preflight_only):
            logger.info("Export-only: no solver processes, GPU scan or input checking.")
            return 0
        if arguments.preflight_only:
            return 0 if check_plan_only(plan, check_gpkg_layers=settings.check_gpkg_layers) else 1
        advisory = None
        checks = CHECK_INPUTS if arguments.preflight is None else arguments.preflight
        if checks:
            advisory = AdvisoryInputCheck(plan, check_gpkg_layers=settings.check_gpkg_layers)
            try:
                advisory.start()
            except RuntimeError as exc:
                logger.warning("Advisory input checking could not start: %s", exc)
                advisory = None
        try:
            with _interruption_flag() as stopped:
                result = launch_plan(plan, settings, interrupted=stopped, advisory=advisory)
        finally:
            if advisory is not None:
                advisory.close()
        if result.interrupted:
            logger.error("Interrupted batch; configured follow-up is not started.")
            return 1
        logger.info("Batch observed %d/%d completions; %d failures.",
                    len(result.outcomes), len(plan.runs),
                    sum(not item.successful for item in result.outcomes))
        outcome_code = result.exit_code
        # Preserve established chaining on normal batch termination, even when
        # one or more children failed; never hide their nonzero exit status.
        if NEXT_RUN_FILE is not None and not _run_follow_up(NEXT_RUN_FILE, settings.working_directory):
            outcome_code = 1
        return outcome_code
    except (OSError, ValueError) as exc:
        logger.exception("Runner preparation or execution failed: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.exception("Runner interrupted before batch completion.")
        return 1
    except Exception:
        logger.exception("Unexpected runner failure.")
        return 1


if __name__ == "__main__":
    parsed = _arguments()
    try:
        code = main(sys.argv[1:])
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
        if not parsed.no_pause and PAUSE_ON_FINISH:
            pause_console()
    raise SystemExit(code)
