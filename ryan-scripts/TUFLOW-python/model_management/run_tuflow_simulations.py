r"""Run ordered TUFLOW simulations from editable parameters or input BAT/text files.
Supports resume of runs using the csv log file.
Copy this wrapper into a model folder with the matching ryan-tools wheel
installed; no repository checkout or PYTHONPATH adjustment is required.
Also supports CLI"""

from pathlib import Path

from ryan_library.functions.tuflow import runner_settings as params

# CLI options override these defaults.
# In MODE = params.SRC.TEXTFILES, TCF can be deleted entirely or set to None.
# The TCF then comes from each BAT/CMD/TXT command (or its SET TCF=... value).
# When Python TCF is supplied, imported commands must select that same TCF.
# MODE = params.SRC.PARAMETER_PRODUCT or params.SRC.BOTH requires Python TCF.
TCF: params.PathLike | None = r".\runs\PROJECT_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf"
TUFLOW_EXE: params.PathLike = r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe"
BATCH_COMMANDS = "-b"
# Generated combinations only: earlier flags vary slower; BAT/TXT run order is never sorted by this.
PRIORITY_ORDER: str | None = "e3 e2 e1"  # None preserves the generated dictionary order.
# In MODE = params.SRC.TEXTFILES, this entire RUN_VARIABLES_RAW block can be deleted.
# Imported -eN/-sN values are read directly from the source command files.
RUN_VARIABLES_RAW: dict[str, str | list[str]] = {
    "e1": "01.0p 20.0p 05.0p 02.0p 10.0p 50.0p",
    "e2": "00720m 01080m 01440m 01800m 02160m 02880m 04320m",
    "e3": "TP01 TP02 TP03 TP04 TP05 TP06 TP07 TP08 TP09 TP10",
    "e4": "Ensembles",
    "s1": "EXG",
    "s2": "FullCatchment",
    "s4": "32M",
}
# GPU_DEVICES = "-pu1"                      # 1 slot: GPU 1
# GPU_DEVICES = ["-pu0", "-pu1"]            # 2 slots: GPU 0, GPU 1
# GPU_DEVICES = [["-pu0", "-pu1"]]          # 1 slot using both GPUs
# None, [] or () gives one automatic-placement slot; GPU IDs cannot repeat.
GPU_DEVICES: params.GPUS = "-pu0"
# Exclude GPU groups used by other local TUFLOW processes; wait rather than launching on a busy GPU.
# Poll once per minute when blocked. Automatic GPU selection cannot be checked reliably.
WAIT_FOR_AVAILABLE_GPU = True
GPU_CHECK_INTERVAL_SECONDS = 60.0
INPUT_FILES: params.InputFiles = []  # BAT/TXT sources for "textfiles"/"both"; Example: ["runs1.bat", "runs2.txt"] or "runs1.bat".
# "parameter_product": generate combinations; "textfiles": import inert
# BAT/CMD/TXT selections; "both": imported first, then generated. Equivalent
# selections are deduplicated. Prefer params.SRC.TEXTFILES; raw "textfiles" works.
MODE: params.SourceMode = params.SRC.PARAMETER_PRODUCT
# "resume": skip observed successes in this wrapper's adjacent CSV;
# "all": reset completion baseline and rerun everything. Use a different
# filename for each TCF version so its run wrapper gets a separate CSV.
# Prefer params.CONT.RESUME; raw "resume" also works.
CONTINUATION_MODE: params.ContMode = params.CONT.RESUME
OPERATION: params.RunMode = params.OP.RUN
WORKING_DIR: params.PathLike | None = None  # Model directory: None uses the this python files directory, not cwd.
# Optional Python follow-up after a normally completed batch (even when
# children failed). None disables. Accepts str or Path, relative to WORKING_DIR.
# Example: "next_batch.py"; not launched after an interruption.
NEXT_RUN_FILE: params.PathLike | None = None
COMPUTATIONAL_PRIORITY: params.PrioritySetting = params.PRIO.LOW
MINIMIZE_ON_LAUNCH = True  # True opens solver windows minimized; False shows them normally.
EXPORT_COMMANDS = True  # True refreshes <wrapper_name>.txt, BAT format.
CHECK_INPUTS = True  # Enable BACKGROUND model-input checks while TUFLOW runs.
USE_LIVE_DASHBOARD = True  # True shows the Rich live dashboard; False uses ordinary console logging.
LIVE_REFRESH_PER_SECOND = 1.0
LIVE_MAX_ROWS = 10  # Maximum task rows shown at once; all runs still execute and are tracked.
MONITOR_PROGRESS = True  # Read model progress in the background when the live dashboard is enabled.
PROGRESS_POLL_INTERVAL_SECONDS = 10.0  # Separate from display refresh; increase for slow network shares.
SHOW_PROGRESS_DIAGNOSTICS = True  # Show the latest timestep and observed final mass error in Detail.
CONSOLE_LOG_LEVEL: params.LogLevel = params.LOG.INFO
WAIT_TIME_AFTER_RUN = 2.0  # Seconds between starts and after observed run exits (>=0; 0 disables cooldown).
PAUSE_ON_FINISH = True  # Pause interactively on finish; --no-pause forces False, --pause forces True.
WRAPPER_VERSION = "2026-11-09.2"

import argparse  # noqa: I001, RUF100
import signal
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from threading import Event

from loguru import logger

from ryan_library.functions.loguru_helpers import configure_serial_logging, forward_standard_logging, log_settings
from ryan_library.functions.tuflow import runner_user_inputs
from ryan_library.functions.wrapper_utils import add_execution_cli_arguments, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.simulation_runner import BatchOutcome, RunnerSettings, display_runner_path
from ryan_library.orchestrators.tuflow.simulation_workflow import (
    WorkflowControls,
    WorkflowOutcome,
    run_simulation_workflow,
)


def _settings(arguments: argparse.Namespace) -> RunnerSettings:
    """Normalize validated editable inputs to the library's strict types."""
    base = arguments.working_directory if arguments.working_directory is not None else WORKING_DIR
    directory = (
        runner_user_inputs.path("WORKING_DIR", base).absolute()
        if base is not None
        else Path(__file__).absolute().parent
    )
    selected_mode = runner_user_inputs.choice(
        "MODE", arguments.mode if arguments.mode is not None else MODE, params.SRC
    )
    configured_tcf = (
        None if arguments.no_tcf else (arguments.tcf if arguments.tcf is not None else globals().get("TCF"))
    )
    selected_inputs = tuple(arguments.input_file) if arguments.input_file is not None else INPUT_FILES
    if selected_mode is not params.SRC.TEXTFILES and "RUN_VARIABLES_RAW" not in globals():
        message = "RUN_VARIABLES_RAW must be defined for generated parameter_product/both runs."
        raise ValueError(message)
    raw_variables = globals().get("RUN_VARIABLES_RAW", {})
    selected_priority = runner_user_inputs.choice("COMPUTATIONAL_PRIORITY", COMPUTATIONAL_PRIORITY, params.PRIO)
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
    wait_for_gpu = runner_user_inputs.boolean(
        "WAIT_FOR_AVAILABLE_GPU",
        arguments.wait_for_available_gpu if arguments.wait_for_available_gpu is not None else WAIT_FOR_AVAILABLE_GPU,
    )
    return RunnerSettings(
        wrapper_file=Path(__file__).absolute(),
        working_directory=directory,
        tcf=selected_tcf,
        executable=selected_executable,
        run_variables=runner_user_inputs.run_variables(raw_variables),
        input_files=runner_user_inputs.input_paths(selected_inputs),
        mode=selected_mode.value,
        batch_commands=selected_batch_commands,
        priority_order=runner_user_inputs.priority_order(PRIORITY_ORDER),
        gpu_devices=runner_user_inputs.gpu_groups(GPU_DEVICES),
        wait_for_available_gpu=wait_for_gpu,
        gpu_check_interval_seconds=runner_user_inputs.finite_number(
            "GPU_CHECK_INTERVAL_SECONDS",
            arguments.gpu_check_interval if arguments.gpu_check_interval is not None else GPU_CHECK_INTERVAL_SECONDS,
            positive=True,
        ),
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
        live_max_rows=runner_user_inputs.positive_integer(
            "LIVE_MAX_ROWS", arguments.live_max_rows if arguments.live_max_rows is not None else LIVE_MAX_ROWS
        ),
        monitor_progress=runner_user_inputs.boolean(
            "MONITOR_PROGRESS",
            arguments.monitor_progress if arguments.monitor_progress is not None else MONITOR_PROGRESS,
        ),
        progress_poll_interval_seconds=runner_user_inputs.finite_number(
            "PROGRESS_POLL_INTERVAL_SECONDS",
            arguments.progress_poll_interval
            if arguments.progress_poll_interval is not None
            else PROGRESS_POLL_INTERVAL_SECONDS,
            positive=True,
        ),
        show_progress_diagnostics=runner_user_inputs.boolean(
            "SHOW_PROGRESS_DIAGNOSTICS",
            arguments.progress_diagnostics if arguments.progress_diagnostics is not None else SHOW_PROGRESS_DIAGNOSTICS,
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
        logger.error("Configured follow-up not found: {}", display_runner_path(target, working_directory))
        return False
    try:
        result = subprocess.run([sys.executable, str(target)], cwd=str(working_directory), check=False)
    except OSError:
        logger.exception(
            "Could not launch configured Python follow-up {}", display_runner_path(target, working_directory)
        )
        return False
    if result.returncode != 0:
        logger.error("Follow-up {} returned {}", display_runner_path(target, working_directory), result.returncode)
    return result.returncode == 0


@forward_standard_logging("pytuflow")
def main(argv: list[str] | None = None, *, _parsed_arguments: argparse.Namespace | None = None) -> int:
    """Return 0 on success, 1 on planning/validation/processing failure."""
    arguments = _parsed_arguments if _parsed_arguments is not None else _arguments(argv)
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        wrapper_source="TUFLOW-python/model_management/run_tuflow_simulations.py",
    )
    try:
        log_level = arguments.console_log_level if arguments.console_log_level is not None else CONSOLE_LOG_LEVEL
        if not isinstance(log_level, str) or log_level.upper() not in params.LOG:
            choices = ", ".join(params.LOG)
            message = f"CONSOLE_LOG_LEVEL={log_level!r} is invalid; use {choices}."
            raise ValueError(message)
        configure_serial_logging(
            console_log_level=log_level.upper(),
            # The default sink follows Rich's stdout and detects terminal colours.
            console_format="{time:HH:mm:ss} | <level>{level:<7}</level> | <level>{message}</level>",
        )
        run_settings = _settings(arguments)
        # Validate finite-choice inputs before export, history or simulation launch.
        if arguments.run_simulations:
            selected_operation = params.OP.RUN
        elif arguments.preflight_only:
            selected_operation = params.OP.PREFLIGHT_ONLY
        elif arguments.export_only:
            selected_operation = params.OP.EXPORT_ONLY
        else:
            selected_operation = runner_user_inputs.choice("OPERATION", OPERATION, params.OP)
        selected_continuation = runner_user_inputs.choice(
            "CONTINUATION_MODE",
            arguments.run_mode if arguments.run_mode is not None else CONTINUATION_MODE,
            params.CONT,
        )
        controls = WorkflowControls(
            operation=selected_operation,
            continuation=selected_continuation,
            export_commands=runner_user_inputs.boolean(
                "EXPORT_COMMANDS", EXPORT_COMMANDS if arguments.export is None else arguments.export
            ),
            check_inputs=runner_user_inputs.boolean(
                "CHECK_INPUTS", CHECK_INPUTS if arguments.preflight is None else arguments.preflight
            ),
        )
        follow_up = runner_user_inputs.path("NEXT_RUN_FILE", NEXT_RUN_FILE) if NEXT_RUN_FILE is not None else None
        log_settings(
            title="Run controls",
            values={
                "OPERATION": controls.operation.value,
                "CONTINUATION_MODE": controls.continuation.value,
                "EXPORT_COMMANDS": controls.exporting,
                "CHECK_INPUTS": controls.checking_inputs,
                "CONSOLE_LOG_LEVEL": log_level.upper(),
                "PAUSE_ON_FINISH": (arguments.pause or PAUSE_ON_FINISH is True) and not arguments.no_pause,
                "NEXT_RUN_FILE": NEXT_RUN_FILE,
            },
            defaults={
                "OPERATION": "run",
                "CONTINUATION_MODE": "resume",
                "EXPORT_COMMANDS": True,
                "CHECK_INPUTS": True,
                "CONSOLE_LOG_LEVEL": "INFO",
                "PAUSE_ON_FINISH": True,
                "NEXT_RUN_FILE": None,
            },
        )
        outcome: WorkflowOutcome = run_simulation_workflow(
            run_settings, controls, follow_up=follow_up, interruption=_interruption_flag
        )
        result: BatchOutcome | None = outcome.batch
        if result is None:
            return outcome.exit_code
        if result.interrupted:
            if follow_up is None:
                logger.error("Batch interrupted.")
            else:
                logger.error("Batch interrupted; configured follow-up was not started.")
            return 1
        logger.log(
            "SUCCESS" if result.successful else "ERROR",
            "Batch observed {}/{} pending completions; {} previously successful skipped; {} failures.",
            len(result.outcomes),
            len(result.selected),
            outcome.skipped,
            sum(not item.successful for item in result.outcomes),
        )
        outcome_code: int = outcome.exit_code
        # Preserve established chaining on normal batch termination, even when
        # one or more children failed; never hide their nonzero exit status.
        if follow_up is not None and not _run_follow_up(
            path=follow_up, working_directory=run_settings.working_directory
        ):
            outcome_code = 1
        return outcome_code
    except OSError, ValueError:
        logger.exception("Runner preparation or execution failed.")
        return 1
    except KeyboardInterrupt:
        logger.exception("Runner interrupted before batch completion.")
        return 1
    except Exception:  # noqa: BLE001 - wrapper owns the process failure exit code
        logger.exception("Unexpected runner failure.")
        return 1
    finally:
        print_wrapper_banner(
            wrapper_file=Path(__file__),
            wrapper_version=WRAPPER_VERSION,
            wrapper_source="TUFLOW-python/model_management/run_tuflow_simulations.py",
            leading_blank_line=True,
        )


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
    parser.add_argument("--live-max-rows", type=int, help="Maximum dashboard task rows shown at once (>=1).")
    parser.add_argument(
        "--monitor-progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Read TLF/HPC model times in the background while the live dashboard is enabled.",
    )
    parser.add_argument(
        "--progress-poll-interval", type=float, help="Seconds between progress log reads per active run (>0)."
    )
    parser.add_argument(
        "--progress-diagnostics",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show/hide the latest HPC timestep and observed final mass error in Detail; retain run phase.",
    )
    parser.add_argument(
        "--launch-delay", type=float, help="Seconds between starts and after run exits (>=0; 0 disables)."
    )
    parser.add_argument(
        "--wait-for-available-gpu",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Wait for explicitly configured GPUs used by other local TUFLOW processes.",
    )
    parser.add_argument(
        "--gpu-check-interval", type=float, help="Seconds between external GPU checks while blocked (>0)."
    )
    parser.add_argument("--pause", action="store_true", help="Pause interactively at the end.")
    parsed = parser.parse_args(argv)
    if parsed.pause and parsed.no_pause:
        parser.error("--pause and --no-pause cannot be combined")
    return parsed


def _cli_main() -> int:
    """Release wrapper resources before waiting for the final console keypress.

    Parse once and pass the same arguments through to main(). Its decorated
    logging context, execution-history lock and dashboard have all closed
    before the prompt. Close Loguru's sinks as well, so they cannot defer
    file flushing until Python interpreter shutdown.
    """
    arguments = _arguments()
    try:
        return main(_parsed_arguments=arguments)
    finally:
        logger.remove()
        if (arguments.pause or PAUSE_ON_FINISH is True) and not arguments.no_pause:
            pause_console()


if __name__ == "__main__":
    raise SystemExit(_cli_main())
