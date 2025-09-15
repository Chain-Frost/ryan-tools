from __future__ import annotations
from pathlib import Path

# 2025-06-05 version


def get_parameters() -> Parameters:

    run_variables_raw: dict[str, str] = {
        "e1": "01.00p pmp",
        "e2": "01440m",
        "s1": "EXG supers6",
        "s2": "bigModel",
        "s4": "16M",
    }

    core_params = CoreParameters(
        tcf=Path(r".\runs\tuflow_01_~s1~_~e1~_~e2~_~s4~.tcf"),
        tuflowexe=Path(r"C:\TUFLOW\2025.0.3\TUFLOW_iSP_w64.exe"),
        batch_commands="-b",
        # Windows START priority: "LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"
        computational_priority="LOW",
        # Optional: a Python script to run after all sims finish
        next_run_file=None,
        # If you want a custom ordering of keys, e.g. "s1 s2 e1 e2", otherwise None
        priority_order=None,
        # If False → only export commands.txt; if True → also run simulations
        run_simulations=True,
        # Each element is a "GPU group."  One simulation uses exactly one group.
        # Example: first sim uses GPU0&1, second sim uses GPU2, etc.
        # [["-pu0", "-pu1"], ["-pu2"]]
        # gpu_devices="pu0",
        # Seconds to wait after launching each simulation
        wait_time_after_run=2.0,
        # If True → script waits for any keypress before exiting (Windows only)
        pause_on_finish=True,
    )

    # Build and validate
    return build_parameters(core_params=core_params, run_variables_raw=run_variables_raw)


def build_parameters(core_params: CoreParameters, run_variables_raw: dict[str, str]) -> Parameters:
    """Split the raw run-variable strings, assemble a Parameters object,
    and validate required fields. This keeps all “internal” logic
    away from the user.
    Args:
        core_params:       A CoreParameters instance (paths, flags, GPUs, etc.).
        run_variables_raw: A dict mapping two-char flags (e.g. "e1") → raw str (e.g. "01.00p pmp").
    Returns:
        Parameters: Fully constructed and validated."""
    # 1) Convert each raw string into list[str] by splitting on whitespace:
    run_variables: dict[str, list[str]] = {
        key: split_input_strings(input_val=value) for key, value in run_variables_raw.items()
    }

    # 2) Build the Parameters dataclass from core_params + run_variables:
    params = Parameters(core_params=core_params, run_variables=run_variables)

    # 3) Validate required fields (raises ValueError if something is missing):
    check_and_set_defaults(params=params)

    return params


def split_input_strings(input_val: str | list[str]) -> list[str]:
    """If input_val is a single string, split it on whitespace.
    If it's already a list[str], return it unchanged.

    Args:
        input_val (str | list[str]): e.g. "01.00p pmp" or ["A", "B"]

    Returns:
        list[str]: e.g. ["01.00p", "pmp"] or ["A", "B"]"""
    if isinstance(input_val, str):
        return input_val.strip().split()
    else:
        return input_val


# imports placed here so that they do not obstruct user editing of the parameters at the top.
import datetime
import itertools
import logging
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
import colorama
import psutil


# ------------------------------------------------------------------------------
# DATA CLASSES
# ------------------------------------------------------------------------------


@dataclass
class CoreParameters:
    """Core parameters for a TUFLOW simulation.

    Attributes:
        tcf:                    Path to the TCF.
        tuflowexe:              Path to the TUFLOW executable (.exe).
        batch_commands:         Windows batch flags (e.g. "-b" or "-x").
        computational_priority: One of ["LOW","BELOWNORMAL","NORMAL","ABOVENORMAL","HIGH","REALTIME"].
        next_run_file:          Optional: Python script to run after all sims finish.
        priority_order:         Optional space-separated string for custom key order; None→insertion order.
        run_simulations:        If False→only export commands.txt; if True→launch sims.
        gpu_devices:            Each element is either a str (e.g. "-pu0") or list[str] (e.g. ["-pu0","-pu1"]).
                                Each simulation uses exactly one of these “groups,” in round robin.
                                If empty list, no GPU flags are ever passed.
        wait_time_after_run:    Seconds to wait after launching each sim.
        pause_on_finish:        If True→wait for any keypress before exiting (Windows only).
    """

    tcf: Path
    tuflowexe: Path
    batch_commands: str = "-x"
    computational_priority: str = "NORMAL"
    next_run_file: str | None = None
    priority_order: str | None = None
    run_simulations: bool = True
    gpu_devices: list[str | list[str]] | None = field(default_factory=list)
    wait_time_after_run: float = 2.0
    pause_on_finish: bool = True


@dataclass
class Parameters:
    """Wraps `core_params` and `run_variables`.

    Attributes:
        core_params:   An instance of CoreParameters.
        run_variables: A dict mapping two-character flags (e.g. "e1" or "s1")
                       to a list of values (e.g. ["01.00p","pmp"])."""

    core_params: CoreParameters
    run_variables: dict[str, list[str]]


@dataclass
class Simulation:
    """Represents exactly one TUFLOW run (one combination of flags).

    Attributes:
        args_for_python:   The list[str] passed to subprocess.Popen([...]).
        command_for_batch: Single-string command line (for commands.txt).
        assigned_gpu:      Either:
                              • a str (e.g. "-pu0"), or
                              • a list[str] (e.g. ["-pu0","-pu1"]),
                              • or None (no GPU flags).
        index:             1-based index of this run.
        start_time:        datetime when the process was launched (or None).
        process:           subprocess.Popen once launched (or None).
        end_time:          datetime when the process finished (or None)."""

    args_for_python: list[str]
    command_for_batch: str
    assigned_gpu: str | list[str] | None
    index: int
    start_time: datetime.datetime | None = None
    process: subprocess.Popen | None = None
    end_time: datetime.datetime | None = None


# ------------------------------------------------------------------------------
# VALIDATION & UTILITY FUNCTIONS
# ------------------------------------------------------------------------------


def check_and_set_defaults(params: Parameters) -> None:
    """Ensure core_params.tcf and core_params.tuflowexe are set.
    Raises ValueError if missing or empty."""
    required: list[str] = ["tcf", "tuflowexe"]
    for field_name in required:
        value: str | None = getattr(params.core_params, field_name)
        if value is None or (isinstance(value, (str, Path)) and not str(value).strip()):
            raise ValueError(f"Required parameter '{field_name}' is missing.")


def validate_file_paths(core: CoreParameters) -> None:
    """Ensure both TUFLOW exe and TCF file exist on disk.
    Raises FileNotFoundError otherwise."""
    if not core.tuflowexe.is_file():
        raise FileNotFoundError(f"TUFLOW executable not found at: {core.tuflowexe}")
    if not core.tcf.is_file():
        raise FileNotFoundError(f"TCF template not found at: {core.tcf}")


def run_post_script(script_path: str) -> None:
    """If the user provided `next_run_file`, run it in a new console after all sims finish.
    Args:
        script_path: Path to a Python script to execute."""
    try:
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_CONSOLE  # type: ignore[assignment]
            proc = subprocess.Popen(
                args=["python", script_path],
                creationflags=creationflags,  # type: ignore[arg-type]
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            proc = subprocess.Popen(
                ["python", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        out, err = proc.communicate()
        if proc.returncode == 0:
            logging.info(msg=f"Post-script {script_path} executed successfully.")
        else:
            logging.error(
                msg=f"Post-script {script_path} failed.\n"
                f"STDOUT:\n{out.decode(errors='ignore')}\n"
                f"STDERR:\n{err.decode(errors='ignore')}"
            )
    except FileNotFoundError:
        logging.error(f"Post‐script not found: {script_path}")
    except Exception as e:
        logging.error(f"Unexpected error running {script_path}: {e}")


def filter_parameters(parameters: dict[str, list[str]], tcf: Path) -> dict[str, list[str]]:
    """Given all run_variables and the TCF filename,
      1) Drop any flags whose first list-element is blank/whitespace.
      2) Warn if the TCF's "~XX~" placeholders don't match the provided keys.
    Args:
        parameters: dict mapping flags ("e1") → list[str] of values.
        tcf: Path to the TCF template (whose filename has "~e1~", "~e2~", ...).
    Returns:
        A new dict containing only non-empty flags."""
    non_empty: dict[str, list[str]] = {k: v for k, v in parameters.items() if v and v[0].strip()}
    provided_flags = set(non_empty.keys())

    # Extract placeholders from TCF filename (two-character placeholders only)
    placeholders: list[str] = re.findall(pattern=r"~(\w{2})~", string=tcf.name)
    tcf_flags: set[str] = set(placeholders)

    missing: set[str] = tcf_flags - provided_flags
    extra: set[str] = provided_flags - tcf_flags

    if missing:
        logging.warning(
            msg=f"TCF filename expects flags {sorted(tcf_flags)}, but run_variables is missing {sorted(missing)}."
        )
    if extra:
        logging.warning(msg=f"run_variables has extra flags not present in TCF: {sorted(extra)}.")

    return non_empty


def format_duration(seconds: float) -> str:
    """Convert a duration in seconds into "HH:MM:SS" or "MM:SS" format.
    Args:
        seconds: duration in seconds.
    Returns:
        A string like "00:05:23" or "01:23:45"."""
    td = datetime.timedelta(seconds=seconds)
    s = str(td)
    # If ≥ 1 hour, zero‐pad to 8 characters ("HH:MM:SS")
    if td.seconds >= 3600:
        s = s.zfill(8)
    return s


def get_psutil_priority(priority: str) -> int:
    """Map WINDOWS START priorities to psutil constants."""
    mapping: dict[str, int] = {
        "LOW": psutil.IDLE_PRIORITY_CLASS,
        "BELOWNORMAL": psutil.BELOW_NORMAL_PRIORITY_CLASS,
        "NORMAL": psutil.NORMAL_PRIORITY_CLASS,
        "ABOVENORMAL": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        "HIGH": psutil.HIGH_PRIORITY_CLASS,
        "REALTIME": psutil.REALTIME_PRIORITY_CLASS,
    }
    return mapping.get(priority.upper(), psutil.NORMAL_PRIORITY_CLASS)


def generate_arg_for_python(
    tuflowexe: Path,
    batch: list[str],
    tcf: Path,
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
    assigned_gpu: str | list[str] | None,
) -> list[str]:
    """Build the list[str] to pass to subprocess.Popen(…).
    It includes:
      • the TUFLOW.exe path
      • any batch flags (e.g. "-b")
      • any GPU flags (either a single "-puX" or a list of them)
      • the "-key value" pairs (left‐padded to max_lengths[key])
      • finally, the TCF path"""
    args: list[str] = [str(tuflowexe)]
    args.extend(batch)

    # Add GPU flags if provided
    if assigned_gpu:
        if isinstance(assigned_gpu, str):
            args.append(assigned_gpu)
        else:
            for gpu_flag in assigned_gpu:
                args.append(gpu_flag)

    for key, value in zip(keys, combo):
        padded_key: str = f"-{key}"
        padded_value: str = value.ljust(max_lengths[key])
        args.append(padded_key)
        args.append(padded_value)

    args.append(str(tcf))
    return args


def generate_arg_for_batch(
    computational_priority: str,
    tuflowexe: Path,
    batch: list[str],
    tcf: Path,
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
) -> str:
    """Build a single-line batch command.  Example:
    START /LOW /WAIT "" "C:\\TUFLOW\\TUFLOW.exe" -b -e1 01.00p -e2 01440m ... "C:\\path\to\tcf.tcf"
    """
    priority_flag: str = f"/{computational_priority.upper()}"
    # Build the command components without the GPU assignment
    parts: list[str] = [f'START {priority_flag} /WAIT "" "{tuflowexe}"']
    parts.extend(batch)

    # Pad the parameter keys and values
    for key, value in zip(keys, combo):
        padded_key = f"-{key}"
        padded_value = value.ljust(max_lengths[key])
        parts.append(padded_key)
        parts.append(padded_value)

    parts.append(f'"{tcf}"')
    command_str = " ".join(parts)
    return command_str


def generate_all_args(
    computational_priority: str,
    tuflowexe: Path,
    batch: list[str],
    tcf: Path,
    keys: list[str],
    combos: list[tuple[str, ...]],
    max_lengths: dict[str, int],
    gpu_devices: list[str | list[str]] | None,
) -> list[Simulation]:
    """
    For each combination, create a Simulation:
      • args_for_python (list[str] for Popen)
      • command_for_batch (string for commands.txt)
      • assigned_gpu  (one group from gpu_devices)
      • index        (1-based)
    """

    sims: list[Simulation] = []

    # If the user gave no gpu_devices, treat it as empty list (→ no GPU flags for every run)
    gpu_list: list[str | list[str]] = gpu_devices if gpu_devices else []

    for i, combo in enumerate(iterable=combos):
        if gpu_list:
            assigned: str | list[str] | None = gpu_list[i % len(gpu_list)]
        else:
            assigned = None

        args_py: list[str] = generate_arg_for_python(
            tuflowexe=tuflowexe,
            batch=batch,
            tcf=tcf,
            keys=keys,
            combo=combo,
            max_lengths=max_lengths,
            assigned_gpu=assigned,
        )

        cmd_batch: str = generate_arg_for_batch(
            computational_priority=computational_priority,
            tuflowexe=tuflowexe,
            batch=batch,
            tcf=tcf,
            keys=keys,
            combo=combo,
            max_lengths=max_lengths,
        )

        sims.append(
            Simulation(
                args_for_python=args_py,
                command_for_batch=cmd_batch,
                assigned_gpu=assigned,
                index=i + 1,
            )
        )

    return sims


def export_commands(commands_list: list[str], tuflowexe: Path, tcf: Path, base_filename: str) -> None:
    """Create a simplified batch file (commands.txt) so that:
      • TUFLOW_EXE     is set once at top
      • TCF            is set once at top
      • Each simulation line uses %TUFLOW_EXE% and %TCF%
      • Finally, append 'Pause' at the end.

    Example:
      @echo off
      set "TUFLOW_EXE=C:\\TUFLOW\2025.0.3\\TUFLOW_iSP_w64.exe"
      set "TCF=.\runs\tuflow_01_~s1~_~e1~_~e2~_~s4~.tcf"

      START /LOW /WAIT "" "%TUFLOW_EXE%" -b -e1 01.00p ... "%TCF%"
      ...
      Pause"""
    script_name: str = Path(__file__).stem
    unique_filename: str = f"{script_name}_{base_filename}"
    file_path = Path(unique_filename)

    try:
        with file_path.open("w", encoding="utf-8") as f:
            # 1) Header
            # f.write("@echo off\n")
            f.write(f'set "TUFLOW_EXE={tuflowexe}"\n')
            f.write(f'set "TCF={tcf}"\n\n')

            # 2) Each command line, replacing full paths with variables
            for cmd in commands_list:
                line: str = cmd.replace(str(tuflowexe), "%TUFLOW_EXE%").replace(str(tcf), "%TCF%")
                f.write(line + "\n")

            # 3) Pause at the end so user can see results
            f.write("\nPause\n")

        logging.info(f"Batch commands exported to {unique_filename}")
    except IOError as exc:
        logging.error(f"Failed to write {unique_filename}: {exc}")


def main() -> None:
    """1) Collect parameters via get_parameters() → build_parameters()
    2) Generate all combinations & build Simulation objects
    3) Export a simplified commands.txt
    4) (If enabled) Launch subprocesses, one per GPU group
    5) Optionally run next_run_file at end
    6) Pause for a keypress (if enabled)

    Gracefully handles Ctrl+C to terminate any still-running child processes."""
    # Basic console logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    pause_on_finish = True  # in case there is an error before params
    try:
        # Initialize colorama for colored output
        colorama.init(autoreset=True)

        # 1) Record start time
        start_dt: datetime.datetime = datetime.datetime.now()
        logging.info(msg=f"Script start: {start_dt:%Y-%m-%d %H:%M:%S}")

        # 2) cd to script folder
        script_dir: Path = Path(__file__).absolute().parent
        os.chdir(path=script_dir)
        logging.info(msg=f"Working dir: {script_dir}")

        # 3) Load parameters
        params: Parameters = get_parameters()
        core: CoreParameters = params.core_params
        pause_on_finish: bool = core.pause_on_finish

        # 4) Validate existence of TUFLOW exe and TCF
        try:
            validate_file_paths(core=core)
        except FileNotFoundError as fe:
            logging.error(fe)
            if pause_on_finish and sys.platform == "win32":
                os.system("pause")
            return

        # 5) Shortcuts
        priority: str = core.computational_priority
        tuflowexe: Path = core.tuflowexe
        batch_flags: list[str] = split_input_strings(input_val=core.batch_commands)
        tcf: Path = core.tcf
        next_run_file: str | None = core.next_run_file
        wait_after: float = core.wait_time_after_run
        gpu_devices: list[str | list[str]] | None = core.gpu_devices

        # 6) Filter run variables
        filtered_vars: dict[str, list[str]] = filter_parameters(parameters=params.run_variables, tcf=tcf)

        # 7) Determine key order
        if core.priority_order:
            order_list: list[str] = split_input_strings(core.priority_order)
            sorted_keys: list[str] = sorted(
                filtered_vars.keys(),
                key=lambda k: (order_list.index(k) if k in order_list else len(order_list)),
            )
        else:
            # Keep the insertion order of the dict
            sorted_keys = list(filtered_vars.keys())

        # 8) Build all combinations
        combos = list(itertools.product(*(filtered_vars[k] for k in sorted_keys)))

        # 9) Compute max_lengths
        max_lengths: dict[str, int] = {k: max(len(val) for val in values) for k, values in filtered_vars.items()}
        logging.debug(f"Max lengths: {max_lengths}")

        # 10) Generate Simulation objects
        simulations: list[Simulation] = generate_all_args(
            computational_priority=priority,
            tuflowexe=tuflowexe,
            batch=batch_flags,
            tcf=tcf,
            keys=sorted_keys,
            combos=combos,
            max_lengths=max_lengths,
            gpu_devices=gpu_devices,
        )

        # 11) Print summary
        logging.info(msg=f"Core params: {core}")
        logging.info(msg=f"Run variables: {filtered_vars}")
        logging.info(msg=f"Total combinations: {len(simulations)}")

        # 12) Show each batch command (for commands.txt)
        total: int = len(simulations)
        pad_width: int = len(str(total))
        logging.info(msg="Generated commands (for commands.txt):")
        for sim in simulations:
            print(f" {sim.index:0{pad_width}}: {sim.command_for_batch}")

        # 13) Export commands.txt
        export_commands(
            commands_list=[sim.command_for_batch for sim in simulations],
            tuflowexe=tuflowexe,
            tcf=tcf,
            base_filename="commands.txt",
        )

        # 14) If run_simulations is False, exit now
        if not core.run_simulations:
            logging.info("run_simulations=False → exiting after exporting commands.txt")
            if pause_on_finish and sys.platform == "win32":
                os.system("pause")
            return

        # 15) Launch subprocesses (max concurrent = len(gpu_devices))
        running: list[Simulation] = []
        queue: list[Simulation] = simulations.copy()

        # Helper to kill all child processes
        def terminate_all() -> None:
            for s in running:
                p = s.process
                if isinstance(p, subprocess.Popen) and p.poll() is None:
                    try:
                        p.terminate()
                        logging.info(f"Terminating simulation {s.index} (PID {p.pid})")
                    except Exception:
                        pass

        # Ctrl+C handler
        def keyboard_interrupt_handler(signum, frame):
            logging.warning("KeyboardInterrupt detected → terminating all child processes.")
            terminate_all()
            if pause_on_finish and sys.platform == "win32":
                os.system("pause")
            sys.exit(1)

        # Register SIGINT handler
        try:
            signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        except Exception:
            pass

        # Main launch loop
        while queue or running:
            # Launch new sims if a GPU group slot is free
            while queue and (not gpu_devices or len(running) < len(gpu_devices)):
                sim: Simulation = queue.pop(0)
                sim.start_time = datetime.datetime.now()
                gpu_desc: str | list[str] = sim.assigned_gpu if sim.assigned_gpu else "no GPU"
                logging.info(f"Launching simulation {sim.index}/{total} on {gpu_desc}")
                logging.debug(f"Full args: {' '.join(sim.args_for_python)}")

                # On Windows, open in a new console window; elsewhere, just Popen
                if sys.platform == "win32":
                    proc = subprocess.Popen(
                        args=sim.args_for_python,
                        creationflags=subprocess.CREATE_NEW_CONSOLE,  # type: ignore[arg-type]
                    )
                else:
                    proc = subprocess.Popen(sim.args_for_python)

                sim.process = proc

                # Set priority (Windows only)
                try:
                    ps_proc = psutil.Process(proc.pid)
                    ps_proc.nice(get_psutil_priority(priority))
                except Exception as e:
                    logging.warning(f"Could not set priority: {e}")

                running.append(sim)
                time.sleep(wait_after)

            # Check for completions
            for sim in running.copy():
                p = sim.process
                if isinstance(p, subprocess.Popen) and p.poll() is not None:
                    sim.end_time = datetime.datetime.now()
                    duration: str = format_duration(
                        seconds=((sim.end_time - sim.start_time).total_seconds() if sim.start_time else 0)
                    )
                    code = p.returncode
                    if code == 0:
                        logging.info(
                            f"{colorama.Fore.GREEN}"
                            f"Simulation {sim.index} completed successfully in {duration}."
                            f"{colorama.Style.RESET_ALL}"
                        )
                    else:
                        logging.info(
                            f"{colorama.Fore.RED}"
                            f"Simulation {sim.index} FAILED with code {code} in {duration}."
                            f"{colorama.Style.RESET_ALL}"
                        )
                    running.remove(sim)

            time.sleep(1)

        # 16) All sims done → run post‐script if any
        if next_run_file:
            logging.info(f"All simulations done → running {next_run_file}")
            run_post_script(next_run_file)
        else:
            logging.info("All simulations done → no post-script to run.")

        # 17) Final pause (Windows only)
        if pause_on_finish and sys.platform == "win32":
            os.system("pause")

    except Exception as err:
        logging.exception(f"Unexpected error: {err}")
        if pause_on_finish and sys.platform == "win32":
            os.system("pause")


if __name__ == "__main__":
    main()
