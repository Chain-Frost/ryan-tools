# ryan-scripts\TUFLOW-python\run_tuflow_2025_09.py
# 2025-09-01 version

"""Single-file TUFLOW launcher for Windows.

USAGE
-----
1. Copy this file into a job folder.
2. Edit `get_parameters()` ONLY - nowhere else.
3. Run:  python run_tuflow_2025_09.py"""

###############################################################################
# ============================= USER PARAMETERS ============================= #
###############################################################################
from pathlib import Path


# ***** EDIT ONLY THIS FUNCTION *****
def get_parameters() -> "Parameters":

    run_variables_raw: dict[str, str] = {
        "e1": "01.0p",  # 02.0p 05.0p 10.0p",
        "e2": "00720m",
        "e5": "BaseILCL",
        "s1": "EXG",
        "s2": "SouthHub",
        "s4": "32M 16M 08M",
    }

    core_params = CoreParameters(
        tcf=Path(r".\runs\Unity_v02_~s2~_~s1~_~e5~_~e1~_~e2~_~s4~.tcf"),
        tuflowexe=Path(r"C:\TUFLOW\2025.1.2\TUFLOW_iSP_w64.exe"),
        batch_commands="-x",  # e.g. "-x", "-b", -t avoid -puN here unless gpu_devices is None/empty.
        gpu_devices=None,
        # None or [] → no -pu passed (engine chooses default GPU). Or set like ["-pu0"] or [["-pu0","-pu1"], "-pu2"].
        # Each element is a GPU *slot*; duplicates allow oversubscription
        # Example: first sim uses GPU0&1, second sim uses GPU2, etc.
        # gpu_devices=[["-pu0", "-pu1"], ["-pu2"]]
        # Windows START priority: "LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"
        computational_priority="LOW",
        # Optional: a Python script to run after all sims finish
        next_run_file=None,
        # If you want a custom ordering of keys, e.g. "s1 s2 e1 e2", otherwise None
        priority_order=None,
        # If False → only export commands.txt; if True → also run simulations
        run_simulations=True,
        # Seconds to wait after launching each simulation
        wait_time_after_run=2.0,
        # If True → script waits for any keypress before exiting (Windows only)
        pause_on_finish=True,
    )

    # Build and validate
    return build_parameters(
        core_params=core_params, run_variables_raw=run_variables_raw
    )


###############################################################################
# ============================= IMPORTS (internal) ========================== #
###############################################################################
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
from dataclasses import dataclass
from types import FrameType
from typing import Any, Final, ClassVar

import colorama
import psutil


###############################################################################
# =============================== DATA CLASSES ============================== #
###############################################################################
@dataclass(slots=True, kw_only=True)
class CoreParameters:
    """Core parameters for a TUFLOW simulation.

    Attributes:
        tcf:                    Path to the TCF.
        tuflowexe:              Path to the TUFLOW executable (.exe).
        batch_commands:         Windows batch flags (e.g. "-b" or "-x").
        gpu_devices:            None/[] → pass no -pu flags (engine chooses default).
                                Or a list of groups: each group is either "-pu0" or ["-pu0","-pu1"].
                                Each simulation uses exactly one group in round-robin.
                                If empty list/None, no GPU flags are ever passed.
        computational_priority: One of ["LOW","BELOWNORMAL","NORMAL","ABOVENORMAL","HIGH","REALTIME"].
        next_run_file:          Optional: Python script to run after all sims finish.
        priority_order:         Optional space-separated string for custom key order; None→insertion order.
        run_simulations:        If False→only export commands.txt; if True→launch sims.
        wait_time_after_run:    Seconds to wait after launching each sim.
        pause_on_finish:        If True→wait for any keypress before exiting (Windows only).
    """

    tcf: Path
    tuflowexe: Path
    batch_commands: str = "-x"
    gpu_devices: list[str | list[str]] | None = None
    computational_priority: str = "NORMAL"
    next_run_file: str | None = None
    priority_order: str | None = None
    run_simulations: bool = True
    wait_time_after_run: float = 2.0
    pause_on_finish: bool = True

    # ---- Defaults map (for styled dumps) ----
    # IMPORTANT: mark as ClassVar so dataclasses ignores it (avoids mutable-default error).
    _DEFAULTS: ClassVar[Final[dict[str, Any]]] = {
        "batch_commands": "-x",
        "gpu_devices": None,
        "computational_priority": "NORMAL",
        "next_run_file": None,
        "priority_order": None,
        "run_simulations": True,
        "wait_time_after_run": 2.0,
        "pause_on_finish": True,
    }

    def dump(self) -> None:
        """Pretty-print core parameters with subdued styling for defaults/None."""
        dim: str = colorama.Style.DIM
        bright: str = colorama.Style.BRIGHT
        reset: str = colorama.Style.RESET_ALL
        cyan: str = colorama.Fore.CYAN

        def style(name: str, value: Any) -> str:
            # Paths and required fields (no sensible default) → bright
            if name in ("tcf", "tuflowexe"):
                return f"{bright}{value}{reset}"
            # Compare to default map
            default_val = self._DEFAULTS.get(name, None)
            is_default = value == default_val
            if value is None or is_default:
                return f"{dim}{value}{reset}"
            return f"{bright}{value}{reset}"

        logging.info("%s==== CORE PARAMETERS ====%s", cyan, reset)
        # Ensure we compute effective batch flags (may raise on conflicts; that’s fine)
        effective_batch: list[str] = get_batch_flags(core=self, for_dump=True)

        items: list[tuple[str, Any]] = [
            ("tcf", self.tcf),
            ("tuflowexe", self.tuflowexe),
            (
                "batch_commands (effective)",
                " ".join(effective_batch) if effective_batch else "",
            ),
            (
                "gpu_devices",
                (
                    self.gpu_devices
                    if self.gpu_devices not in (None, [])
                    else "None/[] (no -pu)"
                ),
            ),
            ("computational_priority", self.computational_priority),
            ("priority_order", self.priority_order),
            ("run_simulations", self.run_simulations),
            ("wait_time_after_run", self.wait_time_after_run),
            ("pause_on_finish", self.pause_on_finish),
            ("next_run_file", self.next_run_file),
        ]
        for name, val in items:
            # For "(effective)" label, look up original key for default styling comparison
            key_for_default = (
                "batch_commands" if name.startswith("batch_commands") else name
            )
            logging.info("%s: %s", name, style(key_for_default, val))


@dataclass(slots=True, kw_only=True)
class Parameters:
    """Wraps `core_params` and `run_variables`.

    Attributes:
        core_params:   An instance of CoreParameters.
        run_variables: A dict mapping two-character flags (e.g. "e1" or "s1")
                       to a list of values (e.g. ["01.00p","pmp"])."""

    core_params: CoreParameters
    run_variables: dict[str, list[str]]


@dataclass(slots=True, kw_only=True)
class Simulation:
    """Represents exactly one TUFLOW run (one combination of flags).

    Attributes:
        args_for_python:   The list[str] passed to subprocess.Popen([...]).
        command_for_batch: Single-string command line (for commands.txt).
        assigned_gpu:      Either:
                              • a str (e.g. "-pu0"), or
                              • a list[str] (e.g. ["-pu0","-pu1"]),
                              • or None (no GPU flags).
        slot_index:        0-based index of the GPU slot used (or None).
        index:             1-based index of this run.
        start_time:        datetime when the process was launched (or None).
        process:           subprocess.Popen once launched (or None).
        end_time:          datetime when the process finished (or None)."""

    args_for_python: list[str]
    command_for_batch: str
    assigned_gpu: str | list[str] | None = None
    slot_index: int | None = None
    index: int = 0
    start_time: datetime.datetime | None = None
    # Popen returns bytes when text=False (our simulations) so one type is enough.
    process: subprocess.Popen[bytes] | None = None
    end_time: datetime.datetime | None = None


_PRIORITY_SET: set[str] = {
    "LOW",
    "BELOWNORMAL",
    "NORMAL",
    "ABOVENORMAL",
    "HIGH",
    "REALTIME",
}
# Case-insensitive -puNNN (0 or positive integer; leading zeros allowed)
_GPU_RE: re.Pattern[str] = re.compile(pattern=r"^-pu\d+$", flags=re.IGNORECASE)
_FLAG_KEY_RE: re.Pattern[str] = re.compile(pattern=r"^-[es][1-9]$")


###############################################################################
# ====================== PARAMETER-BUILDING & VALIDATION ==================== #
###############################################################################
def build_parameters(
    core_params: CoreParameters, run_variables_raw: dict[str, str]
) -> Parameters:
    """Split raw run-variable strings, assemble a Parameters, validate.
    Args:
        core_params:       A CoreParameters instance (paths, flags, GPUs, etc.).
        run_variables_raw: A dict mapping two-char flags (e.g. "e1") → raw str (e.g. "01.00p pmp").

    Returns:
        Parameters: Fully constructed and validated."""
    # 1) Convert each raw string into list[str] by splitting on whitespace:
    run_variables: dict[str, list[str]] = {
        key: split_input_strings(input_val=val)
        for key, val in run_variables_raw.items()
    }

    # 2) Build the Parameters dataclass from core_params + run_variables:
    params = Parameters(core_params=core_params, run_variables=run_variables)

    # 3) Validate required fields (raises ValueError if something is missing):
    check_and_set_defaults(params=params)
    return params


def split_input_strings(input_val: str | list[str]) -> list[str]:
    """Normalise the user-supplied flag values.
    Accept either
        "01.00p pmp"
        ["01.00p", " pmp"]
    and return ['01.00p', 'pmp']."""
    if isinstance(input_val, str):
        parts: list[str] = input_val.strip().split()
    else:
        parts = [str(item).strip() for item in input_val]
    return [p for p in parts if p]  # drop empties


def check_and_set_defaults(params: Parameters) -> None:
    """Ensure core_params.tcf and core_params.tuflowexe are set.
    Raises ValueError if missing or empty.
    Also check gpu flags and computational priority."""
    c: CoreParameters = params.core_params
    if not c.tcf.is_file():
        raise FileNotFoundError(f"TCF file not found: {c.tcf}")
    if not c.tuflowexe.is_file():
        raise FileNotFoundError(f"TUFLOW exe not found: {c.tuflowexe}")
    if c.computational_priority.upper() not in _PRIORITY_SET:
        raise ValueError(f"Invalid priority: {c.computational_priority}")

    # Validate gpu_devices if provided
    if c.gpu_devices:
        for slot in c.gpu_devices:
            if isinstance(slot, str):
                if not _GPU_RE.match(string=slot):
                    raise ValueError(f"Invalid GPU flag: {slot}")
            else:
                for flag in slot:
                    if not _GPU_RE.match(string=flag):
                        raise ValueError(f"Invalid GPU flag: {flag}")

    # Check that run_variables keys are only e1-e9 or s1-s9
    for key in params.run_variables.keys():
        if not re.fullmatch(pattern=r"[es][1-9]", string=key):
            raise ValueError(
                f"Invalid run variable key: {key}. Must be e1-e9 or s1-s9."
            )


###############################################################################
# ====================== ARGUMENT-BUILDING HELPERS ========================= #
###############################################################################
def _build_padded_flags(
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
) -> list[str]:
    """Given parameter keys, one combination (tuple of values), and max_lengths,
    produce a flattened list: ['-key1', 'value1_padded', '-key2', 'value2_padded', ...].
    """
    parts: list[str] = []
    for key, value in zip(keys, combo):
        parts.extend([f"-{key}", value.ljust(max_lengths[key])])
    return parts


def generate_arg_for_python(
    tuflowexe: Path,
    batch: list[str],
    tcf: Path,
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
    assigned_gpu: str | list[str] | None,
) -> list[str]:
    """Build the list[str] to pass to subprocess.Popen([...]) for a single run."""
    args: list[str] = [str(tuflowexe), *batch]
    # Add GPU flags if provided
    if assigned_gpu:
        args.extend([assigned_gpu] if isinstance(assigned_gpu, str) else assigned_gpu)
    args.extend(_build_padded_flags(keys=keys, combo=combo, max_lengths=max_lengths))
    args.append(str(tcf))
    return args


def generate_arg_for_batch(
    priority: str,
    tuflowexe: Path,
    batch: list[str],
    tcf: Path,
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
) -> str:
    """Build a single-line batch command for Windows CMD, e.g.:
    START /LOW /WAIT "" "C:/TUFLOW/TUFLOW_iSP_w64.exe" -b -e1 01.00p -e2 01440m ... "C:/path/to/tcf.tcf"
    """
    # Build the command components without the GPU assignment
    parts: list[str] = [f'START /{priority.upper()} /WAIT "" "{tuflowexe}"', *batch]
    parts.extend(_build_padded_flags(keys=keys, combo=combo, max_lengths=max_lengths))
    parts.append(f'"{tcf}"')
    return " ".join(parts)


###############################################################################
# =============================== UTILITY FUNCTIONS ========================= #
###############################################################################
def filter_parameters(params: dict[str, list[str]], tcf: Path) -> dict[str, list[str]]:
    """Given all run_variables and the TCF filename,
      1) Drop any flags whose first list-element is blank/whitespace.
      2) Warn if the TCF's "~XX~" placeholders don't match the provided keys.
    Args:
        parameters: dict mapping flags ("e1") → list[str] of values.
        tcf: Path to the TCF template (whose filename has "~e1~", "~e2~", ...).
    Returns:
        A new dict containing only non-empty flags."""
    non_empty: dict[str, list[str]] = {
        k: v for k, v in params.items() if v and v[0].strip()
    }
    placeholders: set[str] = set(re.findall(pattern=r"~(\w{2})~", string=tcf.name))
    missing: set[str] = placeholders - non_empty.keys()
    extra: set[str] = non_empty.keys() - placeholders
    if missing:
        logging.warning(
            "TCF filename expects flags %s, but run_variables is missing %s.",
            sorted(placeholders),
            sorted(missing),
        )
    if extra:
        logging.warning(
            "run_variables has extra flags not present in TCF: %s.", sorted(extra)
        )
    return non_empty


def format_duration(seconds: float) -> str:
    """Convert a duration in seconds into "HH:MM:SS" format.
    Always zero-pads hours, minutes, and seconds to two digits."""
    total_secs = int(seconds)
    hours, rem = divmod(total_secs, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


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


def get_batch_flags(core: CoreParameters, *, for_dump: bool = False) -> list[str]:
    """Return effective batch flags, enforcing GPU flag rules.

    Rules:
    - If -puN present in batch_commands AND gpu_devices is set (non-empty) → ERROR.
    - If -puN present in batch_commands AND gpu_devices is None/[] → ACCEPT as-is.
    - If -puN absent in batch_commands → no change.
    """
    flags: list[str] = split_input_strings(input_val=core.batch_commands)
    batch_gpu: list[str] = [f for f in flags if _GPU_RE.match(f)]
    has_gpu_devices: bool = bool(core.gpu_devices)

    if batch_gpu and has_gpu_devices:
        # Abort: double-specified GPU location
        msg = (
            "GPU flags were specified in BOTH places:\n"
            f"  batch_commands: {flags}\n"
            f"  gpu_devices: {core.gpu_devices}\n\n"
            "Fix one of the following ways:\n"
            "  • Remove all -puN from batch_commands and keep gpu_devices set; OR\n"
            "  • Set gpu_devices=None (or []) and keep -puN only in batch_commands.\n"
        )
        raise ValueError(msg)

    # Otherwise accept as-is (including -puN in batch_commands when gpu_devices is None/[])
    return flags


###############################################################################
# =============================== I/O HELPERS =============================== #
###############################################################################
def export_commands(cmds: list[str], tuflowexe: Path, tcf: Path) -> None:
    """Write <script>_commands.txt with one START line per sim and a Pause at the end.
    Create a simplified batch file (commands.txt) so that:
    • TUFLOW_EXE is set once at top
    • TCF is set once at top
    • Each simulation line uses %TUFLOW_EXE% and %TCF%
    • Finally, append 'Pause' at the end.
    Example:
      @echo off
      set "TUFLOW_EXE=C:\\TUFLOW\2025.0.3\\TUFLOW_iSP_w64.exe"
      set "TCF=.\runs\tuflow_01_~s1~_~e1~_~e2~_~s4~.tcf"

      START /LOW /WAIT "" "%TUFLOW_EXE%" -b -e1 01.00p ... "%TCF%"
      ...
      Pause"""
    fn: str = f"{Path(__file__).stem}_commands.txt"
    with open(file=fn, mode="w", encoding="utf-8") as f:
        # 1) Header
        # f.write("@echo off\n")
        f.write(f'set "TUFLOW_EXE={tuflowexe}"\n')
        f.write(f'set "TCF={tcf}"\n\n')

        # 2) Each command line, replacing full paths with variables
        for c in cmds:
            f.write(
                c.replace(str(tuflowexe), "%TUFLOW_EXE%").replace(str(tcf), "%TCF%")
                + "\n"
            )

        # 3) Pause at the end so user can see the model runs
        f.write("\nPause\n")
    logging.info("Batch commands exported to %s (%d commands).", fn, len(cmds))


###############################################################################
# ========================= SIMULATION LAUNCH LOOP ========================= #
###############################################################################
def compute_simulations(params: Parameters) -> list[Simulation]:
    """Build combinations and precompute base args (no GPU injected yet).
    1) Filter run variables based on TCF placeholders.
    2) Determine key order.
    3) Build all combinations (itertools.product).
    4) Compute max_lengths for padding.
    5) Call generate_all_args()."""
    core: CoreParameters = params.core_params
    filtered_vars: dict[str, list[str]] = filter_parameters(
        params=params.run_variables, tcf=core.tcf
    )

    if core.priority_order:
        order_list: list[str] = split_input_strings(input_val=core.priority_order)
        sorted_keys: list[str] = sorted(
            filtered_vars.keys(),
            key=lambda k: order_list.index(k) if k in order_list else len(order_list),
        )
    else:
        sorted_keys = list(filtered_vars.keys())

    combos: list[tuple[str, ...]] = list(
        itertools.product(*(filtered_vars[k] for k in sorted_keys))
    )
    max_len: dict[str, int] = {k: max(map(len, v)) for k, v in filtered_vars.items()}
    logging.debug("Max lengths: %s", max_len)
    batch_flags: list[str] = get_batch_flags(core=core)

    sims: list[Simulation] = []
    for i, combo in enumerate(iterable=combos, start=1):
        cmd_batch: str = generate_arg_for_batch(
            priority=core.computational_priority,
            tuflowexe=core.tuflowexe,
            batch=batch_flags,
            tcf=core.tcf,
            keys=sorted_keys,
            combo=combo,
            max_lengths=max_len,
        )
        args_py: list[str] = generate_arg_for_python(
            tuflowexe=core.tuflowexe,
            batch=batch_flags,
            tcf=core.tcf,
            keys=sorted_keys,
            combo=combo,
            max_lengths=max_len,
            assigned_gpu=None,  # GPU is injected at launch time only
        )
        sims.append(
            Simulation(args_for_python=args_py, command_for_batch=cmd_batch, index=i)
        )
    return sims


def _log_simulation_parameters(
    sim: Simulation, core: CoreParameters, total: int
) -> None:
    """Structured parameter block printed immediately before launch (exact GPU included)."""
    tokens: list[str] = sim.args_for_python
    exe: str = tokens[0]
    tcf: str = tokens[-1]
    batch_flags: list[str] = get_batch_flags(core)

    cursor: int = 1
    batch: list[str] = tokens[cursor : cursor + len(batch_flags)]
    cursor += len(batch_flags)

    gpu: list[str] = []
    while cursor < len(tokens) - 1 and _GPU_RE.match(tokens[cursor]):
        gpu.append(tokens[cursor])
        cursor += 1

    pairs: list[str] = tokens[cursor:-1]
    kv: list[str] = []
    i: int = 0
    while i < len(pairs):
        key: str = pairs[i]
        val: str = pairs[i + 1].strip() if i + 1 < len(pairs) else ""
        kv.append(f"{key}={val}")
        i += 2

    logging.info("---- Simulation %d/%d parameters ----", sim.index, total)
    logging.info("exe: %s", exe)
    logging.info("priority: %s", core.computational_priority)
    logging.info("batch_flags: %s", batch if batch else "[]")
    logging.info("gpu_flags: %s", gpu if gpu else "[] (none)")
    logging.info("flags: %s", " ".join(kv) if kv else "(none)")
    logging.info("tcf: %s", tcf)
    logging.info("full args: %s", " ".join(tokens))


def launch_simulations(sims: list[Simulation], core: CoreParameters) -> None:
    """Launch subprocesses (one per GPU group), monitor completion, handle Ctrl+C, and run any post script."""
    batch_flags: list[str] = get_batch_flags(core=core)
    gpu_slots: list[str | list[str]] = core.gpu_devices or []
    in_use: list[bool] = [False] * len(gpu_slots)

    running: list[Simulation] = []
    queue: list[Simulation] = sims.copy()
    total: int = len(sims)

    def next_free_slot() -> int | None:
        for idx, used in enumerate(iterable=in_use):
            if not used:
                return idx
        return None

    def sigint_handler(signum: int, frame: FrameType | None) -> None:
        logging.warning("Ctrl+C detected - terminating all child processes.")
        for s in running:
            if s.process and s.process.poll() is None:
                logging.info(
                    "Terminating simulation %d (PID %s)", s.index, s.process.pid
                )
                s.process.terminate()
        if core.pause_on_finish:
            os.system("pause")
        sys.exit(1)

    # Register SIGINT handler
    signal.signal(signalnum=signal.SIGINT, handler=sigint_handler)

    # Main launch loop

    # Maximum parallelism: len(gpu_slots) if GPUs are listed, otherwise 1 GPU slot
    max_parallel: int = len(gpu_slots) if gpu_slots else 1
    slot_idx: int | None = None

    while queue or running:
        # ── Can we start another run? ── # Launch new sims if a GPU group slot is free
        can_start: bool = (len(running) < max_parallel) and (
            not gpu_slots or (slot_idx := next_free_slot()) is not None
        )

        if queue and can_start:
            sim: Simulation = queue.pop(0)

            # Inject GPU flags only now (exact assignment; no prediction earlier) (only if gpu_slots was provided)
            if gpu_slots:
                assert slot_idx is not None  # convince type-checker
                gpu_group: str | list[str] = gpu_slots[slot_idx]
                sim.assigned_gpu, sim.slot_index = gpu_group, slot_idx
                in_use[slot_idx] = True

                gpu_flags: list[str] = (
                    [gpu_group] if isinstance(gpu_group, str) else list(gpu_group)
                )
                insert_at: int = 1 + len(batch_flags)
                sim.args_for_python = (
                    sim.args_for_python[:insert_at]
                    + gpu_flags
                    + sim.args_for_python[insert_at:]
                )

            # ---------- launch ----------
            sim.start_time = datetime.datetime.now()
            logging.info(
                "Launching sim %d/%d on %s",
                sim.index,
                total,
                sim.assigned_gpu or "No GPU Assigned (engine default if any)",
            )

            # Print the exact parameters being used at launch
            _log_simulation_parameters(sim=sim, core=core, total=total)

            # On Windows, open in a new console window; elsewhere, just Popen (but we don't support non-Windows here)
            proc: subprocess.Popen[bytes] = subprocess.Popen(
                args=sim.args_for_python,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            psutil.Process(pid=proc.pid).nice(
                value=get_psutil_priority(priority=core.computational_priority)
            )
            sim.process = proc
            running.append(sim)
            time.sleep(core.wait_time_after_run)

        # ── monitor running procs, Check for completions ──
        for sim in running.copy():
            if sim.process and sim.process.poll() is not None:
                sim.end_time = datetime.datetime.now()
                dur: float = (
                    (sim.end_time - sim.start_time).total_seconds()
                    if sim.start_time
                    else 0.0
                )
                status: str = (
                    f"{colorama.Fore.GREEN}OK{colorama.Style.RESET_ALL}"
                    if sim.process.returncode == 0
                    else f"{colorama.Fore.RED}FAIL{colorama.Style.RESET_ALL}"
                )
                logging.info(
                    "Sim %d finished - %s (%s)", sim.index, status, format_duration(dur)
                )
                if sim.slot_index is not None:
                    in_use[sim.slot_index] = False
                running.remove(sim)
        time.sleep(0.2)


def run_post_script(script_path: str | Path) -> None:
    """
    Launch a follow-up Python script in a separate console, capture its output,
    and report success or failure.

    * Windows ➜ new console window (`CREATE_NEW_CONSOLE`)
    * other OS ➜ same terminal (the script will still abort earlier on non-Windows)
    """
    script_path = Path(script_path).resolve()
    if not script_path.is_file():
        logging.error("Post-script not found: %s", script_path)
        return

    cmd: list[str] = ["python", str(script_path)]
    creationflags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0

    try:
        proc: subprocess.Popen[str] = subprocess.Popen(
            args=cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creationflags,
            text=True,  # decode to str automatically
        )
        out, err = proc.communicate()

        if proc.returncode == 0:
            logging.info("Post-script %s executed successfully.", script_path)
            if out.strip():
                logging.debug("Post-script stdout:\n%s", out.rstrip())
        else:
            logging.error(
                "Post-script %s failed with code %s.\nSTDOUT:\n%s\nSTDERR:\n%s",
                script_path,
                proc.returncode,
                out.rstrip(),
                err.rstrip(),
            )
    except Exception as exc:
        logging.exception("Unexpected error while running %s: %s", script_path, exc)


def dump_run_variables(run_vars: dict[str, list[str]]) -> None:
    cyan: str = colorama.Fore.CYAN
    reset: str = colorama.Style.RESET_ALL
    logging.info("%s==== RUN VARIABLES ====%s", cyan, reset)
    if not run_vars:
        logging.info("(none)")
        return
    for k in sorted(run_vars.keys()):
        logging.info("%s: %s", k, run_vars[k])


def dump_simulations_preview(sims: list[Simulation]) -> None:
    """List all simulations generated (no GPU prediction)."""
    cyan: str = colorama.Fore.CYAN
    reset: str = colorama.Style.RESET_ALL
    logging.info("%s==== SIMULATION PLAN (no GPU assignment yet) ====%s", cyan, reset)
    if not sims:
        logging.info("(none)")
        return
    for s in sims:
        logging.info("Sim %d: %s", s.index, s.command_for_batch)


def main() -> None:
    if sys.platform != "win32":
        print("This launcher is Windows-only. Exiting.")
        sys.exit(1)

    colorama.init(autoreset=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    script_dir: Path = Path(__file__).resolve().parent
    os.chdir(path=script_dir)
    logging.info("Working dir: %s", script_dir)

    params: Parameters = get_parameters()

    # Print parameters first (raises early if GPU flags are double specified)
    params.core_params.dump()
    dump_run_variables(run_vars=params.run_variables)

    sims: list[Simulation] = compute_simulations(params=params)
    dump_simulations_preview(sims=sims)

    export_commands(
        cmds=[s.command_for_batch for s in sims],
        tuflowexe=params.core_params.tuflowexe,
        tcf=params.core_params.tcf,
    )

    if not params.core_params.run_simulations:
        logging.info("run_simulations=False → exiting after exporting commands.txt")
        return

    launch_simulations(sims=sims, core=params.core_params)

    if params.core_params.next_run_file:
        run_post_script(script_path=params.core_params.next_run_file)

    logging.info("All finished.")
    if params.core_params.pause_on_finish:
        os.system("pause")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical("An error occurred: %s", e, exc_info=True)
        os.system("pause")
        sys.exit(1)
