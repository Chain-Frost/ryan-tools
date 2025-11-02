# ryan-scripts\TUFLOW-python\run_tuflow_2025_11.py
# 2025-11-02 version - parameter product and/or read from a list.
"""Single-file TUFLOW launcher for Windows.
USAGE
1. Copy this file into a job folder.
2. Edit `get_parameters()` ONLY - nowhere else.
3. Run:  python run_tuflow_2025_11.py"""


# ========= USER PARAMETERS ====== ***** EDIT ONLY THIS FUNCTION *****
def get_parameters() -> "Parameters":
    from pathlib import Path

    # ---- Parameter-product inputs (for 'parameter_product' or 'both') ----
    # Values are whitespace-separated; blanks are ignored.
    run_variables_raw: dict[str, str] = {
        "e1": "02.00p",  # 01.00p 05.00p 02.00p 01.00p 05.00p 10.00p 50.00p
        "e2": "00060m 00090m",  # 00720m 01080m 01440m 01800m 00120m 00030m 00060m  00090m 00180m  00270m 00360m 00540m 00720m
        "e3": "TP01 TP02 TP03  TP04 TP05 TP06 TP07 TP08 TP09 TP10",  #  TP01 TP02 TP03  TP04 TP05 TP06 TP07 TP08 TP09 TP10
        "e4": "CCNear ",  # CCNear CCNone
        "s1": "EXG",
        "s2": "AccessRoad",  # AccessRoad FullCatchment
        "s4": "32M",
        "s9": "MatUp  MatDown",
    }

    # ---- Core TUFLOW settings ----
    core_params = CoreParameters(
        tcf=Path(r".\runs\project_v01_~s2~_~s1~_~e4~_~e1~_~e2~_~e3~_~s4~_~s9~.tcf"),
        tuflowexe=Path(r"C:\TUFLOW\2025.2.0\TUFLOW_iSP_w64.exe"),
        batch_commands="-t",  # e.g. "-x", "-b", "-t". Avoid -puN here unless gpu_devices is None/[]
        priority_order="e4 e2 e1",  # Optional custom ordering of flags; e.g. "s1 s2 e1 e2". If None, uses insertion/first-seen order.
        # GPU slots (round-robin). Set to None/[] to pass no -pu flags at all.
        # Example: first sim uses GPU0&1, second sim uses GPU2, etc.
        # gpu_devices=[["-pu0", "-pu1"], ["-pu2"]]
        # gpu_devices=[["-pu0"], ["-pu1"]],
        # gpu_devices=[["-pu0"]],
        gpu_devices=[["-pu0"]],
        computational_priority="LOW",  # LOW|BELOWNORMAL|NORMAL|ABOVENORMAL|HIGH|REALTIME
        next_run_file=None,  # Export commands.txt (always exported) and optionally capture session START/END lines to a .log
        run_simulations=True,  # If False -> only export commands.txt; if True -> also run simulations
        wait_time_after_run=2.0,  # Seconds to wait after launching each simulation
        pause_on_finish=True,  # If True -> script waits for any keypress before exiting
        # ---- Mode selection ----
        smart_mode="parameter_product",  # "parameter_product" run_variables_raw | "textfiles" parse .bat/.txt command lists (exact combos) | "both" union of both (deduplicated)
        # ---- Input command lists (for 'textfiles' or 'both') ----
        input_files=None,  # [ # r"file1.bat", # r"file2.txt" ]
        export_commands=True,  # to a text file
        capture_console_log=False,  # export command prompt to text file log
        minimize_on_launch=True,
    )

    # Build and validate
    return build_parameters(core_params=core_params, run_variables_raw=run_variables_raw)


# Notes:
# - In textfiles/both mode, lines starting with comments are ignored and any -puN tokens
#   are stripped before parsing. Inline trailing comments are also removed outside quotes.
# - Placeholder enforcement: only -e*/-s* appearing as ~e?~/~s?~ in the TCF filename are
#   used, and any run missing a required placeholder is dropped.
# - priority_order: if provided, it controls key order; otherwise:
#     - textfiles mode: first-seen order from inputs
#     - parameter_product mode: insertion order of run_variables_raw keys

# ============================= IMPORTS (internal) ========================== #
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
from pathlib import Path
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
        gpu_devices:            None/[] -> pass no -pu flags (engine chooses default).
                                Or a list of groups: each group is either "-pu0" or ["-pu0","-pu1"].
                                Each simulation uses exactly one group in round-robin.
        computational_priority: One of ["LOW","BELOWNORMAL","NORMAL","ABOVENORMAL","HIGH","REALTIME"].
        next_run_file:          Optional: Python script to run after all sims finish.
        priority_order:         Optional space-separated string for custom key order; None -> insertion order.
        run_simulations:        If False -> only export commands.txt; if True -> launch sims.
        wait_time_after_run:    Seconds to wait after launching each sim.
        pause_on_finish:        If True -> wait for any keypress before exiting (Windows only).
        capture_console_log:    If True -> write a session .log of exact START/END lines.
        smart_mode:             "parameter_product" | "textfiles" | "both".
        input_files:            List of .bat/.txt files when smart_mode != "parameter_product".
        export_commands:        If True -> write <script>_commands.txt.
        minimize_on_launch:     If True -> new TUFLOW consoles start minimised (do not steal focus).
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
    capture_console_log: bool = False
    smart_mode: str = "parameter_product"
    input_files: list[str] | None = None  # set by get_parameters
    export_commands: bool = True
    minimize_on_launch: bool = False

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
        "capture_console_log": False,
        "smart_mode": "parameter_product",
        "export_commands": True,
        "minimize_on_launch": False,
    }

    def dump(self) -> None:
        """Pretty-print core parameters with subdued styling for defaults/None."""
        dim: str = colorama.Style.DIM
        bright: str = colorama.Style.BRIGHT
        reset: str = colorama.Style.RESET_ALL
        cyan: str = colorama.Fore.CYAN

        def style(name: str, value: Any) -> str:
            # Paths and required fields (no sensible default) -> bright
            if name in ("tcf", "tuflowexe"):
                return f"{bright}{value}{reset}"
            # Compare to default map
            default_val = self._DEFAULTS.get(name, None)
            is_default = value == default_val
            if value is None or is_default:
                return f"{dim}{value}{reset}"
            return f"{bright}{value}{reset}"

        logging.info("%s==== CORE PARAMETERS ====%s", cyan, reset)
        # Ensure we compute effective batch flags (may raise on conflicts; that's fine)
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
                (self.gpu_devices if self.gpu_devices not in (None, []) else "None/[] (no -pu)"),
            ),
            ("computational_priority", self.computational_priority),
            ("priority_order", self.priority_order),
            ("run_simulations", self.run_simulations),
            ("wait_time_after_run", self.wait_time_after_run),
            ("pause_on_finish", self.pause_on_finish),
            ("next_run_file", self.next_run_file),
            ("smart_mode", self.smart_mode),
            ("input_files", self.input_files),
            ("export_commands", self.export_commands),
            ("capture_console_log", self.capture_console_log),
            ("minimize_on_launch", self.minimize_on_launch),
        ]
        for name, val in items:
            # For "(effective)" label, look up original key for default styling comparison
            key_for_default = "batch_commands" if name.startswith("batch_commands") else name
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
                              - a str (e.g. "-pu0"), or
                              - a list[str] (e.g. ["-pu0","-pu1"]),
                              - or None (no GPU flags).
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

    # --- identity & set semantics (for de-duplication across inputs) ---
    def _identity_tokens(self) -> tuple[str, ...]:
        # Treat runs as identical even if GPU tokens differ; GPU is injected later.
        # If args_for_python never contains -puN (current design), this is still stable.
        return tuple(tok for tok in self.args_for_python if not _GPU_RE.match(tok))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Simulation):
            return NotImplemented
        return self._identity_tokens() == other._identity_tokens()

    def __hash__(self) -> int:
        return hash(self._identity_tokens())


# ================================ CONSTANTS =============================== #

_PRIORITY_SET: set[str] = {
    "LOW",
    "BELOWNORMAL",
    "NORMAL",
    "ABOVENORMAL",
    "HIGH",
    "REALTIME",
}
# Case-insensitive -puNNN (0 or positive integer; leading zeros allowed)
_GPU_RE: re.Pattern[str] = re.compile(pattern=r"(?i)^(?:-pu)(?:\d+)$")
_FLAG_KEY_RE: re.Pattern[str] = re.compile(pattern=r"^-[es][1-9]$", flags=re.IGNORECASE)
_SW_SHOWMINNOACTIVE = 7  # Windows API constant


# ====================== PARAMETER-BUILDING & VALIDATION ==================== #
def build_parameters(core_params: CoreParameters, run_variables_raw: dict[str, str]) -> Parameters:
    """Split raw run-variable strings, assemble a Parameters, validate.
    Args:
        core_params:       A CoreParameters instance (paths, flags, GPUs, etc.).
        run_variables_raw: A dict mapping two-char flags (e.g. "e1") -> raw str (e.g. "01.00p pmp").

    Returns:
        Parameters: Fully constructed and validated."""
    # 1) Convert each raw string into list[str] by splitting on whitespace:
    run_variables: dict[str, list[str]] = {
        key: split_input_strings(input_val=val) for key, val in run_variables_raw.items()
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
            raise ValueError(f"Invalid run variable key: {key}. Must be e1-e9 or s1-s9.")


# ====================== ARGUMENT-BUILDING HELPERS ========================= #
def _build_padded_flags(keys: list[str], combo: tuple[str, ...], max_lengths: dict[str, int]) -> list[str]:
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
    *,
    minimize: bool = False,
) -> str:
    """Build a single-line batch command for Windows CMD, e.g.:
    START /LOW /WAIT "" "C:/TUFLOW/TUFLOW_iSP_w64.exe" -b -e1 01.00p -e2 01440m ... "C:/path/to/tcf.tcf"
    """
    # Build the command components without the GPU assignment
    min_flag: str = " /MIN" if minimize else ""  # Keep consoles minimised when requested
    parts: list[str] = [f'START /{priority.upper()}{min_flag} /WAIT "" "{tuflowexe}"', *batch]
    parts.extend(_build_padded_flags(keys=keys, combo=combo, max_lengths=max_lengths))
    parts.append(f'"{tcf}"')
    return " ".join(parts)


# Centralise Simulation construction so CLI launch, preview, and exported batch stay identical.
def _assemble_simulation(
    *,
    core: CoreParameters,
    batch_flags: list[str],
    keys: list[str],
    combo: tuple[str, ...],
    max_lengths: dict[str, int],
    index: int,
) -> Simulation:
    args_py: list[str] = generate_arg_for_python(
        tuflowexe=core.tuflowexe,
        batch=batch_flags,
        tcf=core.tcf,
        keys=keys,
        combo=combo,
        max_lengths=max_lengths,
        assigned_gpu=None,
    )
    cmd_batch: str = generate_arg_for_batch(
        priority=core.computational_priority,
        tuflowexe=core.tuflowexe,
        batch=batch_flags,
        tcf=core.tcf,
        keys=keys,
        combo=combo,
        max_lengths=max_lengths,
        minimize=core.minimize_on_launch,
    )
    return Simulation(args_for_python=args_py, command_for_batch=cmd_batch, index=index)


# =============================== UTILITY FUNCTIONS ========================= #
def filter_parameters(params: dict[str, list[str]], tcf: Path) -> dict[str, list[str]]:
    """Given all run_variables and the TCF filename,
      1) Drop any flags whose first list-element is blank/whitespace.
      2) Warn if the TCF's "~XX~" placeholders don't match the provided keys.
      3) We can return flags that are not present in the tcf filename.
    Args:
        parameters: dict mapping flags ("e1") -> list[str] of values.
        tcf: Path to the TCF template (whose filename has "~e1~", "~e2~", ...).
    Returns:
        A new dict containing only non-empty flags."""
    non_empty: dict[str, list[str]] = {k: v for k, v in params.items() if v and v[0].strip()}
    placeholders: set[str] = set(re.findall(pattern=r"~(\w{2})~", string=tcf.name, flags=re.IGNORECASE))
    missing: set[str] = placeholders - {k.lower() for k in non_empty.keys()}
    extra: set[str] = {k.lower() for k in non_empty.keys()} - placeholders
    if missing:
        logging.warning(
            "TCF expects %s, but run_variables missing %s.",
            sorted(placeholders),
            sorted(missing),
        )
    if extra:
        logging.warning("run_variables has extra flags not present in TCF: %s.", sorted(extra))
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
    - If -puN present in batch_commands AND gpu_devices is set (non-empty) -> ERROR.
    - If -puN present in batch_commands AND gpu_devices is None/[] -> ACCEPT as-is.
    - If -puN absent in batch_commands -> no change.
    """
    flags: list[str] = split_input_strings(input_val=core.batch_commands)
    batch_gpu: list[str] = [f for f in flags if _GPU_RE.match(f)]
    has_gpu_devices: bool = bool(core.gpu_devices)
    if batch_gpu and has_gpu_devices:
        # Abort: double-specified GPU location
        raise ValueError(
            "GPU flags were specified in BOTH places:\n"
            f"  batch_commands: {flags}\n"
            f"  gpu_devices: {core.gpu_devices}\n\n"
            "Fix one of the following ways:\n"
            "  - Remove all -puN from batch_commands and keep gpu_devices set; OR\n"
            "  - Set gpu_devices=None (or []) and keep -puN only in batch_commands.\n"
        )
    # Otherwise accept as-is (including -puN in batch_commands when gpu_devices is None/[])
    return flags


###############################################################################
# =============================== I/O HELPERS =============================== #
###############################################################################
def export_commands(cmds: list[str], tuflowexe: Path, tcf: Path) -> None:
    """Write <script>_commands.txt with one START line per sim and a Pause at the end.
    Create a simplified batch file (commands.txt) so that:
    - TUFLOW_EXE is set once at top
    - TCF is set once at top
    - Each simulation line uses %TUFLOW_EXE% and %TCF%
    - Finally, append 'Pause' at the end.
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
            f.write(c.replace(str(tuflowexe), "%TUFLOW_EXE%").replace(str(tcf), "%TCF%") + "\n")

        # 3) Pause at the end so user can see the model runs
        f.write("\nPause\n")
    logging.info("Batch commands exported to %s (%d commands).", fn, len(cmds))


# ========================= SIMULATION BUILDERS ============================ #
def compute_simulations(params: Parameters) -> list[Simulation]:
    """Parameter-product path: build product of run_variables.
    1) Filter run variables based on TCF placeholders.
    2) Determine key order.
    3) Build all combinations (itertools.product).
    4) Compute max_lengths for padding.
    5) Call generate_all_args()."""
    core: CoreParameters = params.core_params
    filtered_vars: dict[str, list[str]] = filter_parameters(params=params.run_variables, tcf=core.tcf)

    if core.priority_order:
        order_list: list[str] = split_input_strings(input_val=core.priority_order)
        sorted_keys: list[str] = sorted(
            filtered_vars.keys(),
            key=lambda k: order_list.index(k) if k in order_list else len(order_list),
        )
    else:
        sorted_keys = list(filtered_vars.keys())

    combos: list[tuple[str, ...]] = (
        list(itertools.product(*(filtered_vars[k] for k in sorted_keys))) if sorted_keys else []
    )
    max_len: dict[str, int] = {k: max(map(len, v)) for k, v in filtered_vars.items()} if filtered_vars else {}
    logging.debug("Max lengths: %s", max_len)
    batch_flags: list[str] = get_batch_flags(core=core)

    sims: list[Simulation] = []
    for i, combo in enumerate(iterable=combos, start=1):
        sims.append(
            _assemble_simulation(
                core=core,
                batch_flags=batch_flags,
                keys=sorted_keys,
                combo=combo,
                max_lengths=max_len,
                index=i,
            )
        )
    return sims


# ---------- Parsed-list mode helpers  ----------
FlagKey = str  # "e1", "s2", ...
FlagVal = str  # "01.0p", "01440m", ...
Combo = dict[FlagKey, FlagVal]

_GPU_TOKEN_RE: re.Pattern[str] = re.compile(pattern=r"(?<!\S)-pu\d+(?!\S)", flags=re.IGNORECASE)
_FLAG_PAIR_RE: re.Pattern[str] = re.compile(pattern=r"-(?P<key>[es][1-9])\s+(?P<val>\S+)", flags=re.IGNORECASE)
_COMMENT_RE: re.Pattern[str] = re.compile(pattern=r"^\s*@?(?:REM\b|::|#|;|//)", flags=re.IGNORECASE)


def _strip_inline_comments(line: str) -> str:
    markers: tuple[str, ...] = (" ::", " #", " ;", " //", " REM", " rem", " !")
    in_quotes = False
    i = 0
    while i < len(line):
        ch: str = line[i]
        if ch == '"':
            in_quotes: bool = not in_quotes
            i += 1
            continue
        if not in_quotes:
            for m in markers:
                if line.startswith(m, i):
                    return line[:i].rstrip()
        i += 1
    return line


def _is_comment_or_blank(line: str) -> bool:
    return (not line.strip()) or bool(_COMMENT_RE.match(string=line))


def _strip_pu_flags(line: str) -> str:
    return _GPU_TOKEN_RE.sub(repl=" ", string=line).strip()


def _extract_flag_pairs_in_order(line: str) -> list[tuple[FlagKey, FlagVal]]:
    out: list[tuple[FlagKey, FlagVal]] = []
    for m in _FLAG_PAIR_RE.finditer(string=line):
        k = m.group("key").lower().strip()
        v = m.group("val").strip()
        if v:
            out.append((k, v))
    return out


def _canonical_key(combo: Combo) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(((k, v.lower()) for k, v in combo.items()), key=lambda t: t[0]))


def parse_input_files(files: list[Path]) -> tuple[list[Combo], list[str]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    unique: list[Combo] = []
    first_seen_order: list[str] = []
    seen_keys: set[str] = set()
    skipped_count = 0

    for f in files:
        try:
            content: list[str] = f.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception as exc:
            logging.error("Failed to read %s: %s", f, exc)
            continue

        for idx, raw in enumerate(iterable=content, start=1):
            line: str = raw.rstrip("\r\n")
            if _is_comment_or_blank(line=line):
                skipped_count += 1
                continue
            line_no_gpu: str = _strip_pu_flags(line=line)
            line_clean: str = _strip_inline_comments(line=line_no_gpu)
            if not line_clean.strip():
                skipped_count += 1
                continue
            pairs: list[tuple[str, str]] = _extract_flag_pairs_in_order(line=line_clean)
            if not pairs:
                skipped_count += 1
                continue
            for k, _ in pairs:
                if k not in seen_keys:
                    seen_keys.add(k)
                    first_seen_order.append(k)
            combo: Combo = {}
            for k, v in pairs:
                combo[k] = v
            canon = _canonical_key(combo)
            if canon in seen:
                logging.debug("Duplicate run ignored [%s:%d]: %s", f.name, idx, combo)
                continue
            seen.add(canon)
            unique.append(combo)

    logging.info(
        "Parsed %d unique simulations from %d input file(s). Skipped %d lines.",
        len(unique),
        len(files),
        skipped_count,
    )
    return unique, first_seen_order


def combos_to_run_variables_raw(combos: list[Combo]) -> dict[str, str]:
    bins: dict[str, set[str]] = {}
    for c in combos:
        for k, v in c.items():
            bins.setdefault(k, set()).add(v)
    run_vars: dict[str, str] = {}
    for k in sorted(bins.keys()):
        run_vars[k] = " ".join(sorted(bins[k], key=lambda s: s.lower()))
    return run_vars


def _max_lengths_for_padding(combos: list[Combo]) -> dict[str, int]:
    maxlen: dict[str, int] = {}
    for c in combos:
        for k, v in c.items():
            if len(v) > maxlen.get(k, 0):
                maxlen[k] = len(v)
    return dict(maxlen)


def _required_placeholders_from_tcf(tcf: Path) -> list[str]:
    return [x.lower() for x in re.findall(pattern=r"~([es][1-9])~", string=tcf.name, flags=re.IGNORECASE)]


# Which should not be enforcing down flags to match the placeholders in the tcf.
# It is fine to have more flags thant the file name. we just cannot be missing flags.
def _enforce_placeholders(core: CoreParameters, combos: list[Combo]) -> list[Combo]:
    required: list[str] = _required_placeholders_from_tcf(core.tcf)
    req_set: set[str] = set(required)
    if not required:
        logging.warning("TCF filename contains no ~e?~/~s?~ placeholders. No enforcement will occur.")
        return combos
    filtered: list[Combo] = []
    for c in combos:
        trimmed: dict[str, str] = {k: v for k, v in c.items() if k in req_set}
        if set(trimmed.keys()) != req_set:
            missing: list[str] = [k for k in required if k not in trimmed]
            logging.error(
                "Skipping run missing required placeholders %s ; got keys %s",
                missing,
                sorted(c.keys()),
            )
            continue
        filtered.append(trimmed)
    return filtered


def _ordered_keys(core: CoreParameters, combos: list[Combo], first_seen_order: list[str]) -> list[str]:
    keys_present: list[str] = []
    seen: set[str] = set()
    for c in combos:
        for k in c:
            if k not in seen:
                seen.add(k)
                keys_present.append(k)

    if core.priority_order:
        order_list: list[str] = split_input_strings(input_val=core.priority_order)
        ranked: list[str] = [k for k in order_list if k in keys_present]
        for k in first_seen_order:
            if k in keys_present and k not in ranked:
                ranked.append(k)
        return ranked
    return [k for k in first_seen_order if k in keys_present]


def build_simulations_from_combos(
    core: CoreParameters, combos: list[Combo], first_seen_order: list[str]
) -> list[Simulation]:
    combos = _enforce_placeholders(core=core, combos=combos)
    if not combos:
        return []
    ordered_keys: list[str] = _ordered_keys(core=core, combos=combos, first_seen_order=first_seen_order)
    max_lengths: dict[str, int] = _max_lengths_for_padding(combos)
    batch_flags: list[str] = get_batch_flags(core=core)

    sims: list[Simulation] = []
    for i, combo_dict in enumerate(combos, start=1):
        keys_for_this: list[str] = [k for k in ordered_keys if k in combo_dict]
        vals_for_this: tuple[str, ...] = tuple(combo_dict[k] for k in keys_for_this)
        sims.append(
            _assemble_simulation(
                core=core,
                batch_flags=batch_flags,
                keys=keys_for_this,
                combo=vals_for_this,
                max_lengths=max_lengths,
                index=i,
            )
        )
    return sims


# ========================= LAUNCH / MONITOR LOOP ========================== #
def _log_simulation_parameters(sim: Simulation, core: CoreParameters, total: int) -> None:
    """For each sim, only print the full arg string (no timestamp)."""
    tokens: list[str] = sim.args_for_python
    if not tokens:
        return
    # Print exactly what will be executed (including exe), with quoted TCF.
    print(" " + " ".join([*tokens[:-1], f'"{tokens[-1]}"']))


def _args_to_start_line(args: list[str], priority: str) -> str:
    if not args or len(args) < 2:
        return ""
    exe: str = args[0]
    *middle, tcf = args[1:]
    return f'START /{priority.upper()} /WAIT "" "{exe}" ' + " ".join(middle) + f' "{tcf}"'


def _append_session_log(session_log: Path, text: str) -> None:
    try:
        with session_log.open("a", encoding="utf-8") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
    except Exception as exc:
        logging.warning("Failed writing session log %s: %s", session_log, exc)


def launch_simulations(sims: list[Simulation], core: CoreParameters, session_log: Path | None = None) -> None:
    """Launch subprocesses (one per GPU group), monitor completion, handle Ctrl+C, and (optionally) log START lines/results."""
    batch_flags: list[str] = get_batch_flags(core=core)
    gpu_slots: list[str | list[str]] = core.gpu_devices or []
    in_use: list[bool] = [False] * len(gpu_slots)

    running: list[Simulation] = []
    queue: list[Simulation] = sims.copy()
    total: int = len(sims)

    # Session log header
    if session_log and core.capture_console_log:
        _append_session_log(
            session_log=session_log,
            text=f"==== SESSION START ====\npriority={core.computational_priority}\n",
        )

    def next_free_slot() -> int | None:
        for idx, used in enumerate(iterable=in_use):
            if not used:
                return idx
        return None

    def sigint_handler(signum: int, frame: FrameType | None) -> None:
        logging.warning("Ctrl+C detected - terminating all child processes.")
        for s in running:
            if s.process and s.process.poll() is None:
                logging.info("Terminating simulation %d (PID %s)", s.index, s.process.pid)
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
        # -- Can we start another run? -- # Launch new sims if a GPU group slot is free
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

                gpu_flags: list[str] = [gpu_group] if isinstance(gpu_group, str) else list(gpu_group)
                insert_at: int = 1 + len(batch_flags)
                sim.args_for_python = sim.args_for_python[:insert_at] + gpu_flags + sim.args_for_python[insert_at:]

            # ---------- launch ----------
            sim.start_time = datetime.datetime.now()
            print()  # blank line before each simulation's text block
            logging.info(
                "Launching sim %d/%d on %s",
                sim.index,
                total,
                sim.assigned_gpu or "No GPU Assigned (engine default if any)",
            )

            # Print the exact parameters being used at launch
            _log_simulation_parameters(sim=sim, core=core, total=total)

            # On Windows, open in a new console window; elsewhere, just Popen (but we don't support non-Windows here)
            # ---- session command log (exact START line used) ----
            if session_log and core.capture_console_log:
                start_line: str = _args_to_start_line(args=sim.args_for_python, priority=core.computational_priority)
                _append_session_log(
                    session_log=session_log,
                    text=f"[{sim.start_time.strftime(format='%Y-%m-%d %H:%M:%S')}] START sim {sim.index}: {start_line}",
                )
            startupinfo = None
            if core.minimize_on_launch and sys.platform == "win32":
                si = subprocess.STARTUPINFO()
                si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                si.wShowWindow = _SW_SHOWMINNOACTIVE  # (minimized, do not activate)
                startupinfo = si

            proc: subprocess.Popen[bytes] = subprocess.Popen(
                args=sim.args_for_python,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
                startupinfo=startupinfo,
            )
            psutil.Process(pid=proc.pid).nice(value=get_psutil_priority(priority=core.computational_priority))
            sim.process = proc
            running.append(sim)
            time.sleep(core.wait_time_after_run)

        # -- monitor running procs, Check for completions --
        for sim in running.copy():
            if sim.process and sim.process.poll() is not None:
                sim.end_time = datetime.datetime.now()
                dur: float = (sim.end_time - sim.start_time).total_seconds() if sim.start_time else 0.0
                return_code_ok = sim.process.returncode == 0
                status: str = (
                    f"{colorama.Fore.GREEN}OK{colorama.Style.RESET_ALL}"
                    if return_code_ok
                    else f"{colorama.Fore.RED}FAIL{colorama.Style.RESET_ALL}"
                )
                logging.info("Sim %d finished - %s (%s)", sim.index, status, format_duration(dur))
                if session_log and core.capture_console_log:
                    _append_session_log(
                        session_log=session_log,
                        text=f"[{sim.end_time.strftime(format='%Y-%m-%d %H:%M:%S')}] END   sim {sim.index}: {'OK' if return_code_ok else 'FAIL'} ({format_duration(dur)})",
                    )
                if sim.slot_index is not None:
                    in_use[sim.slot_index] = False
                running.remove(sim)
        time.sleep(0.2)

    if session_log and core.capture_console_log:
        _append_session_log(session_log=session_log, text="==== SESSION END ====\n")


def run_post_script(script_path: str | Path) -> None:
    """
    Launch a follow-up Python script in a separate console, capture its output,
    and report success or failure.

    * Windows -> new console window (`CREATE_NEW_CONSOLE`)
    * other OS -> same terminal (the script will still abort earlier on non-Windows)
    """
    script_path = Path(script_path).absolute()
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
    """List all simulations generated (no GPU assignment yet), zero-padded indices."""
    print("==== SIMULATION PLAN (no GPU assignment yet) ====")
    if not sims:
        print("(none)")
        return

    # Width = digits of the max index actually present (robust if indices aren't 1..N)
    max_idx: int = max((s.index for s in sims), default=0)
    width: int = len(str(max_idx)) if max_idx > 0 else 1

    for s in sims:
        args: list[str] = s.args_for_python  # [exe, <batch>, <-e/-s pairs>, tcf]
        if not args:
            continue
        # keep only flags + quoted TCF (omit exe)
        body: str = " ".join([*args[1:-1], f'"{args[-1]}"'])
        print(f"Sim {s.index:0{width}d}: {body}")


# ================================== MAIN ================================== #
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

    script_dir: Path = Path(__file__).absolute().parent
    os.chdir(path=script_dir)
    logging.info("Working dir: %s", script_dir)

    params: Parameters = get_parameters()
    core: CoreParameters = params.core_params

    # Print parameters (raises early if GPU flags are double specified)
    core.dump()

    # ---- Source simulations based on mode ----
    sims: list[Simulation] = []

    if core.smart_mode.lower() in {"textfiles", "both"}:
        combos: list[Combo] = []
        first_seen: list[str] = []
        if not core.input_files:
            logging.warning("smart_mode=%s but input_files is empty.", core.smart_mode)
        else:
            paths: list[Path] = [Path(f) for f in core.input_files]
            combos, first_seen = parse_input_files(files=paths)
        sims_from_files: list[Simulation] = build_simulations_from_combos(
            core=core, combos=combos, first_seen_order=first_seen
        )
        sims.extend(sims_from_files)

        # For logging, aggregate run_variables from parsed combos when not using product
        if core.smart_mode.lower() == "textfiles":
            run_vars_raw_from_files = combos_to_run_variables_raw(combos)
            dump_run_variables({k: split_input_strings(v) for k, v in run_vars_raw_from_files.items()})

    if core.smart_mode.lower() in {"parameter_product", "both"}:
        sims_from_product: list[Simulation] = compute_simulations(params=params)
        sims.extend(sims_from_product)
        dump_run_variables(run_vars=params.run_variables)

    # De-duplicate at Simulation level (identity ignores GPU placement)
    sims = list({s: None for s in sims}.keys())
    sims.sort(key=lambda s: s.index)  # keep stable order within each builder

    dump_simulations_preview(sims=sims)

    # Export commands
    if core.export_commands:
        export_commands(
            cmds=[s.command_for_batch for s in sims],
            tuflowexe=core.tuflowexe,
            tcf=core.tcf,
        )

    if not core.run_simulations:
        logging.info("run_simulations=False -> exiting after exporting commands.txt")
        return

    # Optional console capture
    session_log: Path | None = Path(f"{Path(__file__).stem}_commands.log") if core.capture_console_log else None
    launch_simulations(sims=sims, core=core, session_log=session_log)

    if core.next_run_file:
        run_post_script(script_path=core.next_run_file)

    logging.info("All finished.")
    if core.pause_on_finish:
        os.system("pause")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical("An error occurred: %s", e, exc_info=True)
        os.system("pause")
        sys.exit(1)
