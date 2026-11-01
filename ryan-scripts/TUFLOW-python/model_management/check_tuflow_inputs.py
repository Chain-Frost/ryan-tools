r"""Check TUFLOW BAT/TXT model selections without running the solver.

Inputs: one or more inert command-list files and an optional Python TCF.
Outputs: terminal input findings, optional new issues CSV and/or run-plan JSON.
The command files are NEVER executed. Existing reports are not overwritten.

Examples:
    python check_tuflow_inputs.py --no-pause
    python check_tuflow_inputs.py --runs runs.bat --report issues.csv --no-pause
    python check_tuflow_inputs.py --runs a.bat --runs b.txt --plan-only --no-pause
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-01.2"

# ------------------------ USER-EDITABLE SETTINGS -------------------------
# CLI arguments override these settings. No naming prefix is required.

# Optional TCF: None accepts distinct TCFs in command files. A Path must
# match ALL imported TCFs and supplies the fallback for bare flag-only rows.
# Relative TCF paths use CONFIGURATION_DIR; e.g. Path(r"runs\model.tcf").
TCF: Path | None = None

# BAT/CMD/TXT input files, parsed as data, not executed. Paths relative to
# WORKING_DIR. The absolute shipped path reads the unrenamed runner's TXT
# beside this wrapper. Example: (Path("runs.txt"), Path("extra.bat")).
RUN_FILES: tuple[Path, ...] = (Path(__file__).absolute().with_name("run_tuflow_simulations.txt"),)

# Initial directory for imported command paths: None uses the terminal cwd.
# Example: Path(r"Q:\Project\TUFLOW") or Path(r"\\server\share\TUFLOW").
WORKING_DIR: Path | None = None

# Base for the Python TCF constraint. None uses WORKING_DIR when set, else
# this wrapper's directory. Example: Path(r"Q:\Project\TUFLOW").
CONFIGURATION_DIR: Path | None = None

# Optional issues CSV: None prints findings only. If set, creates a NEW CSV
# relative to WORKING_DIR (or terminal cwd); existing files are protected.
# Example: Path("reports\input_issues.csv").
REPORT_CSV: Path | None = None

# True lists the planned runs without inspecting model inputs; False checks
# all supported inputs including selected GeoPackage layers and BC CSV headers.
# --plan-only / --no-plan-only override this value.
PLAN_ONLY = False

# Optional full-plan JSON for private review: None disables it. Set a Path
# to create a NEW file relative to WORKING_DIR; existing files are protected.
# Example: Path("reports\planned_runs.json").
PLAN_JSON: Path | None = None

# Input syntax: "auto" detects supported BAT/TXT commands; "batch" or
# "commands" selects a specific reader mode. Example: "commands".
INPUT_SYNTAX: Literal["auto", "batch", "commands"] = "auto"

# File encoding for modern Windows BAT/TXT input, including UTF-8 BOMs.
INPUT_ENCODING = "utf-8-sig"

# Number of concurrent independent file inspections: integer 1 through 32.
# Example: 8 on a local SSD; 2 on a busy network share.
IO_WORKERS = 4

# Maximum entries considered when discovering files in one directory.
# Positive integer; e.g. 10_000 reduces work in very large folders.
MAX_DIRECTORY_ENTRIES = 50_000

# True pauses at the end in an interactive console. --no-pause overrides True;
# --pause overrides False. Headless/redirected stdin never waits.
PAUSE_ON_FINISH = True
# ------------------------------------------------------------------------

import argparse
import json
import os
from collections import Counter
from collections.abc import Sequence

from ryan_library.functions.tuflow.command_plan import read_command_plans
from ryan_library.functions.wrapper_utils import add_no_pause_cli_argument, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.command_preflight import CommandPreflightReport, check_command_plan


def main(
    *,
    tcf: Path | None,
    commands: Path | Sequence[Path],
    report: Path | None,
    working_directory: Path | None = None,
    configuration_directory: Path | None = None,
    plan_only: bool = False,
    plan_report: Path | None = None,
    syntax: Literal["auto", "batch", "commands"] = "auto",
    encoding: str = "utf-8-sig",
    io_workers: int = 4,
    maximum_directory_entries: int = 50_000,
) -> int:
    """Plan and check grouped TCFs, printing findings and optionally writing a new CSV."""
    # Establish both bases before parsing; BAT CD never changes Python config.
    initial = (working_directory if working_directory is not None else Path.cwd()).absolute()
    configured_base = (
        configuration_directory
        if configuration_directory is not None
        else (working_directory if working_directory is not None else Path(__file__).absolute().parent)
    ).absolute()
    supplied_sources = (commands,) if isinstance(commands, Path) else tuple(commands)
    # The reader makes paths absolute against process cwd, so anchor them to
    # WORKING_DIR here before parsing. Keep absolute source paths unchanged.
    sources = tuple(source if source.is_absolute() else (initial / source).absolute() for source in supplied_sources)
    plan = read_command_plans(
        sources,
        initial_directory=initial,
        fallback_tcf=str(tcf) if tcf is not None else None,
        configuration_directory=configured_base,
        environment=dict(os.environ),
        syntax=syntax,
        encoding=encoding,
    )
    counts = Counter(str(run.tcf) for run in plan.runs)
    print(
        f"Plan: {len(plan.runs)} active run(s), {len(counts)} TCF(s) "
        f"from {len(plan.source_files)} command file(s); {plan.duplicates_dropped} duplicate(s) discarded"
    )
    for control_file, count in counts.items():
        print(f"  {count} run(s): {control_file}")
    if plan_report is not None:
        output = plan_report.absolute()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8") as stream:
            json.dump({"preflight_performed": False, "runs": plan.manifest()}, stream, indent=2)
        print(f"Private run manifest: {output}")
    if plan_only:
        print("Plan only: no model inputs, executables or preflight have been run.")
        return 0
    result: CommandPreflightReport = check_command_plan(
        plan,
        check_gpkg_layers=True,
        io_workers=io_workers,
        maximum_directory_entries=maximum_directory_entries,
    )
    print(
        f"Preflight: {result.completed_runs}/{result.run_count} run(s) checked, "
        f"{len(result.checked_paths)} unique input path(s), {len(result.issues)} issue(s), "
        f"{result.affected_runs} affected run(s)"
    )
    print(
        f"Timing: parse {result.parse_seconds:.2f}s; contexts {result.context_seconds:.2f}s; "
        f"inventory {result.inventory_seconds:.2f}s"
    )
    if report is not None:
        output = result.write_csv(report)
        print(f"Issues CSV: {output}")
    for issue in result.issues:
        print(
            f"[{issue.finding.status.upper()}] {issue.run.source}:{issue.run.line_start}-{issue.run.line_end} "
            f"run {issue.run.index}: {issue.finding.reference} {issue.finding.detail}"
        )
    return 0 if result.passed else 1


def _parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only TUFLOW BAT/TXT command-plan and full input validation.",
        epilog=r'Example: python check_tuflow_inputs.py --runs "Q:\Project\runs.txt" --report issues.csv --no-pause',
    )
    model = parser.add_mutually_exclusive_group()
    model.add_argument("--tcf", type=Path, help="Set the Python TCF constraint and fallback.")
    model.add_argument("--no-tcf", action="store_true", help="Accept multiple TCFs, overriding TCF.")
    parser.add_argument(
        "--runs", type=Path, action="append", help="BAT/CMD/TXT files relative to WORKING_DIR; repeat in read order."
    )
    parser.add_argument("--working-directory", type=Path, help="Initial directory for imported command paths.")
    parser.add_argument("--configuration-directory", type=Path, help="Base for a relative Python TCF path.")
    report = parser.add_mutually_exclusive_group()
    report.add_argument("--report", type=Path, help="Create a NEW issues CSV; never replace existing reports.")
    report.add_argument("--no-report", action="store_true", help="Disable an editable REPORT_CSV.")
    parser.add_argument(
        "--plan-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Only plan runs, or override PLAN_ONLY to perform all supported input checks.",
    )
    manifest = parser.add_mutually_exclusive_group()
    manifest.add_argument("--plan-report", type=Path, help="Create a NEW private JSON run manifest.")
    manifest.add_argument("--no-plan-report", action="store_true", help="Disable an editable PLAN_JSON.")
    parser.add_argument("--syntax", choices=("auto", "batch", "commands"), help="Override INPUT_SYNTAX.")
    parser.add_argument("--encoding", help="Override INPUT_ENCODING; decoding errors are not ignored.")
    parser.add_argument("--io-workers", type=int, help="Number of file workers, 1-32.")
    parser.add_argument("--maximum-directory-entries", type=int, help="Positive directory discovery limit.")
    parser.add_argument("--pause", action="store_true", help="Enable the final interactive pause.")
    add_no_pause_cli_argument(parser=parser)
    options = parser.parse_args(argv)
    if options.pause and options.no_pause:
        parser.error("--pause and --no-pause cannot be combined")
    return options


def _run_cli(options: argparse.Namespace) -> int:
    """Resolve editable defaults, print the wrapper identity and own errors."""
    work = options.working_directory if options.working_directory is not None else WORKING_DIR
    base = (work if work is not None else Path.cwd()).absolute()

    def output_path(path: Path | None) -> Path | None:
        if path is None:
            return None
        return path if path.is_absolute() else (base / path).absolute()

    tcf = None if options.no_tcf else (options.tcf if options.tcf is not None else TCF)
    report = None if options.no_report else (options.report if options.report is not None else REPORT_CSV)
    plan_report = (
        None if options.no_plan_report else (options.plan_report if options.plan_report is not None else PLAN_JSON)
    )
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        return main(
            tcf=tcf,
            commands=tuple(options.runs) if options.runs is not None else RUN_FILES,
            report=output_path(report),
            working_directory=work,
            configuration_directory=(
                options.configuration_directory if options.configuration_directory is not None else CONFIGURATION_DIR
            ),
            plan_only=options.plan_only if options.plan_only is not None else PLAN_ONLY,
            plan_report=output_path(plan_report),
            syntax=options.syntax if options.syntax is not None else INPUT_SYNTAX,
            encoding=options.encoding if options.encoding is not None else INPUT_ENCODING,
            io_workers=options.io_workers if options.io_workers is not None else IO_WORKERS,
            maximum_directory_entries=(
                options.maximum_directory_entries
                if options.maximum_directory_entries is not None
                else MAX_DIRECTORY_ENTRIES
            ),
        )
    except Exception as exc:  # noqa: BLE001 - wrapper reports an incomplete check as failure
        print(f"Input preflight failed: {exc}")
        return 1
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
        if (options.pause or PAUSE_ON_FINISH) and not options.no_pause:
            pause_console()


if __name__ == "__main__":
    raise SystemExit(_run_cli(_parse_arguments()))
