r"""Inspect independent BAT/text run lists without launching simulations.

Examples:
    python check_tuflow_inputs.py --runs first.bat --runs second.txt --working-directory "T:\\project" --plan-only --no-pause
    python check_tuflow_inputs.py --runs runs.bat --tcf "T:\\project\\model.tcf" --report issues.csv --no-pause

DEFAULT_TCF / --tcf, when configured, must match every active imported TCF;
it also supplies the bare-row fallback. Set DEFAULT_TCF to None for multiple
imported models. Relative input paths are based on the invoking cwd; relative
Python TCF configuration has its own explicit base.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-31.2"
# None permits multiple imported TCFs; a Path also constrains all active runs.
DEFAULT_TCF: Path | None = None
DEFAULT_COMMANDS = Path("run_tuflow_simulations_commands.txt")
DEFAULT_REPORT = Path("tuflow_input_preflight.csv")
DEFAULT_CHECK_GPKG_LAYERS = True
DEFAULT_IO_WORKERS = 4
DEFAULT_MAXIMUM_DIRECTORY_ENTRIES = 50_000

import argparse
import json
import os
from collections import Counter
from collections.abc import Sequence
from typing import Literal

from ryan_library.functions.tuflow.command_plan import read_command_plans
from ryan_library.orchestrators.tuflow.command_preflight import CommandPreflightReport, check_command_plan


def main(
    *,
    tcf: Path | None,
    commands: Path | Sequence[Path],
    report: Path,
    check_gpkg_layers: bool,
    working_directory: Path | None = None,
    configuration_directory: Path | None = None,
    plan_only: bool = False,
    plan_report: Path | None = None,
    syntax: Literal["auto", "batch", "commands"] = "auto",
    encoding: str = "utf-8-sig",
    io_workers: int = DEFAULT_IO_WORKERS,
    maximum_directory_entries: int = DEFAULT_MAXIMUM_DIRECTORY_ENTRIES,
) -> int:
    """Plan every row first, then check grouped TCFs and write a new issue CSV."""
    # Establish both bases before parsing; BAT CD never changes Python config.
    initial = (working_directory if working_directory is not None else Path.cwd()).absolute()
    configured_base = (
        configuration_directory
        if configuration_directory is not None
        else (working_directory if working_directory is not None else Path(__file__).absolute().parent)
    ).absolute()
    sources = (commands,) if isinstance(commands, Path) else tuple(commands)
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
        f"from {len(plan.source_files)} command file(s); duplicates retained"
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
        check_gpkg_layers=check_gpkg_layers,
        io_workers=io_workers,
        maximum_directory_entries=maximum_directory_entries,
    )
    output: Path = result.write_csv(report)
    print(
        f"Preflight: {result.completed_runs}/{result.run_count} run(s) checked, "
        f"{len(result.checked_paths)} unique input path(s), {len(result.issues)} issue(s), "
        f"{result.affected_runs} affected run(s)"
    )
    print(
        f"Timing: parse {result.parse_seconds:.2f}s; contexts {result.context_seconds:.2f}s; "
        f"inventory {result.inventory_seconds:.2f}s"
    )
    print(f"Issues CSV: {output}")
    for issue in result.issues:
        print(
            f"[{issue.status.upper()}] {issue.run.source}:{issue.run.line_start}-{issue.run.line_end} "
            f"run {issue.run.index}: {issue.reference} {issue.detail}"
        )
    return 0 if result.passed else 1


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only, stateful TUFLOW BAT/command-list preflight.")
    parser.add_argument(
        "--tcf",
        type=Path,
        help="Python TCF constraint for ALL runs and fallback for bare rows (default: DEFAULT_TCF).",
    )
    parser.add_argument(
        "--runs",
        type=Path,
        action="append",
        help="BAT/CMD or text command list; repeat --runs for independent files.",
    )
    parser.add_argument(
        "--working-directory", type=Path, help="Initial batch launch directory (default: current directory)."
    )
    parser.add_argument(
        "--configuration-directory",
        type=Path,
        help="Base for relative --tcf (default: --working-directory if set, else wrapper folder).",
    )
    parser.add_argument(
        "--check-gpkg-layers",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable GeoPackage selected-layer checks (default: DEFAULT_CHECK_GPKG_LAYERS).",
    )
    parser.add_argument("--report", type=Path, help="NEW issue CSV path; existing files are not overwritten.")
    parser.add_argument("--plan-only", action="store_true", help="Extract runs without reading model inputs.")
    parser.add_argument("--plan-report", type=Path, help="Write a NEW private JSON run manifest.")
    parser.add_argument("--syntax", choices=("auto", "batch", "commands"), default="auto")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input encoding; decoding errors are not ignored.")
    parser.add_argument("--io-workers", type=int, default=DEFAULT_IO_WORKERS, help="Unique-file workers, 1-32.")
    parser.add_argument(
        "--maximum-directory-entries",
        type=int,
        default=DEFAULT_MAXIMUM_DIRECTORY_ENTRIES,
        help="Bounded file discovery entry limit.",
    )
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    options: argparse.Namespace = _parse_arguments()
    from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        code: int = main(
            tcf=options.tcf if options.tcf is not None else DEFAULT_TCF,
            commands=options.runs if options.runs is not None else DEFAULT_COMMANDS,
            report=options.report if options.report is not None else DEFAULT_REPORT,
            check_gpkg_layers=(
                options.check_gpkg_layers if options.check_gpkg_layers is not None else DEFAULT_CHECK_GPKG_LAYERS
            ),
            working_directory=options.working_directory,
            configuration_directory=options.configuration_directory,
            plan_only=options.plan_only,
            plan_report=options.plan_report,
            syntax=options.syntax,
            encoding=options.encoding,
            io_workers=options.io_workers,
            maximum_directory_entries=options.maximum_directory_entries,
        )
    except Exception as exc:  # noqa: BLE001 - human-facing wrapper reports incomplete work as failure
        print(f"Input preflight failed: {exc}")
        code = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not options.no_pause:
        pause_console()
    raise SystemExit(code)
