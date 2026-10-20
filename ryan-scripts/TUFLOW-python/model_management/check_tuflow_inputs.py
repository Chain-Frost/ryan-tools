"""Check every required TUFLOW input for a proposed set of runs, without running TUFLOW.

Configure DEFAULT_TCF and DEFAULT_COMMANDS near the top, or override them:
    python check_tuflow_inputs.py --tcf "runs\model_~e1~_~e2~.tcf" --runs run_tuflow_simulations_commands.txt --no-pause

Requires installed ryan-tools and optional PyTUFLOW (pip install pytuflow).
The commands file should come from run_tuflow_simulations.py and contain the
exact -e* and -s* combinations planned for execution.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-21.1"
DEFAULT_TCF = Path("runs/model_~e1~_~e2~.tcf")
DEFAULT_COMMANDS = Path("run_tuflow_simulations_commands.txt")
DEFAULT_REPORT = Path("tuflow_input_preflight.csv")
DEFAULT_CHECK_GPKG_LAYERS = True

import argparse

from ryan_library.functions.tuflow.run_flags import read_run_commands
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs


def main(
    *,
    tcf: Path,
    commands: Path,
    report: Path,
    check_gpkg_layers: bool,
) -> int:
    """Run read-only preflight and write one issue register."""
    tcf = tcf.absolute()
    commands = commands.absolute()
    combinations = read_run_commands(commands)
    requests = [RunRequest(index=index, flags=flags) for index, flags in enumerate(combinations, start=1)]
    result = check_simulation_inputs(tcf, requests, check_gpkg_layers=check_gpkg_layers)
    output = result.write_csv(report)
    print(
        f"Preflight: {result.run_count} run(s), {len(result.checked_paths)} unique input path(s), "
        f"{len(result.issues)} issue(s), {result.affected_runs} affected run(s)"
    )
    print(f"Issues CSV: {output}")
    for issue in result.issues[:20]:
        print(f"[{issue.status.upper()}] run {issue.run_index} {issue.origin}: {issue.reference} {issue.detail}")
    if len(result.issues) > 20:
        print(f"Further issues are recorded in the CSV ({len(result.issues) - 20} more).")
    return 0 if result.passed else 1


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check inputs for every TUFLOW run in the exported launcher commands without executing TUFLOW."
    )
    parser.add_argument("--tcf", type=Path, help="TCF template (default: DEFAULT_TCF)")
    parser.add_argument("--runs", type=Path, help="Launcher-exported commands file (default: DEFAULT_COMMANDS)")
    parser.add_argument("--report", type=Path, help="Output issue CSV (default: DEFAULT_REPORT)")
    parser.add_argument(
        "--check-gpkg-layers",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Also verify referenced GeoPackage layer names (default: enabled).",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause after completion.")
    return parser.parse_args()


if __name__ == "__main__":
    options = _parse_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        code = main(
            tcf=options.tcf if options.tcf is not None else DEFAULT_TCF,
            commands=options.runs if options.runs is not None else DEFAULT_COMMANDS,
            report=options.report if options.report is not None else DEFAULT_REPORT,
            check_gpkg_layers=(
                options.check_gpkg_layers
                if options.check_gpkg_layers is not None
                else DEFAULT_CHECK_GPKG_LAYERS
            ),
        )
    except Exception as exc:  # noqa: BLE001 - human-facing wrapper error boundary
        print(f"Input preflight failed: {exc}")
        code = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not options.no_pause:
        pause_console()
    raise SystemExit(code)
