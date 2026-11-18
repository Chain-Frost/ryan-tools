r"""Audit selected TUFLOW rainfall and loss definitions without running TUFLOW.

Inputs are inert BAT/TXT run plans. PyTUFLOW resolves scenarios/events and
source databases. A BC-adjusted rainfall sum is NOT confirmed applied rainfall;
material/soil database rows are NOT confirmed active cell assignments.

Example:
    py -3.14 audit_tuflow_rainfall.py --runs runs.txt --json-report audit.json --no-pause
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-11-18.1"
RUN_FILES: list[Path] = []  # e.g. [Path('run_tuflow_simulations.txt')]
WORKING_DIR: Path | None = None
TCF: Path | None = None  # Optional fallback TCF for bare command rows.
INPUT_ENCODING = "utf-8-sig"
INPUT_SYNTAX = "auto"
JSON_REPORT: Path | None = None  # New exclusive-create JSON file; None prints only.
PAUSE_ON_FINISH = True

import argparse
import json
import os

from rich.console import Console
from rich.table import Table

from ryan_library.functions.tuflow.command_plan import read_command_plans
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.rainfall_input_audit import audit_command_plan, factor_groups


def _arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspect active TUFLOW rainfall and loss inputs; no solver execution.")
    p.add_argument("--runs", type=Path, action="append", help="Repeat BAT/CMD/TXT run-list files in source order.")
    p.add_argument("--working-directory", type=Path, help="Base for relative run-list and TCF paths.")
    p.add_argument("--tcf", type=Path, help="TCF for bare run rows; no default model guessing.")
    p.add_argument("--encoding", help="Override input file text encoding.")
    p.add_argument("--syntax", choices=("auto", "batch", "commands"))
    p.add_argument("--json-report", type=Path, help="Create new, never-overwritten full audit JSON.")
    p.add_argument("--no-json-report", action="store_true", help="Disable the editable JSON_REPORT.")
    p.add_argument("--no-pause", action="store_true", help="Suitable for automation.")
    return p.parse_args()


def _print_report(report: object) -> None:
    from ryan_library.orchestrators.tuflow.rainfall_input_audit import RainfallAudit

    assert isinstance(report, RainfallAudit)
    c = Console()
    summary = Table(title="TUFLOW rainfall and loss input audit — definitions, not executed results")
    for name in ("Run", "TCF / flags", "Rainfall BCs", "Materials", "Soils", "Findings"):
        summary.add_column(name)
    for run in report.runs:
        summary.add_row(
            str(run.run_index),
            f"{Path(run.tcf).name} " + " ".join(f"-{k} {v}" for k, v in run.flags),
            str(len(run.rainfall)),
            str(len(run.materials)),
            str(len(run.soils)),
            str(len(run.findings)),
        )
    c.print(summary)
    for run in report.runs:
        c.print(f"Run {run.run_index}: {run.source}")
        for rain in run.rainfall:
            c.print(
                f"  RF {rain.name}: {rain.source} [{rain.column}], "
                f"BC multiplier={rain.database_multiplier}, add={rain.database_addition}, "
                f"GIS f1={rain.gis_f1}, f2={rain.gis_f2}, "
                f"BC-adjusted sum={rain.bc_adjusted_sum_mm!r} mm; "
                "effective spatial rainfall=UNVERIFIED"
            )
        for material in run.materials:
            c.print(
                f"  Material {material.material_id} (defined only): IL={material.initial_loss_mm}, "
                f"CL={material.continuing_loss_mm_h}, impervious={material.fraction_impervious}, {material.status}"
            )
        for soil in run.soils:
            c.print(f"  Soil {soil.soil_id} (defined only): {soil.method} {dict(soil.parameters)}, {soil.status}")
        for finding in run.findings:
            c.print(f"  {finding.code}: {finding.detail}")
    factors = factor_groups(report)
    c.print(f"Factor-only groups (source event depths deliberately ignored): {len(factors)}")
    for number, (signature, runs) in enumerate(factors.items(), 1):
        c.print(f"  {number}: runs {runs}, selected RF factor definitions={signature}")
    c.print("No native TUFLOW simulation, spatial material/soil co-location, or effective rainfall was certified.")


def main(args: argparse.Namespace | None = None) -> int:
    options = args if args is not None else _arguments()
    base = (options.working_directory if options.working_directory is not None else WORKING_DIR or Path.cwd()).absolute()
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_source="TUFLOW-python/model_management/audit_tuflow_rainfall.py",
        wrapper_version=WRAPPER_VERSION,
        display_base=base,
    )
    try:
        files = options.runs if options.runs is not None else RUN_FILES
        if not files:
            raise ValueError("Select at least one BAT/TXT run list using RUN_FILES or --runs")
        sources = [p if p.is_absolute() else (base / p).absolute() for p in files]
        model = options.tcf if options.tcf is not None else TCF
        plan = read_command_plans(
            sources,
            initial_directory=base,
            configuration_directory=base,
            fallback_tcf=str(model) if model is not None else None,
            environment=dict(os.environ),
            syntax=options.syntax if options.syntax is not None else INPUT_SYNTAX,
            encoding=options.encoding if options.encoding is not None else INPUT_ENCODING,
        )
        report = audit_command_plan(plan)
        _print_report(report)
        path = None if options.no_json_report else (options.json_report if options.json_report is not None else JSON_REPORT)
        if path is not None:
            output = path if path.is_absolute() else (base / path).absolute()
            with output.open("x", encoding="utf-8") as file:
                json.dump(report.to_dict(), file, indent=2)
            print(f"Audit JSON: {output}")
        return 1 if any(run.findings for run in report.runs) else 0
    except Exception as exc:  # noqa: BLE001 - CLI must reject incomplete audits
        Console(stderr=True).print(f"Rainfall audit failed: {exc}")
        return 1
    finally:
        if PAUSE_ON_FINISH and not options.no_pause:
            pause_console()


if __name__ == "__main__":
    raise SystemExit(main())
