r"""Audit selected TUFLOW rainfall and loss definitions without running TUFLOW.

Inputs are inert BAT/TXT run plans. PyTUFLOW resolves scenarios/events and
source databases. A BC-adjusted rainfall sum is NOT confirmed applied rainfall;
material/soil database rows are NOT confirmed active cell assignments.

Example:
    py -3.14 audit_tuflow_rainfall.py --runs runs.txt --json-report audit.json --no-pause
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-11-18.2"
RUN_FILES: list[Path] = []  # e.g. [Path('run_tuflow_simulations.txt')]
WORKING_DIR: Path | None = None
TCF: Path | None = None  # Optional fallback TCF for bare command rows.
INPUT_ENCODING = "utf-8-sig"
INPUT_SYNTAX = "auto"
JSON_REPORT: Path | None = None  # New exclusive-create JSON file; None prints only.
# Optional CHECK-GRID co-location input; matched rasters alone cannot prove
# which TUFLOW run produced them. Supply GRID_RUN_INDEX for multi-run plans.
GRD_CHECK_GIS: Path | None = None  # Native TUFLOW _grd_check GPKG/SHP, alternative to raster pairs.
GRD_CHECK_LAYER: str | None = None  # Required only if GPKG check layer is ambiguous.
MATERIAL_CHECK_GRID: Path | None = None
SOIL_CHECK_GRID: Path | None = None
ACTIVE_CELL_MASK_GRID: Path | None = None
GRID_RUN_INDEX: int | None = None
PAUSE_ON_FINISH = True

import argparse
import json
import os
from dataclasses import asdict

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
    p.add_argument("--grd-check", type=Path, help="Native TUFLOW _grd_check GPKG/SHP with Material/Soil_ID/Code.")
    p.add_argument("--grd-check-layer", help="Named GPKG _grd_check layer, if ambiguous.")
    p.add_argument("--material-check-grid", type=Path, help="Native check-grid raster of material IDs.")
    p.add_argument("--soil-check-grid", type=Path, help="Aligned check-grid raster of soil IDs.")
    p.add_argument("--active-cell-mask-grid", type=Path, help="Optional aligned 1=active/0=inactive raster.")
    p.add_argument("--grid-run-index", type=int, help="Run index these check grids were obtained from.")
    p.add_argument("--no-pause", action="store_true", help="Suitable for automation.")
    return p.parse_args()


def _print_report(report: object) -> None:
    from ryan_library.orchestrators.tuflow.rainfall_input_audit import RainfallAudit

    if not isinstance(report, RainfallAudit):
        message = "Expected a RainfallAudit report"
        raise TypeError(message)
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


def main(args: argparse.Namespace | None = None) -> int:  # noqa: C901 - user-facing audit coordination
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
            message = "Select at least one BAT/TXT run list using RUN_FILES or --runs"
            raise ValueError(message)
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
        grd_source = options.grd_check if options.grd_check is not None else GRD_CHECK_GIS
        grd_layer = options.grd_check_layer if options.grd_check_layer is not None else GRD_CHECK_LAYER
        grid_material = options.material_check_grid if options.material_check_grid is not None else MATERIAL_CHECK_GRID
        grid_soil = options.soil_check_grid if options.soil_check_grid is not None else SOIL_CHECK_GRID
        grid_mask = options.active_cell_mask_grid if options.active_cell_mask_grid is not None else ACTIVE_CELL_MASK_GRID
        grid_index = options.grid_run_index if options.grid_run_index is not None else GRID_RUN_INDEX
        paired: dict[str, object] | None = None
        if grd_source is not None or grd_layer is not None or grid_material is not None or grid_soil is not None or grid_mask is not None or grid_index is not None:
            if grd_source is not None and any(item is not None for item in (grid_material, grid_soil, grid_mask)):
                message = "Choose either native --grd-check GIS or an aligned pair of raster checks"
                raise ValueError(message)
            if grd_source is None and grd_layer is not None:
                message = "--grd-check-layer requires --grd-check"
                raise ValueError(message)
            if grd_source is None and (grid_material is None or grid_soil is None):
                message = "Supply --grd-check or both material and soil check grids"
                raise ValueError(message)
            if grid_index is None:
                if len(report.runs) != 1:
                    message = "Explicit --grid-run-index required for multi-run check grids"
                    raise ValueError(message)
                grid_index = report.runs[0].run_index
            run = next((r for r in report.runs if r.run_index == grid_index), None)
            if run is None:
                message = f"Check-grid run index {grid_index} was not in the actual plan"
                raise ValueError(message)
            from ryan_library.functions.tuflow.loss_combinations import (
                group_observed_pairs,
                observed_pairs_from_check_grids,
                observed_pairs_from_grd_check,
            )

            def anchored(path: Path) -> Path:
                return path if path.is_absolute() else (base / path).absolute()

            if grd_source is not None:
                observed = observed_pairs_from_grd_check(anchored(grd_source), layer=grd_layer)
                check_source = "native-grd-check-GIS"
            else:
                if grid_material is None or grid_soil is None:
                    message = "Paired raster source was not selected"
                    raise ValueError(message)
                observed = observed_pairs_from_check_grids(
                    anchored(grid_material),
                    anchored(grid_soil),
                    active_mask_grid=anchored(grid_mask) if grid_mask is not None else None,
                )
                check_source = "paired-raster-check-grids"
            soil_definitions = {soil.soil_id: soil for soil in run.soils}
            if any(pair.soil_id == -1 for pair in observed):
                from ryan_library.orchestrators.tuflow.rainfall_input_audit import SoilDefinition

                soil_definitions[-1] = SoilDefinition(-1, "NONE", (), "defined")
            groups = group_observed_pairs(
                observed,
                {m.material_id: m for m in run.materials},
                soil_definitions,
            )
            Console().print(
                f"Observed check-grid pairs: {len(observed)}, effective loss groups: {len(groups)} "
                f"(run {grid_index}; check-grid source/run provenance NOT independently established)"
            )
            for group in groups:
                Console().print(
                    f"  {group.category}: {group.cell_count} cells, "
                    f"materials={group.material_ids}, soils={group.soil_ids}, loss signature={group.signature}"
                )
            paired = {
                "run_index": grid_index,
                "source_kind": check_source,
                "grd_check_gis": str(anchored(grd_source)) if grd_source is not None else None,
                "grd_check_layer": grd_layer,
                "material_grid": str(anchored(grid_material)) if grid_material is not None else None,
                "soil_grid": str(anchored(grid_soil)) if grid_soil is not None else None,
                "active_mask_grid": str(anchored(grid_mask)) if grid_mask is not None else None,
                "spatial_pairing": "raster alignment and cell co-location established",
                "run_provenance": "externally supplied / not independently established",
                "observed_pairs": [asdict(pair) for pair in observed],
                "effective_loss_groups": [asdict(group) for group in groups],
            }
        path = None if options.no_json_report else (options.json_report if options.json_report is not None else JSON_REPORT)
        if path is not None:
            output = path if path.is_absolute() else (base / path).absolute()
            document = report.to_dict()
            if paired is not None:
                document["paired_check_grids"] = paired
            with output.open("x", encoding="utf-8") as file:
                json.dump(document, file, indent=2)
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
