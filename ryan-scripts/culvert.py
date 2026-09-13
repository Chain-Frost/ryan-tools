r"""Run maintained culvert solve, analyse, design, and rating workflows.

The project file is JSON. Fixed tailwater elevations are supported by JSON configuration;
Python callers can pass any public ``culvert_solver.TailwaterBoundary`` through ``Scenario``.

Minimal project example::

    {
      "name": "Demo",
      "crossings": [{
        "name": "Crossing A",
        "groups": [{
          "name": "Pipes",
          "quantity": 2,
          "barrel": {
            "shape": "circular",
            "diameter_mm": 1200,
            "length": 40,
            "inlet_invert": 10.0,
            "outlet_invert": 9.5,
            "roughness": 0.013,
            "material": "concrete_pipe"
          }
        }]
      }],
      "scenarios": [{
        "name": "Design",
        "discharge": 4.0,
        "tailwater_elevation": 10.0
      }]
    }

Examples::

    python culvert.py analyse --project "D:\Project\culvert_project.json" --no-pause
    python culvert.py solve --project culvert_project.json --crossing "Crossing A" --scenario Design
    python culvert.py design --project culvert_project.json --diameters-mm 900 1200 1500 --quantities 1 2 3 \
        --max-headwater-elevation 11.5
    python culvert.py rating --project culvert_project.json --min-discharge 0.5 --max-discharge 8 --points 16
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-09-13.1"

WORKING_DIR: Path = Path(__file__).resolve().parent
DEFAULT_PROJECT_FILE = Path("culvert_project.json")
DEFAULT_OUTPUT_DIRECTORY = Path("culvert_results")
DEFAULT_RATING_POINTS = 11
CONSOLE_LOG_LEVEL = "INFO"

import argparse
from collections.abc import Sequence

from culvert_solver import generate_discharge_range
from loguru import logger

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    DesignCriteria,
    RectangularBarrelDefinition,
    Scenario,
)
from ryan_library.functions.culvert.candidate_generation import (
    generate_circular_candidates,
    generate_rectangular_candidates,
)
from ryan_library.functions.culvert.config import load_project_json
from ryan_library.functions.culvert.export import (
    export_design_result_json,
    export_scenario_results_csv,
    export_scenario_results_json,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.culvert.analyse import analyse_project
from ryan_library.orchestrators.culvert.design import design_crossing
from ryan_library.orchestrators.culvert.rating import generate_crossing_rating
from ryan_library.orchestrators.culvert.report import render_design_markdown, render_scenario_markdown
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def _select_named[T](items: Sequence[T], name: str | None, *, get_name: callable[[T], str]) -> T:
    if not items:
        raise ValueError("No selectable items are available.")
    if name is None:
        return items[0]
    for item in items:
        if get_name(item) == name:
            return item
    available = ", ".join(get_name(item) for item in items)
    raise ValueError(f"Unknown name {name!r}. Available: {available}")


def _output_path(base: Path, configured: Path | None) -> Path:
    output = configured or DEFAULT_OUTPUT_DIRECTORY
    return output.resolve() if output.is_absolute() else (base / output).resolve()


def _write_scenario_outputs(results: Sequence, output_directory: Path) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    export_scenario_results_json(results, output_directory / "scenario_results.json")
    export_scenario_results_csv(results, output_directory / "scenario_results.csv")
    (output_directory / "scenario_results.md").write_text(render_scenario_markdown(results), encoding="utf-8")


def main(
    *,
    command: str,
    working_directory: Path | None = None,
    project_file: Path | None = None,
    output_directory: Path | None = None,
    crossing_name: str | None = None,
    scenario_name: str | None = None,
    diameters_mm: tuple[float, ...] | None = None,
    spans_mm: tuple[float, ...] | None = None,
    rises_mm: tuple[float, ...] | None = None,
    quantities: tuple[int, ...] | None = None,
    maximum_headwater_elevation: float | None = None,
    maximum_headwater_depth: float | None = None,
    maximum_outlet_velocity: float | None = None,
    maximum_roadway_discharge: float | None = None,
    minimum_rating_discharge: float | None = None,
    maximum_rating_discharge: float | None = None,
    rating_points: int | None = None,
    console_log_level: str | None = None,
) -> int:
    """Resolve wrapper settings and execute the requested shared culvert workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    configured_project = project_file or DEFAULT_PROJECT_FILE
    project_path = configured_project.resolve() if configured_project.is_absolute() else target_directory / configured_project
    resolved_output = _output_path(target_directory, output_directory)
    if not change_working_directory(target_dir=target_directory):
        return 1

    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            project = load_project_json(project_path)
            crossing: CrossingDefinition = _select_named(
                project.crossings,
                crossing_name,
                get_name=lambda item: item.name,
            )
            scenario: Scenario = _select_named(
                project.scenarios,
                scenario_name,
                get_name=lambda item: item.name,
            )

            if command == "solve":
                results = (solve_crossing_scenario(crossing, scenario),)
                _write_scenario_outputs(results, resolved_output)
                print(render_scenario_markdown(results))
            elif command == "analyse":
                results = analyse_project(project)
                _write_scenario_outputs(results, resolved_output)
                print(render_scenario_markdown(results))
            elif command == "design":
                if all(
                    limit is None
                    for limit in (
                        maximum_headwater_elevation,
                        maximum_headwater_depth,
                        maximum_outlet_velocity,
                        maximum_roadway_discharge,
                    )
                ):
                    raise ValueError("design requires at least one explicit hydraulic design criterion.")
                if len(crossing.groups) != 1:
                    raise ValueError("CLI candidate generation currently requires a single-group crossing.")
                template_group = crossing.groups[0]
                design_quantities = quantities or (template_group.quantity,)
                if isinstance(template_group.barrel, CircularBarrelDefinition):
                    design_diameters = diameters_mm or (template_group.barrel.diameter_mm,)
                    candidates = generate_circular_candidates(
                        crossing,
                        diameters_mm=design_diameters,
                        quantities=design_quantities,
                    )
                elif isinstance(template_group.barrel, RectangularBarrelDefinition):
                    design_spans = spans_mm or (template_group.barrel.span_mm,)
                    design_rises = rises_mm or (template_group.barrel.rise_mm,)
                    candidates = generate_rectangular_candidates(
                        crossing,
                        spans_mm=design_spans,
                        rises_mm=design_rises,
                        quantities=design_quantities,
                    )
                else:
                    raise TypeError("Unsupported barrel definition for candidate generation.")
                criteria = DesignCriteria(
                    maximum_headwater_elevation=maximum_headwater_elevation,
                    maximum_headwater_depth=maximum_headwater_depth,
                    maximum_outlet_velocity=maximum_outlet_velocity,
                    maximum_roadway_discharge=maximum_roadway_discharge,
                )
                result = design_crossing(candidates, project.scenarios, criteria)
                resolved_output.mkdir(parents=True, exist_ok=True)
                export_design_result_json(result, resolved_output / "design_results.json")
                markdown = render_design_markdown(result)
                (resolved_output / "design_results.md").write_text(markdown, encoding="utf-8")
                print(markdown)
            elif command == "rating":
                q_min = minimum_rating_discharge or max(0.01, scenario.discharge / 10.0)
                q_max = maximum_rating_discharge or scenario.discharge
                discharges = generate_discharge_range(q_min, q_max, rating_points or DEFAULT_RATING_POINTS)
                result = generate_crossing_rating(crossing, discharges, scenario.tailwater)
                resolved_output.mkdir(parents=True, exist_ok=True)
                lines = [
                    "discharge_m3s,headwater_elevation_m,tailwater_elevation_m,outlet_velocity_ms,status"
                ]
                lines.extend(
                    f"{point.discharge},{point.headwater_elevation},{point.tailwater_elevation},"
                    f"{point.outlet_velocity},{point.status.value}"
                    for point in result.rating_curve.points
                )
                (resolved_output / "rating_curve.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
                print(f"Wrote {len(result.rating_curve.points)} rating points to {resolved_output / 'rating_curve.csv'}")
            else:
                raise ValueError(f"Unsupported command: {command}")
            logger.success("Culvert workflow completed; outputs: {}", resolved_output)
        except Exception:
            logger.exception("Culvert workflow failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run culvert solve, analysis, design-search, or rating workflows using ryan-culverts hydraulics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=("solve", "analyse", "design", "rating"))
    parser.add_argument("--project", type=Path, help="Project JSON; default: culvert_project.json.")
    parser.add_argument("--directory", type=Path, help="Working directory; default: wrapper directory.")
    parser.add_argument("--output-directory", type=Path, help="Output directory; default: culvert_results.")
    parser.add_argument("--crossing", help="Named crossing; default: first project crossing.")
    parser.add_argument("--scenario", help="Named scenario; default: first project scenario.")
    parser.add_argument("--diameters-mm", type=float, nargs="+")
    parser.add_argument("--spans-mm", type=float, nargs="+")
    parser.add_argument("--rises-mm", type=float, nargs="+")
    parser.add_argument("--quantities", type=int, nargs="+")
    parser.add_argument("--max-headwater-elevation", type=float)
    parser.add_argument("--max-headwater-depth", type=float)
    parser.add_argument("--max-outlet-velocity", type=float)
    parser.add_argument("--max-roadway-discharge", type=float)
    parser.add_argument("--min-discharge", type=float)
    parser.add_argument("--max-discharge", type=float)
    parser.add_argument("--points", type=int)
    parser.add_argument("--console-log-level")
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        command=args.command,
        working_directory=args.directory,
        project_file=args.project,
        output_directory=args.output_directory,
        crossing_name=args.crossing,
        scenario_name=args.scenario,
        diameters_mm=None if args.diameters_mm is None else tuple(args.diameters_mm),
        spans_mm=None if args.spans_mm is None else tuple(args.spans_mm),
        rises_mm=None if args.rises_mm is None else tuple(args.rises_mm),
        quantities=None if args.quantities is None else tuple(args.quantities),
        maximum_headwater_elevation=args.max_headwater_elevation,
        maximum_headwater_depth=args.max_headwater_depth,
        maximum_outlet_velocity=args.max_outlet_velocity,
        maximum_roadway_discharge=args.max_roadway_discharge,
        minimum_rating_discharge=args.min_discharge,
        maximum_rating_discharge=args.max_discharge,
        rating_points=args.points,
        console_log_level=args.console_log_level,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
