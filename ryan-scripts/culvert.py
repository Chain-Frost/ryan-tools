r"""Run maintained culvert solve, analyse, design, and rating workflows.

The versioned project file may be JSON or TOML and supports fixed-elevation and
Manning-channel tailwater definitions.

Minimal project example::

    {
      "schema_version": 1,
      "name": "Demo",
      "crossings": [{
        "name": "Crossing A",
        "groups": [{
          "name": "Pipes",
          "quantity": 2,
          "barrel": {
            "shape": "circular",
            "diameter_mm": 1200,
            "length_m": 40,
            "inlet_invert_elevation_m": 10.0,
            "outlet_invert_elevation_m": 9.5,
            "roughness_manning_n": 0.013,
            "material": "concrete_pipe"
          }
        }]
      }],
      "scenarios": [{
        "name": "Design",
        "discharge_m3s": 4.0,
        "tailwater": {"type": "fixed", "elevation_m": 10.0}
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

import json
from dataclasses import replace
from pathlib import Path
from typing import cast

WRAPPER_VERSION = "2026-09-14.1"

WORKING_DIR: Path = Path(__file__).resolve().parent
DEFAULT_PROJECT_FILE = Path("culvert_project.json")
DEFAULT_OUTPUT_DIRECTORY = Path("culvert_results")
DEFAULT_RATING_POINTS = 11
CONSOLE_LOG_LEVEL = "INFO"

import argparse
from collections.abc import Callable, Sequence

from culvert_solver import generate_discharge_range
from loguru import logger

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    DesignCriteria,
    RectangularBarrelDefinition,
    Scenario,
    ScenarioResult,
)
from ryan_library.functions.culvert.candidate_generation import (
    generate_circular_candidates,
    generate_rectangular_candidates,
)
from ryan_library.functions.culvert.config import load_project
from ryan_library.functions.culvert.event_import import load_event_csv
from ryan_library.functions.culvert.export import (
    export_crossing_rating_csv,
    export_crossing_rating_json,
    export_design_result_json,
    export_scenario_results_csv,
    export_scenario_results_json,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.culvert.analyse import analyse_project
from ryan_library.orchestrators.culvert.design import design_crossing
from ryan_library.orchestrators.culvert.events import materialize_event_scenarios
from ryan_library.orchestrators.culvert.rating import generate_crossing_rating
from ryan_library.orchestrators.culvert.report import (
    render_design_markdown,
    render_scenario_markdown,
    render_scenario_records_markdown,
)
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def _select_named[T](items: Sequence[T], name: str | None, *, get_name: Callable[[T], str]) -> T:
    if not items:
        msg = "No selectable items are available."
        raise ValueError(msg)
    if name is None:
        return items[0]
    for item in items:
        if get_name(item) == name:
            return item
    available = ", ".join(get_name(item) for item in items)
    msg = f"Unknown name {name!r}. Available: {available}"
    raise ValueError(msg)


def _output_path(base: Path, configured: Path | None) -> Path:
    output = configured or DEFAULT_OUTPUT_DIRECTORY
    return output.resolve() if output.is_absolute() else (base / output).resolve()


def _write_scenario_outputs(results: Sequence[ScenarioResult], output_directory: Path) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    export_scenario_results_json(results, output_directory / "scenario_results.json")
    export_scenario_results_csv(results, output_directory / "scenario_results.csv")
    (output_directory / "scenario_results.md").write_text(render_scenario_markdown(results), encoding="utf-8")


def _render_saved_report(output_directory: Path) -> None:
    source = output_directory / "scenario_results.json"
    payload = cast(object, json.loads(source.read_text(encoding="utf-8")))
    if not isinstance(payload, list):
        msg = f"{source} must contain an array of scenario-result objects."
        raise ValueError(msg)
    records: list[dict[str, object]] = []
    for item in cast("list[object]", payload):
        if not isinstance(item, dict):
            msg = f"{source} must contain only scenario-result objects."
            raise ValueError(msg)
        records.append(cast("dict[str, object]", item))
    markdown = render_scenario_records_markdown(records)
    target = output_directory / "scenario_results.md"
    target.write_text(markdown, encoding="utf-8")
    print(markdown)


def _run_design(
    *,
    crossing: CrossingDefinition,
    scenarios: Sequence[Scenario],
    output_directory: Path,
    diameters_mm: tuple[float, ...] | None,
    spans_mm: tuple[float, ...] | None,
    rises_mm: tuple[float, ...] | None,
    quantities: tuple[int, ...] | None,
    maximum_headwater_elevation: float | None,
    maximum_headwater_depth: float | None,
    maximum_outlet_velocity: float | None,
    maximum_roadway_discharge: float | None,
    configured_criteria: DesignCriteria | None,
) -> None:
    if maximum_headwater_elevation is None and configured_criteria is not None:
        maximum_headwater_elevation = configured_criteria.maximum_headwater_elevation
    if maximum_headwater_depth is None and configured_criteria is not None:
        maximum_headwater_depth = configured_criteria.maximum_headwater_depth
    if maximum_outlet_velocity is None and configured_criteria is not None:
        maximum_outlet_velocity = configured_criteria.maximum_outlet_velocity
    maximum_roadway_discharge = (
        maximum_roadway_discharge
        if maximum_roadway_discharge is not None
        else None
        if configured_criteria is None
        else configured_criteria.maximum_roadway_discharge
    )
    if configured_criteria is None and all(
        limit is None
        for limit in (
            maximum_headwater_elevation,
            maximum_headwater_depth,
            maximum_outlet_velocity,
            maximum_roadway_discharge,
        )
    ):
        msg = "design requires at least one explicit hydraulic design criterion."
        raise ValueError(msg)
    if len(crossing.groups) != 1:
        msg = "CLI candidate generation currently requires a single-group crossing."
        raise ValueError(msg)

    template_group = crossing.groups[0]
    design_quantities = quantities or (template_group.quantity,)
    template_barrel: object = template_group.barrel
    if isinstance(template_barrel, CircularBarrelDefinition):
        candidates = generate_circular_candidates(
            crossing,
            diameters_mm=diameters_mm or (template_barrel.diameter_mm,),
            quantities=design_quantities,
        )
    elif isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
        template_barrel, RectangularBarrelDefinition
    ):
        candidates = generate_rectangular_candidates(
            crossing,
            spans_mm=spans_mm or (template_barrel.span_mm,),
            rises_mm=rises_mm or (template_barrel.rise_mm,),
            quantities=design_quantities,
        )
    else:
        msg = "Unsupported barrel definition for candidate generation."
        raise TypeError(msg)

    criteria = DesignCriteria(
        maximum_headwater_elevation=maximum_headwater_elevation,
        maximum_headwater_depth=maximum_headwater_depth,
        maximum_headwater_ratio=(None if configured_criteria is None else configured_criteria.maximum_headwater_ratio),
        minimum_freeboard=None if configured_criteria is None else configured_criteria.minimum_freeboard,
        maximum_outlet_velocity=maximum_outlet_velocity,
        maximum_roadway_discharge=maximum_roadway_discharge,
        maximum_barrel_count=(None if configured_criteria is None else configured_criteria.maximum_barrel_count),
        maximum_total_structure_width=(
            None if configured_criteria is None else configured_criteria.maximum_total_structure_width
        ),
        require_resolved_result=(True if configured_criteria is None else configured_criteria.require_resolved_result),
    )
    result = design_crossing(candidates, scenarios, criteria)
    output_directory.mkdir(parents=True, exist_ok=True)
    export_design_result_json(result, output_directory / "design_results.json", criteria=criteria)
    markdown = render_design_markdown(result)
    (output_directory / "design_results.md").write_text(markdown, encoding="utf-8")
    print(markdown)


def _run_rating(
    *,
    crossing: CrossingDefinition,
    scenario: Scenario,
    output_directory: Path,
    minimum_discharge: float | None,
    maximum_discharge: float | None,
    points: int | None,
) -> None:
    q_min = minimum_discharge or max(0.01, scenario.discharge / 10.0)
    q_max = maximum_discharge or scenario.discharge
    discharges = generate_discharge_range(q_min, q_max, points or DEFAULT_RATING_POINTS)
    result = generate_crossing_rating(crossing, discharges, scenario.tailwater)
    output_directory.mkdir(parents=True, exist_ok=True)
    rating_path = output_directory / "rating_curve.csv"
    export_crossing_rating_csv(result, rating_path)
    export_crossing_rating_json(result, output_directory / "rating_curve.json")
    print(f"Wrote {len(result.rating_curve.points)} rating points to {rating_path}")


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
    event_file: Path | None = None,
) -> int:
    """Resolve wrapper settings and execute the requested shared culvert workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    configured_project = project_file or DEFAULT_PROJECT_FILE
    project_path = (
        configured_project.resolve()
        if configured_project.is_absolute()
        else (target_directory / configured_project).resolve()
    )
    resolved_output = _output_path(target_directory, output_directory)
    if not change_working_directory(target_dir=target_directory):
        return 1

    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            if command == "report":
                _render_saved_report(resolved_output)
                logger.success("Culvert report completed; outputs: {}", resolved_output)
                return 0
            project = load_project(project_path)
            crossing: CrossingDefinition = _select_named(
                project.crossings,
                crossing_name,
                get_name=lambda item: item.name,
            )
            if event_file is not None:
                event_path = event_file if event_file.is_absolute() else target_directory / event_file
                events = load_event_csv(event_path)
                project = replace(
                    project,
                    scenarios=materialize_event_scenarios(
                        events,
                        crossing,
                        default_tailwater=project.scenarios[0].tailwater,
                    ),
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
            elif command in {"analyse", "compare"}:
                results = analyse_project(project)
                _write_scenario_outputs(results, resolved_output)
                print(render_scenario_markdown(results))
            elif command == "design":
                _run_design(
                    crossing=crossing,
                    scenarios=project.scenarios,
                    output_directory=resolved_output,
                    diameters_mm=diameters_mm,
                    spans_mm=spans_mm,
                    rises_mm=rises_mm,
                    quantities=quantities,
                    maximum_headwater_elevation=maximum_headwater_elevation,
                    maximum_headwater_depth=maximum_headwater_depth,
                    maximum_outlet_velocity=maximum_outlet_velocity,
                    maximum_roadway_discharge=maximum_roadway_discharge,
                    configured_criteria=project.design_criteria,
                )
            elif command == "rating":
                _run_rating(
                    crossing=crossing,
                    scenario=scenario,
                    output_directory=resolved_output,
                    minimum_discharge=minimum_rating_discharge,
                    maximum_discharge=maximum_rating_discharge,
                    points=rating_points,
                )
            else:
                msg = f"Unsupported command: {command}"
                raise ValueError(msg)
            logger.success("Culvert workflow completed; outputs: {}", resolved_output)
        except OSError, ValueError, RuntimeError:
            logger.exception("Culvert workflow failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run culvert solve, analysis, design-search, or rating workflows using ryan-culverts hydraulics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=("solve", "analyse", "compare", "design", "rating", "report"))
    parser.add_argument("--project", type=Path, help="Versioned project JSON or TOML; default: culvert_project.json.")
    parser.add_argument("--directory", type=Path, help="Working directory; default: wrapper directory.")
    parser.add_argument("--output-directory", type=Path, help="Output directory; default: culvert_results.")
    parser.add_argument("--crossing", help="Named crossing; default: first project crossing.")
    parser.add_argument("--scenario", help="Named scenario; default: first project scenario.")
    parser.add_argument(
        "--events-csv",
        type=Path,
        help="Replace project scenarios with imported flow/headwater targets; no hydrology is performed.",
    )
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
        event_file=args.events_csv,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
