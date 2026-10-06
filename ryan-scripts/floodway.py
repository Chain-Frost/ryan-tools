r"""Run the maintained floodway overtopping design-assessment workflow.

The hydraulic crossing/project file remains the normal culvert JSON/TOML schema.
Floodway formation/design inputs are supplied separately so formation response
does not become part of the authoritative crossing-hydraulics schema.

Minimal formation JSON example::

    {
      "schema_version": 1,
      "name": "Floodway formation",
      "crest_flow_length_m": 9.0,
      "zones": [
        {"zone": "D", "slope": 0.03, "roughness_manning_n": 0.015},
        {"zone": "C", "elevation_m": 10.0},
        {
          "zone": "B",
          "slope": 0.3333333333,
          "roughness_manning_n": 0.04,
          "hec23_riprap": {
            "selected_d50_m": 0.15,
            "uniformity_coefficient": 2.1,
            "porosity": 0.45
          }
        }
      ]
    }

Examples::

    python floodway.py --project culvert_project.json --formation floodway_formation.json --no-pause
    python floodway.py --project culvert_project.toml --formation floodway.toml --crossing "Floodway A" --no-pause
    python floodway.py --project culvert_project.json --formation floodway_formation.json --scenario "1% AEP"
    python floodway.py --project culvert_project.json --formation floodway_formation.json --sweep-points 31 --no-pause
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-06.1"

WORKING_DIR: Path = Path(__file__).resolve().parent
DEFAULT_PROJECT_FILE = Path("culvert_project.json")
DEFAULT_FORMATION_FILE = Path("floodway_formation.json")
DEFAULT_OUTPUT_DIRECTORY = Path("floodway_results")
DEFAULT_SWEEP_POINTS = 21
CONSOLE_LOG_LEVEL = "INFO"

import argparse
from collections.abc import Callable, Sequence

from loguru import logger

from ryan_library.classes.culvert import CrossingDefinition, Scenario
from ryan_library.classes.floodway import FloodwayEventEnvelope, FloodwayScenarioAssessment
from ryan_library.functions.culvert.config import load_project
from ryan_library.functions.floodway import (
    export_floodway_envelope_json,
    export_floodway_governors_csv,
    export_floodway_scenarios_json,
    load_floodway_formation,
)
from ryan_library.functions.loguru_helpers import normalize_log_level, setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.floodway import (
    assess_floodway_crossing,
    assess_floodway_discharge_sweep,
    build_floodway_envelope_from_assessments,
    render_floodway_envelope_markdown,
    render_floodway_scenario_markdown,
)


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


def _resolve_path(base: Path, configured: Path) -> Path:
    return configured.resolve() if configured.is_absolute() else (base / configured).resolve()


def _output_path(base: Path, configured: Path | None) -> Path:
    output = configured or DEFAULT_OUTPUT_DIRECTORY
    return output.resolve() if output.is_absolute() else (base / output).resolve()


def _write_outputs(
    *,
    assessments: Sequence[FloodwayScenarioAssessment],
    envelope: FloodwayEventEnvelope,
    output_directory: Path,
) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    export_floodway_scenarios_json(assessments, output_directory / "floodway_scenarios.json")
    export_floodway_envelope_json(envelope, output_directory / "floodway_envelope.json")
    export_floodway_governors_csv(envelope, output_directory / "floodway_governors.csv")
    (output_directory / "floodway_envelope.md").write_text(
        render_floodway_envelope_markdown(envelope),
        encoding="utf-8",
    )
    scenario_markdown = "\n\n---\n\n".join(
        render_floodway_scenario_markdown(item) for item in assessments
    )
    (output_directory / "floodway_scenarios.md").write_text(
        scenario_markdown + ("\n" if scenario_markdown else ""),
        encoding="utf-8",
    )


def main(
    *,
    working_directory: Path | None = None,
    project_file: Path | None = None,
    formation_file: Path | None = None,
    output_directory: Path | None = None,
    crossing_name: str | None = None,
    scenario_name: str | None = None,
    sweep_scenario_name: str | None = None,
    sweep_points: int | None = None,
    console_log_level: str | None = None,
) -> int:
    """Resolve wrapper settings and execute the floodway assessment workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    project_path = _resolve_path(target_directory, project_file or DEFAULT_PROJECT_FILE)
    formation_path = _resolve_path(target_directory, formation_file or DEFAULT_FORMATION_FILE)
    resolved_output = _output_path(target_directory, output_directory)
    if not change_working_directory(target_dir=target_directory):
        return 1

    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            project = load_project(project_path)
            formation = load_floodway_formation(formation_path)
            crossing: CrossingDefinition = _select_named(
                project.crossings,
                crossing_name,
                get_name=lambda item: item.name,
            )
            if crossing.roadway is None:
                msg = f"Crossing {crossing.name!r} does not define roadway overtopping geometry."
                raise ValueError(msg)

            if scenario_name is None:
                scenarios: tuple[Scenario, ...] = project.scenarios
            else:
                scenarios = (
                    _select_named(
                        project.scenarios,
                        scenario_name,
                        get_name=lambda item: item.name,
                    ),
                )

            assessments = assess_floodway_crossing(crossing, scenarios, formation)

            resolved_sweep_points = DEFAULT_SWEEP_POINTS if sweep_points is None else sweep_points
            if resolved_sweep_points < 0 or resolved_sweep_points == 1:
                msg = "sweep_points must be 0 to disable the sweep or at least 2."
                raise ValueError(msg)
            if resolved_sweep_points >= 2:
                sweep_base = (
                    max(scenarios, key=lambda item: item.discharge)
                    if sweep_scenario_name is None
                    else _select_named(
                        project.scenarios,
                        sweep_scenario_name,
                        get_name=lambda item: item.name,
                    )
                )
                sweep_assessments = assess_floodway_discharge_sweep(
                    crossing,
                    sweep_base,
                    formation,
                    points=resolved_sweep_points,
                )
                assessments = assessments + sweep_assessments

            envelope = build_floodway_envelope_from_assessments(assessments)
            _write_outputs(
                assessments=assessments,
                envelope=envelope,
                output_directory=resolved_output,
            )
            print(render_floodway_envelope_markdown(envelope))
            logger.success("Floodway assessment completed; outputs: {}", resolved_output)
        except (OSError, TypeError, ValueError, RuntimeError):
            logger.exception("Floodway assessment failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assess overtopping floodway formation response using ryan-culverts crossing hydraulics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project", type=Path, help="Culvert project JSON/TOML; default: culvert_project.json.")
    parser.add_argument(
        "--formation",
        type=Path,
        help="Floodway formation JSON/TOML; default: floodway_formation.json.",
    )
    parser.add_argument("--directory", type=Path, help="Working directory; default: wrapper directory.")
    parser.add_argument("--output-directory", type=Path, help="Output directory; default: floodway_results.")
    parser.add_argument("--crossing", help="Named crossing; default: first project crossing.")
    parser.add_argument("--scenario", help="Named scenario; default: assess every project scenario.")
    parser.add_argument(
        "--sweep-scenario",
        help="Scenario providing the downstream boundary for the interior discharge sweep; default: highest-Q selected event.",
    )
    parser.add_argument(
        "--sweep-points",
        type=int,
        help="Interior discharge sweep points; default: 21. Use 0 to disable.",
    )
    parser.add_argument("--console-log-level", type=normalize_log_level)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.directory,
        project_file=args.project,
        formation_file=args.formation,
        output_directory=args.output_directory,
        crossing_name=args.crossing,
        scenario_name=args.scenario,
        sweep_scenario_name=args.sweep_scenario,
        sweep_points=args.sweep_points,
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
