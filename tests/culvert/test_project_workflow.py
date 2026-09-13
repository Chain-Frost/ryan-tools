"""Project-level culvert workflow and machine-readable export tests."""

import json
from pathlib import Path

from ryan_library.classes.culvert import (
    Alternative,
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    CulvertProject,
    Scenario,
)
from ryan_library.functions.culvert.export import export_scenario_results_csv, export_scenario_results_json
from ryan_library.orchestrators.culvert.analyse import analyse_project


def _crossing(name: str, diameter_mm: float) -> CrossingDefinition:
    return CrossingDefinition(
        name=name,
        groups=(
            CulvertGroupDefinition(
                name="Pipes",
                quantity=2,
                barrel=CircularBarrelDefinition(
                    diameter_mm=diameter_mm,
                    length=40.0,
                    inlet_invert=10.0,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )


def test_project_analysis_evaluates_scenarios_and_alternatives(tmp_path: Path) -> None:
    project = CulvertProject(
        name="Demo",
        crossings=(_crossing("Existing", 1200.0),),
        scenarios=(
            Scenario(name="Minor", discharge=2.0, tailwater=9.7),
            Scenario(name="Major", discharge=4.0, tailwater=10.0),
        ),
        alternatives=(Alternative(name="Upgrade", crossing=_crossing("Proposed", 1500.0)),),
    )

    results = analyse_project(project)

    assert len(results) == 4
    assert {result.alternative_name for result in results} == {None, "Upgrade"}

    json_path = export_scenario_results_json(results, tmp_path / "results.json")
    csv_path = export_scenario_results_csv(results, tmp_path / "results.csv")
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    assert len(payload) == 4
    assert csv_path.read_text(encoding="utf-8").startswith("alternative,crossing,scenario,")
