"""Imported flow and headwater-target event tests."""

from pathlib import Path

import pytest

from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
)
from ryan_library.functions.culvert.event_import import load_event_csv
from ryan_library.functions.culvert.export import scenario_result_record
from ryan_library.orchestrators.culvert.events import materialize_event_scenarios
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def _crossing() -> CrossingDefinition:
    return CrossingDefinition(
        name="Event crossing",
        groups=(
            CulvertGroupDefinition(
                name="Pipe",
                barrel=CircularBarrelDefinition(
                    diameter_mm=1200,
                    length=40,
                    inlet_invert=10,
                    outlet_invert=9.5,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
    )


def test_imports_discharge_and_headwater_targets_in_order(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    path.write_text(
        "name,aep_percent,discharge_m3s,target_headwater_elevation_m,"
        "tailwater_elevation_m,source,notes\n"
        "Minor,20,2,,,External model,\n"
        ",1,,11.5,9.7,Design table,Target level\n",
        encoding="utf-8",
    )

    crossing = _crossing()
    events = load_event_csv(path)
    scenarios = materialize_event_scenarios(events, crossing, default_tailwater=9.5)

    assert [scenario.name for scenario in scenarios] == ["Minor", "1% AEP"]
    assert scenarios[0].discharge == 2.0
    assert scenarios[0].tailwater_override_elevation is None
    assert scenarios[1].discharge > 0.0
    assert scenarios[1].target_headwater_elevation == 11.5
    assert scenarios[1].tailwater == 9.7
    assert scenarios[1].tailwater_override_elevation == 9.7

    result = solve_crossing_scenario(crossing, scenarios[1])
    record = scenario_result_record(result)

    assert result.tailwater_was_event_override
    assert result.target_headwater_residual is not None
    assert abs(result.target_headwater_residual) < 1e-4
    assert record["tailwater_override_elevation_m"] == 9.7
    assert record["tailwater_was_event_override"] is True
    assert record["target_headwater_residual_m"] == result.target_headwater_residual


def test_import_reports_invalid_row_number(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    path.write_text("name,discharge_m3s,target_headwater_elevation_m\nBad,2,11\n", encoding="utf-8")

    with pytest.raises(ValueError, match="row 2"):
        load_event_csv(path)
