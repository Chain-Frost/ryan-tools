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
from ryan_library.orchestrators.culvert.events import materialize_event_scenarios


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

    events = load_event_csv(path)
    scenarios = materialize_event_scenarios(events, _crossing(), default_tailwater=9.5)

    assert [scenario.name for scenario in scenarios] == ["Minor", "1% AEP"]
    assert scenarios[0].discharge == 2.0
    assert scenarios[1].discharge > 0.0
    assert scenarios[1].target_headwater_elevation == 11.5
    assert scenarios[1].tailwater == 9.7


def test_import_reports_invalid_row_number(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    path.write_text("name,discharge_m3s,target_headwater_elevation_m\nBad,2,11\n", encoding="utf-8")

    with pytest.raises(ValueError, match="row 2"):
        load_event_csv(path)
