"""Read-only TUFLOW rain/loss inventory fixture tests."""

from pathlib import Path

import pandas as pd

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.rainfall_input_audit import (
    _material_definition,
    audit_command_plan,
    factor_groups,
)


class GisPath:
    def gis_attributes(self) -> list[dict[str, object]]:
        return [{"Name": "RF1", "f1": 1.5, "f2": 0.0}]


class GisInput:
    lhs = "Read GIS RF"

    def __init__(self) -> None:
        self.files = [GisPath()]


class BC:
    df = pd.DataFrame(
        {"Source": ["event.csv"], "Column 2": ["TP01"], "Mult Col 2": [1.2], "Add Col 2": [0.0]},
        index=["RF1"],
    )

    def value(self, name: str) -> pd.DataFrame:
        assert name == "RF1"
        return pd.DataFrame({"TP01": [10.0, 20.0]})


class Materials:
    df = pd.DataFrame(
        {"Rainfall Losses": ["30,5", "0,0"], "Fraction Impervious": [0.0, 0.5]}, index=[1, 2]
    )


class Soils:
    df = pd.DataFrame({"Method": ["ILCL"]}, index=[3])

    def value(self, soil_id: int) -> dict[str, str | float]:
        assert soil_id == 3
        return {"method": "ILCL", "IL": 20.0, "CL": 3.0}


class Context:
    def get_inputs(self) -> list[GisInput]:
        return [GisInput()]

    def bc_dbase(self) -> BC:
        return BC()

    def mat_file(self) -> Materials:
        return Materials()

    def soils_file(self) -> Soils:
        return Soils()


class Model:
    def __init__(self, path: Path) -> None:
        self.path = path

    def context(self, flags: dict[str, str]) -> Context:
        assert flags
        return Context()


def test_selected_run_inventory_and_factor_groups() -> None:
    runs = tuple(
        PlannedRun(
            index=number,
            source=Path("runs.txt"),
            line_start=number,
            line_end=number,
            tcf=Path("model.tcf"),
            working_directory=Path.cwd(),
            flag_pairs=(("e1", event),),
            arguments=("-e1", event),
            executable=None,
        )
        for number, event in ((1, "a"), (2, "b"))
    )
    plan = CommandPlan(source=Path("runs.txt"), runs=runs, directories=(), ignored_statements=0, syntax="commands")
    audit = audit_command_plan(plan, model_factory=Model)
    assert len(audit.runs) == 2
    assert audit.runs[0].rainfall[0].bc_adjusted_sum_mm == 30.0
    assert audit.runs[0].rainfall[0].spatially_adjusted_sum_mm is None
    assert audit.runs[0].materials[0].initial_loss_mm == 30.0
    assert audit.runs[0].soils[0].parameters[0] == ("CL", "3.0")
    assert audit.runs[0].spatial_combinations_verified is False
    assert len(factor_groups(audit)) == 1
    assert list(factor_groups(audit).values()) == [(1, 2)]
    assert any(finding.code == "SPATIAL_COMBINATION_UNRESOLVED" for finding in audit.runs[0].findings)


def test_unresolved_material_does_not_become_zero() -> None:
    material = _material_definition(1, {"Rainfall Losses": "", "Fraction Impervious": 0.0})
    assert material.status == "unresolved"
    assert material.initial_loss_mm is None
