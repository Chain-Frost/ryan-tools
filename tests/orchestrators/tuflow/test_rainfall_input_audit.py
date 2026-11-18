"""Read-only TUFLOW rain/loss inventory fixture tests."""

# Tests deliberately exercise internal validation and use of private helpers.
# pyright: reportPrivateUsage=false

import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import ClassVar, Self

import pandas as pd
import pytest

import ryan_library.orchestrators.tuflow.rainfall_input_audit as audit_module
from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.rainfall_input_audit import (
    _material_definition,
    _soil_definition,
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
    df = pd.DataFrame({"Rainfall Losses": ["30,5", "0,0"], "Fraction Impervious": [0.0, 0.5]}, index=[1, 2])


class Soils:
    df = pd.DataFrame({"Method": ["ILCL"]}, index=[3])

    def value(self, soil_id: int) -> dict[str, str | float]:
        assert soil_id == 3
        return {"method": "ILCL", "IL": 20.0, "CL": 3.0}


class Context:
    def find_input(self, *, recursive: bool = False) -> list[GisInput]:
        assert recursive
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


def test_selected_run_inventory_and_factor_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_gis_records(path: object) -> tuple[dict[str, object], ...]:
        assert isinstance(path, GisPath)
        return tuple(path.gis_attributes())

    monkeypatch.setattr(audit_module, "_gis_records", fake_gis_records)
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


def test_missing_optional_material_losses_and_impervious_default_to_zero() -> None:
    material = _material_definition(1, {"Manning's n": 0.03, "Rainfall Losses": ""})
    assert material.status == "defined"
    assert material.initial_loss_mm == 0.0
    assert material.continuing_loss_mm_h == 0.0
    assert material.fraction_impervious == 0.0
    roughness_only = _material_definition(2, {"Manning's n": 0.03})
    assert roughness_only.status == "defined"
    assert roughness_only.initial_loss_mm == 0.0
    assert roughness_only.continuing_loss_mm_h == 0.0
    assert roughness_only.fraction_impervious == 0.0
    assert _material_definition(3, {"Unexpected": 0.03}).status == "unresolved"


def test_invalid_explicit_material_loss_remains_unresolved() -> None:
    material = _material_definition(1, {"Rainfall Losses": "invalid", "Fraction Impervious": 0.0})
    assert material.status == "unresolved"
    assert material.initial_loss_mm is None


def test_missing_material_schema_is_unresolved() -> None:
    class MaterialWithUnknownColumns:
        df: ClassVar[pd.DataFrame] = pd.DataFrame([[0.03, "30,5", 0.5]], index=[1])

    rows = audit_module._db_rows(MaterialWithUnknownColumns())
    assert _material_definition(1, rows[0][1]).status == "unresolved"

    class MaterialWithSupportedColumns:
        COLUMN_NAMES: ClassVar[list[str]] = ["Manning's n", "Rainfall Losses", "Fraction Impervious"]
        df: ClassVar[pd.DataFrame] = pd.DataFrame([[0.03, "30,5", 0.5]], index=[1])

    parsed = _material_definition(1, audit_module._db_rows(MaterialWithSupportedColumns())[0][1])
    assert parsed.initial_loss_mm == 30
    assert parsed.continuing_loss_mm_h == 5
    assert parsed.fraction_impervious == 0.5


def test_invalid_soil_parameters_cannot_be_reported_defined() -> None:
    assert _soil_definition(1, {"method": "ILCL", "IL": float("nan"), "CL": 2.0}).status == "unresolved"
    assert _soil_definition(1, {"method": "GA", "Suction": 3}).status == "unresolved"
    assert _soil_definition(1, {"method": "ILCL", "IL": 0, "CL": 0}).status == "defined"


def test_selected_empty_soil_database_cannot_be_treated_as_absent() -> None:
    class EmptySoils:
        df: ClassVar[pd.DataFrame] = pd.DataFrame({"Method": []})

    class EmptySoilContext:
        def soils_file(self) -> EmptySoils:
            return EmptySoils()

    findings: list[audit_module.Finding] = []
    assert not audit_module._soil_rows(EmptySoilContext(), findings, 1)
    assert any(item.code == "SOIL_UNRESOLVED" for item in findings)


def test_ts1_group_uses_group_metadata_and_selected_column(monkeypatch: pytest.MonkeyPatch) -> None:
    class Groups(Context):
        def bc_dbase(self) -> BC:
            class GroupBC(BC):
                df = pd.DataFrame({"Source": ["rain.ts1"], "Column 2": [""], "Mult Col 2": [1.2]}, index=["Group"])

                def value(self, name: str) -> pd.DataFrame:
                    assert name == "N1|Group"
                    return pd.DataFrame({"N1": [3.0, 4.0]})

            return GroupBC()

    def selected_names(*_args: object) -> dict[str, tuple[str | None, str | None]]:
        return {"N1|Group": (None, None)}

    monkeypatch.setattr(audit_module, "_active_rainfall_names", selected_names)
    findings: list[audit_module.Finding] = []
    rows = audit_module._rainfall_rows(Groups(), findings, 1)
    assert rows[0].column == "N1"
    assert rows[0].bc_adjusted_sum_mm == 7
    assert not any(item.code == "RF_BC_MISSING" for item in findings)


def test_bad_explicit_bc_multiplier_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    class Bad(Context):
        def bc_dbase(self) -> BC:
            class BadBC(BC):
                df = pd.DataFrame(
                    {"Source": ["event.csv"], "Column 2": ["TP01"], "Mult Col 2": ["broken"]}, index=["RF1"]
                )

            return BadBC()

    def selected_names(*_args: object) -> dict[str, tuple[str | None, str | None]]:
        return {"RF1": (None, None)}

    monkeypatch.setattr(audit_module, "_active_rainfall_names", selected_names)
    findings: list[audit_module.Finding] = []
    rows = audit_module._rainfall_rows(Bad(), findings, 1)
    assert rows[0].status == "unresolved"
    assert rows[0].bc_adjusted_sum_mm is None
    assert any(item.code == "RF_SERIES_UNRESOLVED" for item in findings)


def test_partial_rainfall_not_grouped() -> None:
    boundary = audit_module.RainfallBoundary(
        "RF1", "rain.csv", "TP01", "1", "0", None, None, 10, None, "bc-adjusted-only"
    )
    valid = audit_module.RunAudit(1, "a.tcf", (), "runs.txt:1", (boundary,), (), (), ())
    partial = audit_module.RunAudit(
        2, "a.tcf", (), "runs.txt:2", (boundary,), (), (), (audit_module.Finding(2, "RF_BC_MISSING", "RF2"),)
    )
    assert list(factor_groups(audit_module.RainfallAudit((valid, partial))).values()) == [(1,)]


def test_gis_reader_uses_supported_context_manager_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[str] = []

    class GISAttributes:
        def __init__(self, path: object) -> None:
            assert path == "gis.gpkg >> rf_R"

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            closed.append("closed")

        def __iter__(self) -> Iterator[dict[str, object]]:
            yield {"Name": "RF1", "f1": 1.2}

    root = ModuleType("pytuflow")
    util = ModuleType("pytuflow.util")
    gis = ModuleType("pytuflow.util.gis")
    gis.__dict__["GISAttributes"] = GISAttributes
    util.__dict__["gis"] = gis
    monkeypatch.setitem(sys.modules, "pytuflow", root)
    monkeypatch.setitem(sys.modules, "pytuflow.util", util)
    monkeypatch.setitem(sys.modules, "pytuflow.util.gis", gis)
    assert audit_module._gis_records("gis.gpkg >> rf_R") == ({"Name": "RF1", "f1": 1.2},)
    assert closed == ["closed"]


def test_conflicting_gis_rainfall_factors_do_not_enter_equivalence_group(monkeypatch: pytest.MonkeyPatch) -> None:
    def conflicting_records(_path: object) -> tuple[dict[str, object], ...]:
        return ({"Name": "RF1", "f1": 1.2}, {"Name": "RF1", "f1": 2.0})

    monkeypatch.setattr(audit_module, "_gis_records", conflicting_records)
    run = PlannedRun(
        index=1,
        source=Path("runs.txt"),
        line_start=1,
        line_end=1,
        tcf=Path("model.tcf"),
        working_directory=Path.cwd(),
        flag_pairs=(("e1", "a"),),
        arguments=("-e1", "a"),
        executable=None,
    )
    plan = CommandPlan(Path("runs.txt"), (run,), (), 0, "commands")
    audit = audit_command_plan(plan, model_factory=Model)
    assert any(item.code == "RF_FACTOR_CONFLICT" for item in audit.runs[0].findings)
    assert not factor_groups(audit)
