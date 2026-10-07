"""Tests for per-crossing physical configuration and TUFLOW losses."""

from pathlib import Path

import pandas as pd
import pytest

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert import tuflow_attributes
from ryan_library.functions.culvert.tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    TuflowLossParameters,
)


def test_load_mixed_physical_configurations_and_losses_from_csv(tmp_path: Path) -> None:
    source = tmp_path / "culvert_attributes.csv"
    pd.DataFrame(
        [
            {
                "ID": "C01",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Entry Loss": 0.5,
                "Exit Loss": 1.0,
            },
            {
                "ID": "C02",
                "Material": "csp",
                "Inlet Configuration": "thin-edge-projecting",
                "Entry Loss": 0.9,
                "Exit Loss": 1.0,
            },
            {
                "ID": "C03",
                "Material": "hdpe",
                "Inlet Configuration": "mitered-to-slope",
            },
        ]
    ).to_csv(source, index=False)

    attributes = tuflow_attributes.load_tuflow_culvert_attributes(source)

    assert attributes["C01"].material is CulvertMaterialName.CONCRETE_PIPE
    assert attributes["C01"].inlet_configuration is CircularInletConfiguration.SQUARE_EDGE_HEADWALL
    assert attributes["C01"].losses.entry_loss_coefficient == pytest.approx(0.5)
    assert attributes["C02"].material is CulvertMaterialName.CORRUGATED_STEEL
    assert attributes["C02"].inlet_configuration is CircularInletConfiguration.THIN_EDGE_PROJECTING
    assert attributes["C03"].material is CulvertMaterialName.SMOOTH_HDPE
    assert attributes["C03"].inlet_configuration is CircularInletConfiguration.MITERED_TO_SLOPE


def test_resolve_configuration_does_not_infer_from_roughness_or_losses() -> None:
    row = {"n_nF_Cd": 0.024, "EntryC_or_WSa": 0.9}
    with pytest.raises(ValueError, match="material is unresolved"):
        tuflow_attributes.resolve_circular_configuration(row)


def test_external_configuration_precedes_inline_values() -> None:
    row = {
        "Material": "concrete_pipe",
        "Inlet Configuration": "square-edge-headwall",
    }
    external = tuflow_attributes.TuflowCulvertAttributes(
        crossing_id="C01",
        material=CulvertMaterialName.CORRUGATED_STEEL,
        inlet_configuration=CircularInletConfiguration.THIN_EDGE_PROJECTING,
    )

    configuration = tuflow_attributes.resolve_circular_configuration(row, attributes=external)

    assert configuration == CircularCulvertConfiguration(
        material=CulvertMaterialName.CORRUGATED_STEEL,
        inlet=CircularInletConfiguration.THIN_EDGE_PROJECTING,
    )


def test_external_losses_override_only_fields_they_supply() -> None:
    row = {
        "EntryC_or_WSa": 0.5,
        "ExitC_or_WSb": 0.8,
        "Form_Loss": 0.1,
        "WConF_or_WEx": 0.9,
    }
    external = tuflow_attributes.TuflowCulvertAttributes(
        crossing_id="C01",
        losses=TuflowLossParameters(
            entry_loss_coefficient=0.7,
            exit_loss_coefficient=1.0,
        ),
    )

    losses = tuflow_attributes.resolve_tuflow_losses(row, attributes=external)

    assert losses == TuflowLossParameters(
        entry_loss_coefficient=0.7,
        exit_loss_coefficient=1.0,
        form_loss_coefficient=0.1,
        width_contraction_coefficient=0.9,
    )


def test_attribute_source_rejects_duplicate_ids(tmp_path: Path) -> None:
    source = tmp_path / "duplicates.csv"
    pd.DataFrame(
        [
            {"ID": "C01", "Material": "concrete", "Inlet": "headwall"},
            {"ID": "C01", "Material": "csp", "Inlet": "projecting"},
        ]
    ).to_csv(source, index=False)

    with pytest.raises(ValueError, match="Duplicate culvert attribute identifier"):
        tuflow_attributes.load_tuflow_culvert_attributes(source)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("concrete", CulvertMaterialName.CONCRETE_PIPE),
        ("RCP", CulvertMaterialName.CONCRETE_PIPE),
        ("csp", CulvertMaterialName.CORRUGATED_STEEL),
        ("hdpe", CulvertMaterialName.SMOOTH_HDPE),
    ],
)
def test_parse_culvert_material_aliases(value: str, expected: CulvertMaterialName) -> None:
    assert tuflow_attributes.parse_culvert_material(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("square-edge-with-headwall", CircularInletConfiguration.SQUARE_EDGE_HEADWALL),
        ("projecting", CircularInletConfiguration.THIN_EDGE_PROJECTING),
        ("grooved-end-in-headwall", CircularInletConfiguration.GROOVED_END_HEADWALL),
        ("mitered-to-conform-to-slope", CircularInletConfiguration.MITERED_TO_SLOPE),
    ],
)
def test_parse_inlet_aliases(value: str, expected: CircularInletConfiguration) -> None:
    from ryan_library.functions.culvert.tuflow_configuration import parse_circular_inlet_configuration

    assert parse_circular_inlet_configuration(value) is expected


def test_attribute_source_rejects_nonfinite_loss(tmp_path: Path) -> None:
    source = tmp_path / "bad_loss.csv"
    pd.DataFrame([{"ID": "C01", "Entry Loss": float("inf")}]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="finite numeric value"):
        tuflow_attributes.load_tuflow_culvert_attributes(source)
