"""Tests for backend-neutral physical TUFLOW culvert configuration."""

import pytest

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    CulvertShapeName,
    TuflowLossParameters,
    parse_tuflow_shape,
)


def test_circular_configuration_owns_shape_material_and_inlet() -> None:
    configuration = CircularCulvertConfiguration(
        material=CulvertMaterialName.CORRUGATED_STEEL,
        inlet=CircularInletConfiguration.THIN_EDGE_PROJECTING,
    )

    assert configuration.shape is CulvertShapeName.CIRCULAR
    assert configuration.material is CulvertMaterialName.CORRUGATED_STEEL
    assert configuration.inlet is CircularInletConfiguration.THIN_EDGE_PROJECTING


def test_material_inlet_pair_is_validated() -> None:
    with pytest.raises(ValueError, match="not valid"):
        CircularCulvertConfiguration(
            material=CulvertMaterialName.CONCRETE_PIPE,
            inlet=CircularInletConfiguration.THIN_EDGE_PROJECTING,
        )


def test_tuflow_type_maps_to_physical_shape() -> None:
    assert parse_tuflow_shape("C") is CulvertShapeName.CIRCULAR
    assert parse_tuflow_shape("R") is CulvertShapeName.BOX


def test_loss_parameters_are_separate_numeric_model_inputs() -> None:
    losses = TuflowLossParameters(
        entry_loss_coefficient=0.7,
        exit_loss_coefficient=1.0,
        form_loss_coefficient=0.2,
        width_contraction_coefficient=0.8,
    )

    assert losses.entry_loss_coefficient == pytest.approx(0.7)
    assert losses.form_loss_coefficient == pytest.approx(0.2)
