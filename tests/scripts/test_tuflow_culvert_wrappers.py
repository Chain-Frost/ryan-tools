"""Regression tests for migrated TUFLOW culvert wrappers."""

import runpy
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_attributes import TuflowCulvertAttributes
from ryan_library.functions.culvert.tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    TuflowLossParameters,
)
from ryan_library.functions.culvert.tuflow_engines import (
    CulvertEngine,
    CulvertEngineResult,
    TuflowCircularCulvert,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAXIMUMS_SCRIPT = PROJECT_ROOT / "ryan-scripts" / "tuflow" / "tuflow_culvert_from_maximums.py"
NWK_SCRIPT = PROJECT_ROOT / "ryan-scripts" / "tuflow" / "tuflow_culvert_from_1d_nwk.py"


def _maximums_namespace() -> dict[str, Any]:
    return runpy.run_path(str(MAXIMUMS_SCRIPT))


def _nwk_namespace() -> dict[str, Any]:
    return runpy.run_path(str(NWK_SCRIPT))


def test_maximums_mapping_uses_canonical_barrel_count_and_preserves_zero_invert() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "Chan ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Height": 1.2,
            "Length": 30.0,
            "US Invert": 0.0,
            "DS Invert": -0.2,
            "n or Cd": 0.024,
            "Num_barrels": 3,
        }
    )

    assert definition.barrels == 3
    assert definition.inlet_invert_m == pytest.approx(0.0)
    assert definition.outlet_invert_m == pytest.approx(-0.2)


def test_maximums_rejects_malformed_canonical_barrel_count() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="Num_barrels must contain a numeric integer barrel count"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "n or Cd": 0.024,
                "Num_barrels": "damaged",
            }
        )


def test_maximums_rejects_missing_manning_roughness() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="positive Manning roughness"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "Num_barrels": 1,
            }
        )


def test_maximums_rejects_missing_required_geometry() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="required Maximums geometry fields"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "Num_barrels": 1,
            }
        )


def test_maximums_rejects_missing_barrel_count() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="barrel count is required"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "n or Cd": 0.024,
                "Num_barrels": float("nan"),
            }
        )


def test_maximums_rejects_malformed_present_numeric_geometry() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="Length must contain a numeric value"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": "damaged",
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "Num_barrels": 1,
            }
        )


@pytest.mark.parametrize("ratio", [0.0, -1.0, float("inf"), float("nan")])
def test_maximums_rejects_invalid_headwater_ratio(ratio: float) -> None:
    namespace = _maximums_namespace()
    validate = cast("Callable[[float], float]", namespace["_headwater_ratio"])

    with pytest.raises(ValueError, match="finite and strictly positive"):
        validate(ratio)


def test_maximums_applies_numeric_blockage_to_circular_diameter() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "Chan ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Height": 1.2,
            "Length": 30.0,
            "US Invert": 10.0,
            "DS Invert": 9.8,
            "n or Cd": 0.024,
            "Num_barrels": 1,
            "pBlockage": 50.0,
        }
    )

    assert definition.diameter_m == pytest.approx(1.2 * (0.5**0.5))
    assert definition.hw_diameter_m == pytest.approx(1.2)


def test_maximums_rejects_unresolved_category_blockage() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="numeric percentage"):
        build_definition(
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "Num_barrels": 1,
                "pBlockage": "B",
            }
        )


def test_maximums_mapping_rejects_rectangular_type() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="circular TUFLOW Type 'C' rows only"):
        build_definition(
            {
                "Chan ID": "BOX01",
                "Type": "R",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "n or Cd": 0.013,
                "Num_barrels": 1,
            }
        )


def test_maximums_selection_keeps_governing_row_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _maximums_namespace()
    select_rows = cast(
        "Callable[[Path, str, str | None], list[dict[str, Any]]]",
        namespace["_selected_rows"],
    )
    source = pd.DataFrame(
        [
            {
                "Chan ID": "C01",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 10.0,
                "US_h": None,
                "DS_h": None,
            },
            {
                "Chan ID": "C01",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 9.0,
                "US_h": 11.5,
                "DS_h": 10.7,
            },
        ]
    )

    def read_excel(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return source.copy()

    monkeypatch.setattr(pd, "read_excel", read_excel)

    rows = select_rows(Path("maximums.xlsx"), "Maximums", None)

    assert len(rows) == 1
    assert rows[0]["Q"] == pytest.approx(10.0)
    assert pd.isna(rows[0]["US_h"])
    assert pd.isna(rows[0]["DS_h"])


def test_maximums_selection_preserves_base_run_scenarios(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _maximums_namespace()
    select_rows = cast(
        "Callable[[Path, str, str | None], list[dict[str, Any]]]",
        namespace["_selected_rows"],
    )
    source = pd.DataFrame(
        [
            {
                "Chan ID": "C01",
                "trim_runcode": "EXG",
                "internalName": "Model_EXG_01.0p_060m_TP01",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 8.0,
            },
            {
                "Chan ID": "C01",
                "trim_runcode": "EXG",
                "internalName": "Model_EXG_01.0p_120m_TP02",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 10.0,
            },
            {
                "Chan ID": "C01",
                "trim_runcode": "DEV",
                "internalName": "Model_DEV_01.0p_060m_TP01",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 12.0,
            },
            {
                "Chan ID": "C01",
                "trim_runcode": "DEV",
                "internalName": "Model_DEV_01.0p_120m_TP02",
                "aep_text": "1%",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Q": 11.0,
            },
        ]
    )

    def read_excel(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return source.copy()

    monkeypatch.setattr(pd, "read_excel", read_excel)

    rows = select_rows(Path("maximums.xlsx"), "Maximums", None)

    assert [(row["Run"], row["Q"]) for row in rows] == [
        ("DEV", pytest.approx(12.0)),
        ("EXG", pytest.approx(10.0)),
    ]


def test_physical_configuration_is_explicit_not_inferred_from_roughness() -> None:
    from ryan_library.functions.culvert.tuflow_attributes import resolve_circular_configuration

    with pytest.raises(ValueError, match="material is unresolved"):
        resolve_circular_configuration({"n_nF_Cd": 0.024})
    with pytest.raises(ValueError, match="inlet configuration is unresolved"):
        resolve_circular_configuration({"Material": "csp", "n_nF_Cd": 0.024})


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("concrete", CulvertMaterialName.CONCRETE_PIPE),
        ("rcp", CulvertMaterialName.CONCRETE_PIPE),
        ("concrete_pipe", CulvertMaterialName.CONCRETE_PIPE),
        ("csp", CulvertMaterialName.CORRUGATED_STEEL),
        ("corrugated_steel", CulvertMaterialName.CORRUGATED_STEEL),
        ("hdpe", CulvertMaterialName.SMOOTH_HDPE),
    ],
)
def test_material_aliases_are_explicit(value: str, expected: CulvertMaterialName) -> None:
    from ryan_library.functions.culvert.tuflow_attributes import parse_culvert_material

    assert parse_culvert_material(value) is expected


def test_maximums_external_attributes_override_inline_material_and_losses() -> None:
    namespace = _maximums_namespace()
    build_definition = namespace["_definition"]
    attributes = TuflowCulvertAttributes(
        crossing_id="C01",
        material=CulvertMaterialName.CORRUGATED_STEEL,
        inlet_configuration=CircularInletConfiguration.THIN_EDGE_PROJECTING,
        losses=TuflowLossParameters(
            entry_loss_coefficient=0.9,
            exit_loss_coefficient=1.0,
            form_loss_coefficient=0.0,
        ),
    )

    definition = build_definition(
        {
            "Chan ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Height": 1.2,
            "Length": 30.0,
            "US Invert": 10.0,
            "DS Invert": 9.8,
            "n or Cd": 0.024,
            "Num_barrels": 1,
            "Entry Loss": 0.7,
            "Exit Loss": 0.6,
            "Fixed Loss": 0.2,
        },
        None,
        None,
        attributes,
    )

    assert definition.configuration == CircularCulvertConfiguration(
        material=CulvertMaterialName.CORRUGATED_STEEL,
        inlet=CircularInletConfiguration.THIN_EDGE_PROJECTING,
    )


def test_1d_nwk_external_attributes_override_inline_material_and_losses() -> None:
    namespace = _nwk_namespace()
    build_definition = namespace["_definition"]
    attributes = TuflowCulvertAttributes(
        crossing_id="C01",
        material=CulvertMaterialName.CONCRETE_PIPE,
        inlet_configuration=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        losses=TuflowLossParameters(
            entry_loss_coefficient=0.5,
            exit_loss_coefficient=1.0,
            form_loss_coefficient=0.0,
            width_contraction_coefficient=1.0,
        ),
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "csp",
            "Inlet Configuration": "thin-edge-projecting",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Form_Loss": 0.4,
            "WConF_or_WEx": 0.8,
            "EntryC_or_WSa": 0.7,
            "ExitC_or_WSb": 0.6,
            "Number_of": 1,
        },
        1,
        None,
        None,
        attributes,
    )

    assert definition.configuration == CircularCulvertConfiguration(
        material=CulvertMaterialName.CONCRETE_PIPE,
        inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
    )


@pytest.mark.parametrize(
    ("ratios", "message"),
    [
        ([0.0], "finite and strictly positive"),
        ([-1.0], "finite and strictly positive"),
        ([float("inf")], "finite and strictly positive"),
        ([float("nan")], "finite and strictly positive"),
        ([1.5, 1.5], "must be unique"),
        ([], "at least one"),
    ],
)
def test_1d_nwk_rejects_invalid_headwater_ratios(ratios: list[float], message: str) -> None:
    namespace = _nwk_namespace()
    validate = cast("Callable[[list[float]], tuple[float, ...]]", namespace["_headwater_ratios"])

    with pytest.raises(ValueError, match=message):
        validate(ratios)


def test_1d_nwk_seed_flow_uses_circular_area() -> None:
    namespace = _nwk_namespace()
    seed_flow = cast("Callable[[TuflowCircularCulvert], float]", namespace["_seed_flow_hint"])
    definition = TuflowCircularCulvert(
        name="C01",
        configuration=CircularCulvertConfiguration(
            material=CulvertMaterialName.CONCRETE_PIPE,
            inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ),
        diameter_m=2.0,
        length_m=30.0,
        inlet_invert_m=10.0,
        outlet_invert_m=9.5,
        roughness_manning_n=0.013,
        barrels=2,
    )

    assert seed_flow(definition) == pytest.approx(2.0 * 3.141592653589793)


def test_1d_nwk_skips_ignored_features() -> None:
    namespace = _nwk_namespace()
    select_active_rows = cast(
        "Callable[[list[dict[str, Any]], str | None], list[dict[str, Any]]]",
        namespace["_select_active_rows"],
    )
    rows: list[dict[str, Any]] = [
        {"ID": "IGNORED", "Ignore": "T"},
        {"ID": "ACTIVE", "Ignore": ""},
        {"ID": "IGNORED_Y", "Ignore": "y"},
    ]

    selected = select_active_rows(rows, None)

    assert [row["ID"] for row in selected] == ["ACTIVE"]
    with pytest.raises(ValueError, match="marked ignored"):
        select_active_rows(rows, "IGNORED")


def test_1d_nwk_geometry_length_requires_projected_metre_crs() -> None:
    namespace = _nwk_namespace()
    validate = cast("Callable[[Any], None]", namespace["_require_metric_projected_crs"])

    metre_crs = SimpleNamespace(
        is_projected=True,
        axis_info=(
            SimpleNamespace(unit_conversion_factor=1.0),
            SimpleNamespace(unit_conversion_factor=1.0),
        ),
    )
    validate(metre_crs)

    with pytest.raises(ValueError, match="projected CRS"):
        validate(None)
    with pytest.raises(ValueError, match="projected CRS"):
        validate(SimpleNamespace(is_projected=False, axis_info=()))
    with pytest.raises(ValueError, match="axis units in metres"):
        validate(
            SimpleNamespace(
                is_projected=True,
                axis_info=(
                    SimpleNamespace(unit_conversion_factor=0.3048),
                    SimpleNamespace(unit_conversion_factor=0.3048),
                ),
            )
        )


def test_1d_nwk_rejects_blank_culvert_id() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="ID is required"):
        build_definition(
            {
                "ID": None,
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "n_nF_Cd": 0.013,
                "Number_of": 1,
            },
            2,
        )


def test_1d_nwk_negative_length_uses_digitized_geometry_length() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Width_or_D": 1.2,
            "Len_or_ANA": -1.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Number_of": 2,
            "geometry": SimpleNamespace(length=42.5),
        },
        1,
    )

    assert definition.length_m == pytest.approx(42.5)
    assert definition.barrels == 2


def test_1d_nwk_applies_numeric_blockage_to_circular_diameter() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "pBlockage": 50.0,
            "Number_of": 1,
        },
        1,
    )

    assert definition.diameter_m == pytest.approx(1.2 * (0.5**0.5))
    assert definition.hw_diameter_m == pytest.approx(1.2)
    assert definition.inlet_invert_m == pytest.approx(10.0)


def test_1d_nwk_rejects_blank_categorical_blockage_default() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )
    blockage_numeric_key = cast("str", namespace["BLOCKAGE_NUMERIC_KEY"])

    with pytest.raises(ValueError, match="Blockage Default"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "n_nF_Cd": 0.013,
                "pBlockage": "",
                "Number_of": 1,
                blockage_numeric_key: False,
            },
            1,
        )


def test_1d_nwk_rejects_category_blockage_without_resolved_percentage() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="numeric percentage"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "pBlockage": "B",
                "Number_of": 1,
            },
            1,
        )


def test_1d_nwk_rejects_malformed_canonical_barrel_count() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="Number_of must contain a numeric integer barrel count"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "n_nF_Cd": 0.013,
                "Number_of": "damaged",
            },
            1,
        )


def test_1d_nwk_rejects_missing_manning_roughness() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="positive Manning roughness"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "Number_of": 1,
            },
            1,
        )


def test_1d_nwk_rejects_missing_barrel_count() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="barrel count is required"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "n_nF_Cd": 0.013,
                "Number_of": float("nan"),
            },
            1,
        )


def test_1d_nwk_preserves_tuflow_loss_parameters() -> None:
    namespace = _nwk_namespace()
    build_definition = namespace["_definition"]

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Form_Loss": 0.2,
            "WConF_or_WEx": 0.8,
            "EntryC_or_WSa": 0.7,
            "ExitC_or_WSb": 0.6,
            "Number_of": 1,
        },
        1,
    )

    assert definition.losses == TuflowLossParameters(
        entry_loss_coefficient=0.7,
        exit_loss_coefficient=0.6,
        form_loss_coefficient=0.2,
        width_contraction_coefficient=0.8,
    )


def test_1d_nwk_accepts_supported_concrete_loss_coefficients() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Form_Loss": 0.0,
            "WConF_or_WEx": 1.0,
            "EntryC_or_WSa": 0.5,
            "ExitC_or_WSb": 1.0,
            "Number_of": 1,
        },
        1,
    )

    assert definition.name == "C01"


def test_1d_nwk_zero_number_of_defaults_to_one_barrel() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Material": "concrete_pipe",
            "Inlet Configuration": "square-edge-headwall",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Number_of": 0,
        },
        1,
    )

    assert definition.barrels == 1


def test_1d_nwk_rejects_unresolved_invert_sentinel() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="-99999 sentinel"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": -99999,
                "DS_Invert": 9.8,
                "Number_of": 1,
            },
            1,
        )


@pytest.mark.parametrize("script_path", [MAXIMUMS_SCRIPT, NWK_SCRIPT])
def test_wrappers_require_explicit_overwrite(script_path: Path, tmp_path: Path) -> None:
    namespace = runpy.run_path(str(script_path))
    ensure_output = namespace["_ensure_output_available"]
    output = tmp_path / "results.csv"
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        ensure_output(output, overwrite=False)

    ensure_output(output, overwrite=True)


def test_maximums_hy8_workspaces_are_unique_by_aep_and_require_overwrite(tmp_path: Path) -> None:
    namespace = _maximums_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "C01", "EXG", "1%", "Q @ DS_h TW", overwrite=False)
    second = workspace(tmp_path, "C01", "EXG", "2%", "Q @ DS_h TW", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second

    marker = first / "retained.txt"
    marker.write_text("old run", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        workspace(tmp_path, "C01", "EXG", "1%", "Q @ DS_h TW", overwrite=False)

    replaced = workspace(tmp_path, "C01", "EXG", "1%", "Q @ DS_h TW", overwrite=True)
    assert replaced == first
    assert not marker.exists()


def test_1d_nwk_hy8_workspace_requires_explicit_overwrite(tmp_path: Path) -> None:
    namespace = _nwk_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=False)
    assert first is not None

    marker = first / "retained.txt"
    marker.write_text("old run", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=False)

    replaced = workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=True)
    assert replaced == first
    assert not marker.exists()


def test_maximums_workspace_sanitization_is_collision_safe(tmp_path: Path) -> None:
    namespace = _maximums_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "A B", "EXG", "1%", "Q @ DS_h TW", overwrite=False)
    second = workspace(tmp_path, "A_B", "EXG", "1%", "Q @ DS_h TW", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second
    assert first.exists()
    assert second.exists()
    assert len(first.name) <= 134
    assert len(second.name) <= 134


def test_1d_nwk_workspace_sanitization_is_collision_safe(tmp_path: Path) -> None:
    namespace = _nwk_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "A B", "HW:D = 1.5", overwrite=False)
    second = workspace(tmp_path, "A_B", "HW:D = 1.5", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second
    assert first.exists()
    assert second.exists()


def test_1d_nwk_run_preserves_original_source_row_after_ignore_filter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _nwk_namespace()
    frame = pd.DataFrame(
        [
            {
                "ID": "IGNORED",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Ignore": "T",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.5,
                "n_nF_Cd": 0.013,
                "Number_of": 1,
            },
            {
                "ID": "ACTIVE",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Ignore": "",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.5,
                "n_nF_Cd": 0.013,
                "Number_of": 1,
            },
        ]
    )

    def read_file(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return frame.copy()

    monkeypatch.setattr(namespace["gpd"], "read_file", read_file)

    def fake_inverse(
        definition: TuflowCircularCulvert,
        *,
        scenario: str,
        headwater_elevation_m: float,
        **_kwargs: object,
    ) -> CulvertEngineResult:
        return CulvertEngineResult(
            engine=CulvertEngine.RYAN_CULVERTS,
            crossing=definition.name,
            scenario=scenario,
            requested_discharge_m3s=None,
            requested_headwater_m=headwater_elevation_m,
            computed_discharge_m3s=1.0,
            headwater_elevation_m=headwater_elevation_m,
            headwater_ratio=(headwater_elevation_m - definition.inlet_invert_m) / definition.hw_diameter_m,
            outlet_velocity_mps=1.0,
            flow_type="test",
            roadway_discharge_m3s=0.0,
            overtopping=False,
            status="valid",
        )

    namespace["solve_tuflow_culvert_inverse"] = fake_inverse
    output = tmp_path / "results.csv"
    args = namespace["_parser"]().parse_args(
        [
            str(tmp_path / "network.gpkg"),
            "--layer",
            "1d_nwk",
            "--engine",
            "ryan-culverts",
            "--output-csv",
            str(output),
        ]
    )

    assert namespace["run"](args) == 0
    result = pd.read_csv(output)
    assert set(result["Crossing"]) == {"ACTIVE"}
    assert set(result["Source Row"]) == {2}


@pytest.mark.parametrize("explicit_workspace", [False, True])
def test_native_maximums_does_not_create_hy8_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    explicit_workspace: bool,
) -> None:
    namespace = _maximums_namespace()
    monkeypatch.chdir(tmp_path)
    workbook = tmp_path / "maximums.xlsx"
    pd.DataFrame(
        [
            {
                "Chan ID": "C01",
                "Type": "C",
                "Material": "concrete_pipe",
                "Inlet Configuration": "square-edge-headwall",
                "Height": 1.2,
                "Length": 30.0,
                "Q": 2.0,
                "trim_runcode": "EXG",
                "US Invert": 10.0,
                "DS Invert": 9.5,
                "n or Cd": 0.024,
                "Num_barrels": 1,
            }
        ]
    ).to_excel(workbook, sheet_name="Maximums", index=False)  # pyright: ignore[reportUnknownMemberType]
    workspace = tmp_path / "explicit-workspace"
    cli = [str(workbook), "--engine", "ryan-culverts", "--keep-workspace"]
    if explicit_workspace:
        cli.extend(["--workspace", str(workspace)])
    args = namespace["_parser"]().parse_args(cli)
    assert namespace["run"](args) == 0
    assert args.output_csv.exists()
    output = pd.read_csv(args.output_csv)
    assert set(output["Run"]) == {"EXG"}
    assert not workspace.exists()
    assert not (tmp_path / "hy8-workspaces").exists()
