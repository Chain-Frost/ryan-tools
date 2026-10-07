"""Tests for per-crossing TUFLOW culvert attribute sources."""

from pathlib import Path
import pandas as pd
import pytest

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert import tuflow_attributes


def test_load_mixed_materials_and_losses_from_csv(tmp_path: Path) -> None:
    source = tmp_path / "culvert_attributes.csv"
    pd.DataFrame(
        [
            {
                "ID": "C01",
                "Material": "concrete_pipe",
                "Entry Loss": 0.5,
                "Exit Loss": 1.0,
                "Fixed Loss": 0.0,
            },
            {
                "ID": "C02",
                "Material": "csp",
                "Entry Loss": 0.9,
                "Exit Loss": 1.0,
                "Fixed Loss": 0.0,
            },
            {
                "ID": "C03",
                "Material": "hdpe",
                "Entry Loss": 0.5,
                "Exit Loss": 1.0,
                "Fixed Loss": 0.0,
            },
        ]
    ).to_csv(source, index=False)

    attributes = tuflow_attributes.load_tuflow_culvert_attributes(source)

    assert attributes["C01"].material is CulvertMaterialName.CONCRETE_PIPE
    assert attributes["C01"].entry_loss == pytest.approx(0.5)
    assert attributes["C02"].material is CulvertMaterialName.CORRUGATED_STEEL
    assert attributes["C02"].entry_loss == pytest.approx(0.9)
    assert attributes["C03"].material is CulvertMaterialName.SMOOTH_HDPE


def test_load_losses_from_1d_nwk_style_vector_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "culverts.shp"
    frame = pd.DataFrame(
        [
            {
                "ID": "C01",
                "EntryC_or_WSa": 0.5,
                "ExitC_or_WSb": 1.0,
                "Form_Loss": 0.2,
                "WConF_or_WEx": 1.0,
            }
        ]
    )

    def read_file(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return frame.copy()

    monkeypatch.setattr(tuflow_attributes.gpd, "read_file", read_file)

    attributes = tuflow_attributes.load_tuflow_culvert_attributes(source)

    item = attributes["C01"]
    assert item.material is None
    assert item.entry_loss == pytest.approx(0.5)
    assert item.exit_loss == pytest.approx(1.0)
    assert item.form_loss == pytest.approx(0.2)
    assert item.width_contraction == pytest.approx(1.0)


def test_attribute_source_rejects_duplicate_ids(tmp_path: Path) -> None:
    source = tmp_path / "duplicates.csv"
    pd.DataFrame(
        [
            {"ID": "C01", "Material": "concrete"},
            {"ID": "C01", "Material": "csp"},
        ]
    ).to_csv(source, index=False)

    with pytest.raises(ValueError, match="Duplicate culvert attribute identifier"):
        tuflow_attributes.load_tuflow_culvert_attributes(source)


def test_attribute_source_requires_crossing_identifier(tmp_path: Path) -> None:
    source = tmp_path / "missing_id.csv"
    pd.DataFrame([{"Material": "concrete"}]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="requires one identifier column"):
        tuflow_attributes.load_tuflow_culvert_attributes(source)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("concrete", CulvertMaterialName.CONCRETE_PIPE),
        ("RCP", CulvertMaterialName.CONCRETE_PIPE),
        ("csp", CulvertMaterialName.CORRUGATED_STEEL),
        ("corrugated steel", CulvertMaterialName.CORRUGATED_STEEL),
        ("hdpe", CulvertMaterialName.SMOOTH_HDPE),
    ],
)
def test_parse_culvert_material_aliases(value: str, expected: CulvertMaterialName) -> None:
    assert tuflow_attributes.parse_culvert_material(value) is expected


def test_attribute_source_rejects_nonfinite_loss(tmp_path: Path) -> None:
    source = tmp_path / "bad_loss.csv"
    pd.DataFrame([{"ID": "C01", "Entry Loss": float("inf")}]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="finite numeric value"):
        tuflow_attributes.load_tuflow_culvert_attributes(source)
