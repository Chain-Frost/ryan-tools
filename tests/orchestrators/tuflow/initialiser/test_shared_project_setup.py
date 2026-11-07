"""Contract tests for the initialiser's shared TUFLOW project generator."""

from pathlib import Path

import fiona
import pytest
from rasterio.crs import CRS

from ryan_library.orchestrators.tuflow import project_setup as shared_project_setup
from ryan_library.orchestrators.tuflow.initialiser import project_setup as initialiser_setup


def _fake_tuflow_generation(*, control_file: Path, expected_empty_dir: Path, **_kwargs: object) -> None:
    assert control_file.name == "Create_Empties.tcf"
    expected_empty_dir.mkdir(parents=True, exist_ok=True)
    for layer in ("2d_code", "2d_loc", "2d_rf"):
        with fiona.open(
            expected_empty_dir / f"{layer}_empty.gpkg",
            "w",
            driver="GPKG",
            layer=f"{layer}_empty_R",
            crs_wkt=CRS.from_epsg(28351).to_wkt(),
            schema={"geometry": "Polygon", "properties": {"Code": "int"}},
        ):
            pass


def test_existing_and_migrated_working_layer_defaults_are_distinct() -> None:
    assert [item.empty_type for item in shared_project_setup.DEFAULT_WORKING_LAYERS] == [
        "2d_bc",
        "2d_code",
        "2d_loc",
        "2d_rf",
    ]
    assert [item.empty_type for item in initialiser_setup.DEFAULT_WORKING_LAYERS] == [
        "2d_code",
        "2d_loc",
        "2d_rf",
    ]


@pytest.mark.parametrize("copy_utilities", [False, True])
def test_initialiser_project_uses_shared_core_with_original_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    copy_utilities: bool,
) -> None:
    monkeypatch.setattr(shared_project_setup, "_run_tuflow_empty_generation", _fake_tuflow_generation)
    templates = (
        Path(__file__).resolve().parents[4]
        / "ryan_library"
        / "orchestrators"
        / "tuflow"
        / "initialiser"
        / "resources"
        / "tuflow_templates"
    )
    prj = tmp_path / "MGA94_Zone_51.prj"
    prj.write_text(CRS.from_epsg(28351).to_wkt(), encoding="utf-8")
    tuflow_executable = tmp_path / "TUFLOW.exe"
    tuflow_executable.touch()
    utility_root: Path | None = None
    if copy_utilities:
        utility_root = tmp_path / "scripts"
        utility_file = utility_root / "gis_processing" / "rename_geopackage_layer_to_filename.py"
        utility_file.parent.mkdir(parents=True)
        utility_file.write_text("# explicit initialiser utility\n", encoding="utf-8")
    result = initialiser_setup.initialize_tuflow_project(
        initialiser_setup.TuflowProjectConfig(
            output_dir=tmp_path / "project",
            project_name="Example",
            scenario_name="EXG",
            prj_file=prj,
            tuflow_executable=tuflow_executable,
            templates_dir=templates,
            copy_utilities=copy_utilities,
            utility_source_root=utility_root,
        )
    )
    assert [path.name for path in result.working_layers] == [
        "2d_code_EXG_01_R.gpkg",
        "2d_loc_EXG_01_R.gpkg",
        "2d_rf_EXG_01_R.gpkg",
    ]
    assert not (result.project_dir / "model" / "gis" / "EXG" / "2d_loc_EXG_01_R.qml").exists()
    assert not (result.project_dir / "model" / "gis" / "EXG" / "2d_bc_EXG_01_L.gpkg").exists()
    expected_utilities = (
        (result.project_dir / "model" / "gis" / "rename_geopackage_layer_to_filename.py",) if copy_utilities else ()
    )
    assert result.copied_utilities == expected_utilities
    assert (result.project_dir / "runs" / "Example_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf").is_file()
    assert initialiser_setup.read_empty_schema(
        source_path=result.empty_dir / "2d_code_empty.gpkg",
        source_layer="2d_code_empty_R",
    ) == {"geometry": "Polygon", "properties": {"Code": "int"}}


def test_initialiser_requires_explicit_utility_root_before_writing(tmp_path: Path) -> None:
    config = initialiser_setup.TuflowProjectConfig(
        output_dir=tmp_path / "project",
        project_name="Example",
        scenario_name="EXG",
        prj_file=tmp_path / "projection.prj",
        tuflow_executable=tmp_path / "TUFLOW.exe",
        templates_dir=tmp_path / "templates",
        copy_utilities=True,
    )
    with pytest.raises(ValueError, match="utility_source_root"):
        initialiser_setup.initialize_tuflow_project(config)
    assert not config.output_dir.exists()
