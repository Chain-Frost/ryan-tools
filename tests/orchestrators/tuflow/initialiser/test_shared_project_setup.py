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
    for layer in ("2d_bc", "2d_code", "2d_loc", "2d_rf"):
        suffix = "L" if layer == "2d_bc" else "R"
        with fiona.open(
            expected_empty_dir / f"{layer}_empty.gpkg",
            "w",
            driver="GPKG",
            layer=f"{layer}_empty_{suffix}",
            crs_wkt=CRS.from_epsg(28351).to_wkt(),
            schema={"geometry": "LineString" if suffix == "L" else "Polygon", "properties": {"Code": "int"}},
        ):
            pass


def test_both_entry_points_share_working_layers_and_utility_defaults() -> None:
    assert initialiser_setup.DEFAULT_WORKING_LAYERS is shared_project_setup.DEFAULT_WORKING_LAYERS
    assert [item.empty_type for item in initialiser_setup.DEFAULT_WORKING_LAYERS] == [
        "2d_bc",
        "2d_code",
        "2d_loc",
        "2d_rf",
    ]
    assert initialiser_setup.TuflowProjectConfig.__dataclass_fields__["copy_utilities"].default is True


@pytest.mark.parametrize("copy_utilities", [False, True])
def test_initialiser_project_matches_standard_starter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    copy_utilities: bool,
) -> None:
    monkeypatch.setattr(shared_project_setup, "_run_tuflow_empty_generation", _fake_tuflow_generation)
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
            copy_utilities=copy_utilities,
            utility_source_root=utility_root,
        )
    )
    standard = shared_project_setup.initialize_tuflow_project(
        shared_project_setup.TuflowProjectConfig(
            output_dir=tmp_path / "standard",
            project_name="Example",
            scenario_name="EXG",
            prj_file=prj,
            tuflow_executable=tuflow_executable,
            templates_dir=initialiser_setup.DEFAULT_TEMPLATES_DIR,
            copy_utilities=copy_utilities,
            utility_source_root=utility_root,
        )
    )
    initial_files = {path.relative_to(result.project_dir) for path in result.project_dir.rglob("*") if path.is_file()}
    standard_files = {
        path.relative_to(standard.project_dir) for path in standard.project_dir.rglob("*") if path.is_file()
    }
    assert initial_files == standard_files
    for relative in initial_files:
        left = result.project_dir / relative
        right = standard.project_dir / relative
        if relative.suffix == ".gpkg":
            with fiona.open(left) as initial_layer, fiona.open(right) as standard_layer:
                assert initial_layer.schema == standard_layer.schema
                assert initial_layer.crs == standard_layer.crs
                assert len(initial_layer) == len(standard_layer) == 0
        elif relative.suffix != ".tif":
            assert left.read_bytes() == right.read_bytes()
    assert [path.name for path in result.working_layers] == [
        "2d_bc_EXG_01_L.gpkg",
        "2d_code_EXG_01_R.gpkg",
        "2d_loc_EXG_01_R.gpkg",
        "2d_rf_EXG_01_R.gpkg",
    ]
    assert (result.project_dir / "model" / "gis" / "EXG" / "2d_loc_EXG_01_R.qml").exists()
    assert (result.project_dir / "model" / "gis" / "EXG" / "2d_bc_EXG_01_L.gpkg").exists()
    expected_utilities = (
        (result.project_dir / "model" / "gis" / "rename_geopackage_layer_to_filename.py",) if copy_utilities else ()
    )
    assert result.copied_utilities == expected_utilities
    boundary_text = (result.project_dir / "model" / "BOUND_01.tbc").read_text(encoding="utf-8")
    assert "    Read GIS BC ==" in boundary_text
    assert "<<BC_READ_COMMENT>>" not in boundary_text
    events_text = (result.project_dir / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    assert "BC BC Event Source ==" not in events_text
    assert (result.project_dir / "runs" / "Example_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf").is_file()
    assert initialiser_setup.read_empty_schema(
        source_path=result.empty_dir / "2d_code_empty.gpkg",
        source_layer="2d_code_empty_R",
    ) == {"geometry": "Polygon", "properties": {"Code": "int"}}


def test_shared_templates_use_active_boundary_read(tmp_path: Path) -> None:
    """Both entry points use the same active boundary read."""
    config = shared_project_setup.TuflowProjectConfig(
        output_dir=tmp_path / "unused",
        project_name="Example",
        scenario_name="EXG",
        prj_file=tmp_path / "projection.prj",
        tuflow_executable=tmp_path / "TUFLOW.exe",
        templates_dir=initialiser_setup.DEFAULT_TEMPLATES_DIR,
        copy_utilities=False,
        include_2d_loc_style=False,
    )
    project_dir = tmp_path / "generated"
    shared_project_setup._copy_text_templates(config=config, project_dir=project_dir)  # pyright: ignore[reportPrivateUsage]
    text = (project_dir / "model" / "BOUND_01.tbc").read_text(encoding="utf-8")
    assert "    Read GIS BC ==" in text
    assert "    ! Read GIS BC ==" not in text
    assert "<<BC_READ_COMMENT>>" not in text
    events = (project_dir / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    assert "BC BC Event Source ==" not in events
    tcf = next((project_dir / "runs").glob("Example_*.tcf")).read_text(encoding="utf-8")
    assert "<<OutputRoot>>\\check\\" in tcf
    assert "Write Check Files == " in tcf
