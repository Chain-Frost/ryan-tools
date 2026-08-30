"""Focused tests for the TUFLOW project setup workflow."""

from collections.abc import Mapping
from io import StringIO
from pathlib import Path
import sqlite3
from typing import cast

import fiona  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger
import pytest
from rasterio.crs import CRS  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]

from ryan_library.orchestrators.tuflow import project_setup
from ryan_library.orchestrators.tuflow.project_setup import (
    DEFAULT_UTILITY_SOURCE_ROOT,
    TuflowProjectConfig,
    initialize_tuflow_project,
)


def _write_source_empty(
    path: Path,
    *,
    layer: str,
    geometry: str,
    properties: dict[str, str],
    feature_geometry: Mapping[str, object] | None = None,
) -> None:
    schema: dict[str, object] = {"geometry": geometry, "properties": properties}
    with fiona.open(  # pyright: ignore[reportUnknownMemberType]
        path,
        mode="w",
        driver="GPKG",
        layer=layer,
        schema=schema,
        crs="EPSG:28351",
    ) as collection:
        if feature_geometry is not None:
            values: dict[str, float | int | str] = {
                name: 1 if field_type == "int" else 1.0 if field_type == "float" else "x"
                for name, field_type in properties.items()
            }
            collection.write(  # pyright: ignore[reportUnknownMemberType]
                {"geometry": feature_geometry, "properties": values}
            )


def _fake_tuflow_generation(*, control_file: Path, expected_empty_dir: Path, **_kwargs: object) -> None:
    assert control_file.name == "Create_Empties.tcf"
    expected_empty_dir.mkdir(parents=True, exist_ok=True)
    _write_source_empty(
        expected_empty_dir / "2d_bc_empty.gpkg",
        layer="2d_bc_empty_L",
        geometry="LineString",
        properties={"Type": "str:2", "Flags": "str:3", "f": "float"},
    )
    for empty_type, properties in (
        ("2d_code", {"Code": "int"}),
        ("2d_loc", {"Comment": "str:250"}),
        ("2d_rf", {"Name": "str:100", "f1": "float"}),
    ):
        _write_source_empty(
            expected_empty_dir / f"{empty_type}_empty.gpkg",
            layer=f"{empty_type}_empty_R",
            geometry="Polygon",
            properties=properties,
        )


def test_default_utility_source_root_is_relative_to_orchestrator() -> None:
    assert DEFAULT_UTILITY_SOURCE_ROOT == (
        Path(project_setup.__file__).resolve().parents[3] / "ryan-scripts" / "TUFLOW-python"
    )


def test_utility_source_folders_are_python_packages() -> None:
    source_folders = (
        "culvert_results",
        "gis_processing",
        "log_processing",
        "model_management",
        "po_and_timeseries",
        "raster_processing",
    )
    for folder in source_folders:
        assert (DEFAULT_UTILITY_SOURCE_ROOT / folder / "__init__.py").is_file()


def test_repository_2d_loc_style_is_available() -> None:
    style_file: Path = project_setup.DEFAULT_2D_LOC_STYLE_FILE
    assert style_file.name == "2d_loc_qa_check.qml"
    assert "<qgis" in style_file.read_text(encoding="utf-8")


def test_packaged_utility_root_is_the_automatic_wheel_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packaged_root: Path = tmp_path / "installed" / "ryan_library" / "resources" / "tuflow_project_utilities"
    packaged_root.mkdir(parents=True)
    for utility in project_setup._PROJECT_UTILITIES:  # pyright: ignore[reportPrivateUsage]
        packaged_utility: Path = packaged_root / utility.relative_source
        packaged_utility.parent.mkdir(parents=True, exist_ok=True)
        packaged_utility.touch()
    monkeypatch.setattr(project_setup, "DEFAULT_UTILITY_SOURCE_ROOT", tmp_path / "missing_repository")
    monkeypatch.setattr(project_setup, "_PACKAGED_UTILITY_SOURCE_ROOT", packaged_root)
    assert project_setup._resolve_utility_source_root() == packaged_root  # pyright: ignore[reportPrivateUsage]


def test_project_layers_are_empty_derivatives_of_tuflow_empties(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    templates_dir = Path(project_setup.__file__).parents[2] / "resources" / "tuflow_templates"
    prj_file = tmp_path / "MGA94_Zone_51.prj"
    projection_wkt = cast(str, CRS.from_epsg(28351).to_wkt())  # pyright: ignore[reportUnknownMemberType]
    prj_file.write_text(projection_wkt, encoding="utf-8")
    tuflow_executable = tmp_path / "TUFLOW_iSP_w64.exe"
    tuflow_executable.touch()
    fake_generator = _fake_tuflow_generation
    utility_sources = tmp_path / "utility_sources"
    gis_utility = utility_sources / "gis_processing" / "rename_geopackage_layer_to_filename.py"
    results_utility = utility_sources / "culvert_results" / "combine_culvert_maximums.py"
    root_utility = utility_sources / "model_management" / "run_tuflow_simulations.py"
    for source in (gis_utility, results_utility, root_utility):
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"# {source.stem}\n", encoding="utf-8")
    monkeypatch.setattr(
        project_setup,
        "_run_tuflow_empty_generation",
        fake_generator,
    )
    monkeypatch.setattr(project_setup, "DEFAULT_UTILITY_SOURCE_ROOT", utility_sources)

    result = initialize_tuflow_project(
        TuflowProjectConfig(
            output_dir=tmp_path / "project",
            project_name="Example",
            scenario_name="baseModel",
            prj_file=prj_file,
            tuflow_executable=tuflow_executable,
            templates_dir=templates_dir,
        )
    )

    assert [path.name for path in result.working_layers] == [
        "2d_bc_baseModel_01_L.gpkg",
        "2d_code_baseModel_01_R.gpkg",
        "2d_loc_baseModel_01_R.gpkg",
        "2d_rf_baseModel_01_R.gpkg",
    ]
    assert result.copied_utilities == (
        result.project_dir / "model" / "gis" / "rename_geopackage_layer_to_filename.py",
        result.project_dir / "results" / "combine_culvert_maximums.py",
        result.project_dir / "run_tuflow_simulations.py",
    )
    for copied_utility in result.copied_utilities:
        assert copied_utility.read_text(encoding="utf-8") == f"# {copied_utility.stem}\n"
    loc_style: Path = result.project_dir / "model" / "gis" / "baseModel" / "2d_loc_baseModel_01_R.qml"
    style_file: Path = project_setup._resolve_2d_loc_style_file()  # pyright: ignore[reportPrivateUsage]
    assert loc_style.read_bytes() == style_file.read_bytes()
    with sqlite3.connect(result.empty_dir / "2d_bc_empty.gpkg") as source_connection:
        assert source_connection.execute('SELECT count(*) FROM "2d_bc_empty_L"').fetchone() == (0,)
    with sqlite3.connect(result.working_layers[0]) as target_connection:
        assert target_connection.execute('SELECT count(*) FROM "2d_bc_baseModel_01_L"').fetchone() == (0,)
        assert target_connection.execute(
            "SELECT geometry_type_name FROM gpkg_geometry_columns WHERE table_name = '2d_bc_baseModel_01_L'"
        ).fetchone() == ("LINESTRING",)
        columns = target_connection.execute('PRAGMA table_info("2d_bc_baseModel_01_L")').fetchall()
        assert [str(column[1]) for column in columns] == ["fid", "geom", "Type", "Flags", "f"]
    projection_dir: Path = result.project_dir / "model" / "gis"
    assert (projection_dir / "projection_MGA94_Zone_51.prj").is_file()
    assert (projection_dir / "projection_MGA94_Zone_51.gpkg").is_file()
    assert (projection_dir / "projection_MGA94_Zone_51.tif").is_file()
    with sqlite3.connect(projection_dir / "projection_MGA94_Zone_51.gpkg") as projection_connection:
        assert projection_connection.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'"
        ).fetchone() == ("projection_MGA94_Zone_51",)
    assert not (projection_dir / "projection.gpkg").exists()
    assert not (result.project_dir / "check").exists()
    assert not (result.project_dir / "model" / "xf").exists()
    assert "..\\model\\gis\\projection_MGA94_Zone_51.gpkg" in (
        result.project_dir / "runs" / "Create_Empties.tcf"
    ).read_text(encoding="utf-8")
    assert not list((result.project_dir / "model" / "gis" / "baseModel").glob("*__scenario__*"))

    runs_dir: Path = result.project_dir / "runs"
    main_tcf: Path = runs_dir / "Example_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~s4~.tcf"
    assert main_tcf.is_file()
    assert not (runs_dir / "Example_01.tcf").exists()
    tcf_text: str = main_tcf.read_text(encoding="utf-8")
    normalized_tcf_lines = tuple(" ".join(line.split()) for line in tcf_text.splitlines())
    assert any(line.startswith("Read File == ..\\runs\\run_times_01.trd") for line in normalized_tcf_lines)
    assert any(line.startswith("Read File == ..\\runs\\model_cell_sizes_01.trd") for line in normalized_tcf_lines)
    assert "..\\model\\gis\\projection_MGA94_Zone_51.gpkg" in tcf_text
    assert "..\\model\\gis\\projection_MGA94_Zone_51.tif" in tcf_text
    assert "If Event == PMP" in tcf_text
    assert any(line == "BC Database == ..\\bc_dbase\\bc_dbase_PMP_01.csv" for line in normalized_tcf_lines)
    assert any(line.startswith("Read Soils File == ..\\model\\Soil_01.tsoilf") for line in normalized_tcf_lines)

    soil_text: str = (result.project_dir / "model" / "Soil_01.tsoilf").read_text(encoding="utf-8")
    soil_lines: list[str] = soil_text.splitlines()
    assert any(line.lstrip().startswith("99,") and "NONE" in line and "No infiltration" in line for line in soil_lines)
    assert any(
        line.lstrip().startswith("100,") and "ILCL" in line and "<<InitialLoss>>" in line and "<<ContLoss>>" in line
        for line in soil_lines
    )

    geometry_text: str = (result.project_dir / "model" / "GEOM_01.tgc").read_text(encoding="utf-8")
    assert "Set Soil == 99" in geometry_text
    assert "Set Soil == 100" in geometry_text

    run_times_text: str = (runs_dir / "run_times_01.trd").read_text(encoding="utf-8")
    assert "If Event == 00015m" in run_times_text
    assert "Else If Event == 07200m" in run_times_text
    assert "End Time == 126 ! hours" in run_times_text

    cell_sizes_text: str = (runs_dir / "model_cell_sizes_01.trd").read_text(encoding="utf-8")
    assert "If Scenario == 01M" in cell_sizes_text
    assert "Else If Scenario == 100M" in cell_sizes_text
    assert "Set Variable VarCellSize == 100" in cell_sizes_text

    events_text: str = (result.project_dir / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    required_durations = (
        "00015m",
        "00030m",
        "00045m",
        "00060m",
        "00090m",
        "00120m",
        "00150m",
        "00180m",
        "00240m",
        "00270m",
        "00300m",
        "00360m",
        "00720m",
        "01080m",
        "01440m",
        "01800m",
        "02160m",
        "02880m",
        "04320m",
        "05760m",
        "07200m",
    )
    for duration in required_durations:
        assert f"Define Event == {duration}" in events_text
        assert f"BC Event Source == ~StormDuration~ | {duration}" in events_text
        assert f"BC Event Source == ~DUR~ | {duration}" in events_text


def test_invalid_scenario_name_is_rejected_before_writes(tmp_path: Path) -> None:
    templates_dir: Path = Path(project_setup.__file__).parents[2] / "resources" / "tuflow_templates"
    prj_file: Path = tmp_path / "projection.prj"
    prj_file.touch()
    tuflow_executable: Path = tmp_path / "TUFLOW_iSP_w64.exe"
    tuflow_executable.touch()
    output_dir: Path = tmp_path / "project"

    with pytest.raises(ValueError, match="Invalid scenario name"):
        initialize_tuflow_project(
            TuflowProjectConfig(
                output_dir=output_dir,
                project_name="Example",
                scenario_name="../escape",
                prj_file=prj_file,
                tuflow_executable=tuflow_executable,
                templates_dir=templates_dir,
            )
        )

    assert not output_dir.exists()


def test_missing_project_utility_is_reported_without_stopping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = tmp_path / "project"
    utility_source_root = tmp_path / "utility_sources"
    available_utility = utility_source_root / "gis_processing" / "rename_geopackage_layer_to_filename.py"
    available_utility.parent.mkdir(parents=True)
    available_utility.write_text("# available\n", encoding="utf-8")
    monkeypatch.setattr(project_setup, "DEFAULT_UTILITY_SOURCE_ROOT", utility_source_root)
    messages = StringIO()
    sink_id: int = logger.add(messages, level="ERROR", format="{message}")
    try:
        copied = project_setup._copy_project_utilities(  # pyright: ignore[reportPrivateUsage]
            config=TuflowProjectConfig(
                output_dir=project_dir,
                project_name="Example",
                scenario_name="baseModel",
                prj_file=tmp_path / "projection.prj",
                tuflow_executable=tmp_path / "TUFLOW.exe",
                templates_dir=tmp_path / "templates",
            ),
            project_dir=project_dir,
        )
    finally:
        logger.remove(sink_id)

    assert copied == (project_dir / "model" / "gis" / "rename_geopackage_layer_to_filename.py",)
    assert "Project utility not found; skipping" in messages.getvalue()
    assert str(utility_source_root / "culvert_results" / "combine_culvert_maximums.py") in messages.getvalue()


def test_populated_empty_source_is_rejected(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty"
    scenario_dir = tmp_path / "model" / "gis" / "baseModel"
    empty_dir.mkdir()
    scenario_dir.mkdir(parents=True)
    _write_source_empty(
        empty_dir / "2d_bc_empty.gpkg",
        layer="2d_bc_empty_L",
        geometry="LineString",
        properties={"Type": "str:2"},
        feature_geometry={"type": "LineString", "coordinates": ((0.0, 0.0), (1.0, 1.0))},
    )

    with pytest.raises(ValueError, match="Canonical TUFLOW empty layer contains features"):
        project_setup._create_working_layer_from_empty(  # pyright: ignore[reportPrivateUsage]
            empty_dir=empty_dir,
            scenario_dir=scenario_dir,
            scenario_name="baseModel",
            spec=project_setup.WorkingLayerSpec("2d_bc", "L"),
            overwrite=False,
        )

    assert not (scenario_dir / "2d_bc_baseModel_01_L.gpkg").exists()
