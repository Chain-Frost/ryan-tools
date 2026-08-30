"""Create a small, standard TUFLOW project from maintained templates.

The workflow asks the selected TUFLOW executable to create the authoritative
``*_empty.gpkg`` files. It then creates zero-feature, geometry-specific working
layers from those schemas.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
from typing import Final, Literal, cast

import fiona  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger
import numpy as np
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]

GeometrySuffix = Literal["P", "L", "R"]
UtilityDestination = Literal[".", "model/gis", "results"]


@dataclass(frozen=True, slots=True)
class WorkingLayerSpec:
    """Describe one working layer derived from a TUFLOW empty GeoPackage."""

    empty_type: str
    geometry_suffix: GeometrySuffix


@dataclass(frozen=True, slots=True)
class ProjectUtilitySpec:
    """Describe one maintained utility copied into a generated project."""

    relative_source: Path
    destination_folder: UtilityDestination


@dataclass(frozen=True, slots=True)
class TuflowProjectConfig:
    """Inputs for :func:`initialize_tuflow_project`."""

    output_dir: Path
    project_name: str
    scenario_name: str
    prj_file: Path
    tuflow_executable: Path
    templates_dir: Path
    copy_utilities: bool = True
    overwrite: bool = False


@dataclass(frozen=True, slots=True)
class ProjectSetupResult:
    """Paths created by a successful project setup."""

    project_dir: Path
    empty_dir: Path
    working_layers: tuple[Path, ...]
    copied_utilities: tuple[Path, ...]


DEFAULT_WORKING_LAYERS: Final[tuple[WorkingLayerSpec, ...]] = (
    WorkingLayerSpec(empty_type="2d_bc", geometry_suffix="L"),
    WorkingLayerSpec(empty_type="2d_code", geometry_suffix="R"),
    WorkingLayerSpec(empty_type="2d_loc", geometry_suffix="R"),
    WorkingLayerSpec(empty_type="2d_rf", geometry_suffix="R"),
)
DEFAULT_UTILITY_SOURCE_ROOT: Final[Path] = Path(__file__).resolve().parents[3] / "ryan-scripts" / "TUFLOW-python"
_PACKAGED_UTILITY_SOURCE_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
DEFAULT_2D_LOC_STYLE_FILE: Final[Path] = (
    Path(__file__).resolve().parents[3] / "qgis-resources" / "styles" / "TUFLOW" / "2d_loc_qa_check.qml"
)
_PACKAGED_2D_LOC_STYLE_FILE: Final[Path] = (
    Path(__file__).resolve().parents[2] / "resources" / "qgis" / "tuflow" / "2d_loc_qa_check.qml"
)
_PROJECT_UTILITIES: Final[tuple[ProjectUtilitySpec, ...]] = (
    ProjectUtilitySpec(Path("gis_processing/rename_geopackage_layer_to_filename.py"), "model/gis"),
    ProjectUtilitySpec(Path("culvert_results/combine_culvert_maximums.py"), "results"),
    ProjectUtilitySpec(Path("gis_processing/apply_qgis_styles_to_results.py"), "results"),
    ProjectUtilitySpec(Path("po_and_timeseries/check_timeseries_stability.py"), "results"),
    ProjectUtilitySpec(Path("po_and_timeseries/combine_pomm_results.py"), "results"),
    ProjectUtilitySpec(Path("po_and_timeseries/create_pomm_mean_peak_report.py"), "results"),
    ProjectUtilitySpec(Path("raster_processing/run_asc_to_asc_mean_then_maximum.py"), "results"),
    ProjectUtilitySpec(Path("model_management/run_tuflow_simulations.py"), "."),
    ProjectUtilitySpec(Path("log_processing/create_log_summary_report.py"), "."),
)

_PROJECT_FOLDERS: Final[tuple[str, ...]] = (
    "bc_dbase",
    "empty",
    "model/gis",
    "results",
    "runs/log",
    "terrain",
)
_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_TEXT_LENGTH_PATTERN: Final[re.Pattern[str]] = re.compile(pattern=r"^TEXT\((\d+)\)$", flags=re.IGNORECASE)


def initialize_tuflow_project(
    config: TuflowProjectConfig,
    *,
    working_layers: tuple[WorkingLayerSpec, ...] = DEFAULT_WORKING_LAYERS,
) -> ProjectSetupResult:
    """Create project folders, templates, canonical empties, and working layers.

    Existing files are protected unless ``config.overwrite`` is true. The
    TUFLOW executable is run directly with ``-b -nmb``; no shell is involved.
    """
    _validate_config(config)
    project_dir: Path = config.output_dir.resolve()
    project_dir.mkdir(parents=True, exist_ok=True)
    _create_folders(project_dir)
    _copy_text_templates(config=config, project_dir=project_dir)
    copied_utilities: tuple[Path, ...] = (
        _copy_project_utilities(config=config, project_dir=project_dir) if config.copy_utilities else ()
    )

    gis_dir: Path = project_dir / "model" / "gis"
    _create_projection_files(
        prj_file=config.prj_file.resolve(),
        gis_dir=gis_dir,
        overwrite=config.overwrite,
    )

    empties_tcf: Path = project_dir / "runs" / "Create_Empties.tcf"
    empty_dir: Path = project_dir / "empty"
    _run_tuflow_empty_generation(
        tuflow_executable=config.tuflow_executable.resolve(),
        control_file=empties_tcf,
        expected_empty_dir=empty_dir,
    )

    scenario_dir: Path = gis_dir / config.scenario_name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    created_layers: tuple[Path, ...] = tuple(
        _create_working_layer_from_empty(
            empty_dir=empty_dir,
            scenario_dir=scenario_dir,
            scenario_name=config.scenario_name,
            spec=spec,
            overwrite=config.overwrite,
        )
        for spec in working_layers
    )
    for spec, working_layer in zip(working_layers, created_layers, strict=True):
        if spec.empty_type == "2d_loc":
            _copy_2d_loc_style(working_layer=working_layer, overwrite=config.overwrite)
    logger.success("Created TUFLOW project at {} with {} empty working layers", project_dir, len(created_layers))
    return ProjectSetupResult(
        project_dir=project_dir,
        empty_dir=empty_dir,
        working_layers=created_layers,
        copied_utilities=copied_utilities,
    )


def _validate_config(config: TuflowProjectConfig) -> None:
    for label, value in (("project name", config.project_name), ("scenario name", config.scenario_name)):
        if not _NAME_PATTERN.fullmatch(value):
            raise ValueError(f"Invalid {label} {value!r}; use letters, numbers, underscores, or hyphens.")
    if not config.prj_file.is_file():
        raise FileNotFoundError(f"Projection file not found: {config.prj_file}")
    if config.prj_file.suffix.casefold() != ".prj":
        raise ValueError(f"Projection file must use the .prj extension: {config.prj_file}")
    if not config.tuflow_executable.is_file():
        raise FileNotFoundError(f"TUFLOW executable not found: {config.tuflow_executable}")
    if not config.templates_dir.is_dir():
        raise FileNotFoundError(f"TUFLOW template directory not found: {config.templates_dir}")
    style_file: Path = _resolve_2d_loc_style_file()
    if not style_file.is_file():
        raise FileNotFoundError(f"2d_loc QML style not found: {style_file}")


def _create_folders(project_dir: Path) -> None:
    for relative_folder in _PROJECT_FOLDERS:
        folder: Path = project_dir / relative_folder
        folder.mkdir(parents=True, exist_ok=True)
        logger.debug("Ensured project directory exists: {}", folder)


def _copy_text_templates(*, config: TuflowProjectConfig, project_dir: Path) -> None:
    replacements: dict[str, str] = {
        "__project__": config.project_name,
        "__scenario__": config.scenario_name,
        "<<PROJECT_NAME>>": config.project_name,
        "<<SCENARIO_NAME>>": config.scenario_name,
        "<<PROJECTION_NAME>>": f"projection_{config.prj_file.stem}",
        "<<DATE>>": dt.datetime.now().strftime("%B %Y"),
        "<<TUFLOW_EXE>>": str(config.tuflow_executable.resolve()),
    }
    for template_file in config.templates_dir.rglob("*"):
        if not template_file.is_file() or template_file.name == "__init__.py" or "__pycache__" in template_file.parts:
            continue
        if template_file.suffix.casefold() == ".gpkg":
            raise ValueError(f"Bundled working GeoPackages are not permitted: {template_file}")

        relative_text = str(template_file.relative_to(config.templates_dir))
        for placeholder in ("__project__", "__scenario__"):
            relative_text: str = relative_text.replace(placeholder, replacements[placeholder])
        destination: Path = project_dir / relative_text
        if destination.exists() and not config.overwrite:
            raise FileExistsError(f"Refusing to overwrite existing project file: {destination}")

        content: str = template_file.read_text(encoding="utf-8")
        for placeholder, replacement in replacements.items():
            content = content.replace(placeholder, replacement)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
        logger.debug("Created project file from template: {}", destination)


def _copy_project_utilities(*, config: TuflowProjectConfig, project_dir: Path) -> tuple[Path, ...]:
    copied: list[Path] = []
    source_root: Path = _resolve_utility_source_root()
    for utility in _PROJECT_UTILITIES:
        source_file: Path = source_root / utility.relative_source
        if not source_file.is_file():
            logger.error("Project utility not found; skipping: {}", source_file)
            continue

        destination_folder: Path = project_dir / utility.destination_folder
        destination: Path = destination_folder / source_file.name
        if destination.exists() and not config.overwrite:
            logger.error("Project utility already exists and overwrite is disabled; skipping: {}", destination)
            continue
        try:
            destination_folder.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src=source_file, dst=destination)
        except OSError:
            logger.exception(
                "Could not copy project utility {} to {}; continuing project setup",
                source_file,
                destination,
            )
            continue
        copied.append(destination)
        logger.info("Copied project utility {} to {}", source_file, destination)
    return tuple(copied)


def _resolve_utility_source_root() -> Path:
    if DEFAULT_UTILITY_SOURCE_ROOT.is_dir():
        return DEFAULT_UTILITY_SOURCE_ROOT
    if all((_PACKAGED_UTILITY_SOURCE_ROOT / utility.relative_source).is_file() for utility in _PROJECT_UTILITIES):
        logger.debug(
            "Repository utility folder not found at {}; using packaged utilities from {}",
            DEFAULT_UTILITY_SOURCE_ROOT,
            _PACKAGED_UTILITY_SOURCE_ROOT,
        )
        return _PACKAGED_UTILITY_SOURCE_ROOT
    return DEFAULT_UTILITY_SOURCE_ROOT


def _resolve_2d_loc_style_file() -> Path:
    if DEFAULT_2D_LOC_STYLE_FILE.is_file():
        return DEFAULT_2D_LOC_STYLE_FILE
    if _PACKAGED_2D_LOC_STYLE_FILE.is_file():
        return _PACKAGED_2D_LOC_STYLE_FILE
    return DEFAULT_2D_LOC_STYLE_FILE


def _copy_2d_loc_style(*, working_layer: Path, overwrite: bool) -> Path:
    destination: Path = working_layer.with_suffix(".qml")
    if destination.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing 2d_loc QML style: {destination}")
    shutil.copy2(src=_resolve_2d_loc_style_file(), dst=destination)
    logger.info("Copied 2d_loc QML style to {}", destination)
    return destination


def _create_projection_files(*, prj_file: Path, gis_dir: Path, overwrite: bool) -> None:
    wkt: str = prj_file.read_text(encoding="utf-8-sig")
    projection_name: str = f"projection_{prj_file.stem}"
    projection_prj: Path = gis_dir / f"{projection_name}.prj"
    projection_gpkg: Path = gis_dir / f"{projection_name}.gpkg"
    projection_tif: Path = gis_dir / f"{projection_name}.tif"
    for path in (projection_prj, projection_gpkg, projection_tif):
        if path.exists() and not overwrite:
            raise FileExistsError(f"Refusing to overwrite existing projection file: {path}")
        if path.exists():
            path.unlink()

    shutil.copy2(src=prj_file, dst=projection_prj)
    projection_schema: dict[str, object] = {"geometry": "Point", "properties": {"id": "str"}}
    with fiona.open(  # pyright: ignore[reportUnknownMemberType]
        projection_gpkg,
        mode="w",
        driver="GPKG",
        layer=projection_name,
        schema=projection_schema,
        crs_wkt=wkt,
        encoding="UTF-8",
    ):
        pass

    transform = from_origin(west=-0.5, north=0.5, xsize=1.0, ysize=1.0)  # pyright: ignore[reportUnknownVariableType]
    with rasterio.open(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        fp=projection_tif,
        mode="w",
        driver="GTiff",
        width=1,
        height=1,
        count=1,
        dtype="float32",
        crs=wkt,
        transform=transform,
        nodata=-999.0,
        compress="deflate",
    ) as dataset:  # pyright: ignore[reportUnknownVariableType]
        dataset.write(np.zeros((1, 1, 1), dtype=np.float32))  # pyright: ignore[reportUnknownMemberType]
    logger.info("Created projection files in {}", gis_dir)


def _run_tuflow_empty_generation(*, tuflow_executable: Path, control_file: Path, expected_empty_dir: Path) -> None:
    command: list[str] = [str(tuflow_executable), "-b", "-nmb", str(control_file)]
    logger.info("Generating canonical empty GeoPackages with TUFLOW")
    completed: subprocess.CompletedProcess[str] = subprocess.run(
        args=command,
        cwd=control_file.parent,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.stdout:
        logger.debug("TUFLOW empty-file generation output:\n{}", completed.stdout.rstrip())
    if completed.returncode != 0:
        tail: str = "\n".join(completed.stdout.splitlines()[-20:])
        raise RuntimeError(f"TUFLOW empty-file generation failed with exit code {completed.returncode}.\n{tail}")
    if not expected_empty_dir.is_dir():
        raise RuntimeError(f"TUFLOW reported success but did not create the empty directory: {expected_empty_dir}")


def _create_working_layer_from_empty(
    *,
    empty_dir: Path,
    scenario_dir: Path,
    scenario_name: str,
    spec: WorkingLayerSpec,
    overwrite: bool,
) -> Path:
    source_path: Path = empty_dir / f"{spec.empty_type}_empty.gpkg"
    source_layer: str = f"{spec.empty_type}_empty_{spec.geometry_suffix}"
    target_layer: str = f"{spec.empty_type}_{scenario_name}_01_{spec.geometry_suffix}"
    target_path: Path = scenario_dir / f"{target_layer}.gpkg"
    if not source_path.is_file():
        raise FileNotFoundError(f"TUFLOW empty GeoPackage not found: {source_path}")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing working layer: {target_path}")
    if target_path.exists():
        target_path.unlink()

    schema: dict[str, object] = _read_fiona_schema(source_path=source_path, source_layer=source_layer)
    with fiona.open(  # pyright: ignore[reportUnknownMemberType]
        fp=source_path,
        layer=source_layer,
    ) as source:
        crs_wkt: str | None = cast(str | None, source.crs_wkt)  # pyright: ignore[reportUnknownMemberType]

    with fiona.open(  # pyright: ignore[reportUnknownMemberType]
        fp=target_path,
        mode="w",
        driver="GPKG",
        layer=target_layer,
        schema=schema,
        crs_wkt=crs_wkt,
        encoding="UTF-8",
    ):
        pass
    logger.info("Created empty working layer {} from {}:{}", target_path, source_path, source_layer)
    return target_path


def _read_fiona_schema(*, source_path: Path, source_layer: str) -> dict[str, object]:
    """Read a complete Fiona schema, including fields Fiona may omit."""
    uri: str = f"file:{source_path.as_posix()}?mode=ro"
    with sqlite3.connect(database=uri, uri=True) as connection:
        layer_row = connection.execute(
            "SELECT geometry_type_name FROM gpkg_geometry_columns WHERE lower(table_name) = lower(?)",
            (source_layer,),
        ).fetchone()
        if layer_row is None:
            available: list[str] = [
                str(row[0])
                for row in connection.execute(
                    "SELECT table_name FROM gpkg_contents WHERE data_type = 'features' ORDER BY table_name"
                )
            ]
            raise ValueError(
                f"Layer {source_layer!r} not found in {source_path}. Available feature layers: {available}"
            )
        geometry_name: str = _fiona_geometry_name(str(layer_row[0]))
        feature_count = connection.execute(f"SELECT count(*) FROM {_quote_sql_identifier(source_layer)}").fetchone()
        if feature_count is None or int(feature_count[0]) != 0:
            raise ValueError(
                f"Canonical TUFLOW empty layer contains features: {source_path}:{source_layer}. "
                "Refusing to create working GIS from a populated source."
            )
        columns = connection.execute(f"PRAGMA table_info({_quote_sql_identifier(source_layer)})").fetchall()

    properties: dict[str, str] = {}
    for column in columns:
        column_name = str(column[1])
        declared_type = str(column[2])
        if column_name.casefold() in {"fid", "geom"}:
            continue
        properties[column_name] = _fiona_field_type(declared_type)
    return {"geometry": geometry_name, "properties": properties}


def _quote_sql_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _fiona_geometry_name(gpkg_geometry: str) -> str:
    names: dict[str, str] = {"POINT": "Point", "LINESTRING": "LineString", "POLYGON": "Polygon"}
    try:
        return names[gpkg_geometry.upper()]
    except KeyError as exc:
        raise ValueError(f"Unsupported working-layer geometry type: {gpkg_geometry}") from exc


def _fiona_field_type(declared_type: str) -> str:
    normalized: str = declared_type.strip().upper()
    text_match: re.Match[str] | None = _TEXT_LENGTH_PATTERN.fullmatch(normalized)
    if text_match:
        return f"str:{text_match.group(1)}"
    if normalized in {"TEXT", "VARCHAR", "CHAR"}:
        return "str"
    if normalized in {"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION"}:
        return "float"
    if normalized in {"INTEGER", "INT", "SMALLINT", "MEDIUMINT", "BOOLEAN"}:
        return "int"
    if normalized == "DATE":
        return "date"
    if normalized in {"DATETIME", "TIMESTAMP"}:
        return "datetime"
    raise ValueError(f"Unsupported GeoPackage field type in TUFLOW empty schema: {declared_type!r}")
