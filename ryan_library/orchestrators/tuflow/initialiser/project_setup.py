"""Initialiser defaults backed by the maintained shared TUFLOW project generator.

The initialiser creates three GIS working layers, does not copy utilities by
default, and does not install a 2d_loc QML style. The legacy project setup
continues to expose its four-layer and style/utility defaults.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from ryan_library.orchestrators.tuflow import project_setup as shared_project_setup
from ryan_library.orchestrators.tuflow.project_setup import (
    ProjectSetupResult as ProjectSetupResult,
    ProjectUtilitySpec as ProjectUtilitySpec,
    WorkingLayerSpec as WorkingLayerSpec,
)

from .gis_types import LayerSchema


@dataclass(frozen=True, slots=True)
class TuflowProjectConfig:
    """Configuration for the original initialiser project profile."""

    output_dir: Path
    project_name: str
    scenario_name: str
    prj_file: Path
    tuflow_executable: Path
    templates_dir: Path
    copy_utilities: bool = False
    utility_source_root: Path | None = None
    overwrite: bool = False


DEFAULT_WORKING_LAYERS: Final[tuple[WorkingLayerSpec, ...]] = (
    WorkingLayerSpec(empty_type="2d_code", geometry_suffix="R"),
    WorkingLayerSpec(empty_type="2d_loc", geometry_suffix="R"),
    WorkingLayerSpec(empty_type="2d_rf", geometry_suffix="R"),
)
DEFAULT_UTILITY_SOURCE_ROOT: Final[Path] = Path("__ryan_tools_not_configured__")
DEFAULT_2D_LOC_STYLE_FILE: Final[Path] = Path(__file__).absolute().parent / "resources" / "2d_loc_qa_check.qml"
_PACKAGED_2D_LOC_STYLE_FILE: Final[Path] = DEFAULT_2D_LOC_STYLE_FILE


def initialize_tuflow_project(
    config: TuflowProjectConfig,
    *,
    working_layers: tuple[WorkingLayerSpec, ...] = DEFAULT_WORKING_LAYERS,
) -> ProjectSetupResult:
    """Create TUFLOW empties through the shared generator using initialiser defaults."""
    if config.copy_utilities and (config.utility_source_root is None or not config.utility_source_root.is_dir()):
        msg = "copy_utilities requires a valid utility_source_root"
        raise ValueError(msg)
    shared_config = shared_project_setup.TuflowProjectConfig(
        output_dir=config.output_dir,
        project_name=config.project_name,
        scenario_name=config.scenario_name,
        prj_file=config.prj_file,
        tuflow_executable=config.tuflow_executable,
        templates_dir=config.templates_dir,
        copy_utilities=config.copy_utilities,
        overwrite=config.overwrite,
        utility_source_root=config.utility_source_root,
        include_2d_loc_style=False,
    )
    return shared_project_setup.initialize_tuflow_project(shared_config, working_layers=working_layers)


def read_empty_schema(*, source_path: Path, source_layer: str) -> LayerSchema:
    """Read the shared TUFLOW empty-layer schema for the migrated verifier."""
    return cast("LayerSchema", shared_project_setup.read_empty_schema(source_path=source_path, source_layer=source_layer))
