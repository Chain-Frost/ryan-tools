"""Use the shared starter project before adding initialiser-generated model data."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from .. import project_setup as shared_project_setup
from ..project_setup import ProjectSetupResult, WorkingLayerSpec
from .gis_types import LayerSchema

DEFAULT_TEMPLATES_DIR: Final[Path] = Path(__file__).absolute().parents[3] / "resources" / "tuflow_templates"

DEFAULT_WORKING_LAYERS: Final[tuple[WorkingLayerSpec, ...]] = shared_project_setup.DEFAULT_WORKING_LAYERS


@dataclass(frozen=True, slots=True)
class TuflowProjectConfig:
    """Inputs for the shared starter project."""

    output_dir: Path
    project_name: str
    scenario_name: str
    prj_file: Path
    tuflow_executable: Path
    templates_dir: Path = DEFAULT_TEMPLATES_DIR
    copy_utilities: bool = True
    utility_source_root: Path | None = None
    overwrite: bool = False


def initialize_tuflow_project(
    config: TuflowProjectConfig,
    *,
    working_layers: tuple[WorkingLayerSpec, ...] = DEFAULT_WORKING_LAYERS,
) -> ProjectSetupResult:
    """Create the same starter project as the standard setup entry point."""
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
    )
    return shared_project_setup.initialize_tuflow_project(shared_config, working_layers=working_layers)


def read_empty_schema(*, source_path: Path, source_layer: str) -> LayerSchema:
    """Read a canonical TUFLOW empty-layer schema for the migrated verifier."""
    return cast(
        "LayerSchema",
        shared_project_setup.read_empty_schema(source_path=source_path, source_layer=source_layer),
    )
