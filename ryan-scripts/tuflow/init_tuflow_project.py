r"""Create a compact, standard TUFLOW project.

The wrapper creates folders and control-file templates, asks TUFLOW to write
its canonical empty GeoPackages, then derives the initial scenario GIS layers
from those empty schemas. Existing files are protected unless ``--overwrite``
is supplied.

Examples:
    python init_tuflow_project.py --output E:\Projects\MyProject\tuflow --name MyProject ^
        --scenario bigModel --prj E:\Projects\MyProject\MGA2020_50.prj --no-pause
    python init_tuflow_project.py --help
"""

from __future__ import annotations

import argparse
from pathlib import Path

WRAPPER_VERSION = "2026-08-31.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent

# Editable defaults. CLI arguments override these values.
DEFAULT_OUTPUT_DIR = Path(".")
DEFAULT_PROJECT_NAME = "New_Project"
DEFAULT_SCENARIO_NAME = "bigModel"
DEFAULT_TUFLOW_EXE = Path(r"C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe")
DEFAULT_OVERWRITE = False
DEFAULT_COPY_UTILITIES = True

from loguru import logger

from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_execution_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.project_setup import (
    ProjectSetupResult,
    TuflowProjectConfig,
    initialize_tuflow_project,
)
import ryan_library.resources


def _find_templates_dir() -> Path:
    """Find the project templates in the installed or development package."""
    installed_dir: Path = Path(ryan_library.resources.__file__).parent / "tuflow_templates"
    if installed_dir.is_dir():
        return installed_dir
    repository_dir: Path = Path(__file__).resolve().parents[2] / "ryan_library" / "resources" / "tuflow_templates"
    if repository_dir.is_dir():
        return repository_dir
    raise FileNotFoundError(
        f"TUFLOW templates not found at {installed_dir} or {repository_dir}. Rebuild and install ryan_functions."
    )


def main(
    *,
    output_dir: Path,
    project_name: str,
    scenario_name: str,
    prj_file: Path,
    tuflow_executable: Path,
    overwrite: bool,
    copy_utilities: bool,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
) -> int:
    """Initialize one TUFLOW project and return a process exit code."""
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    configure_serial_logging(console_log_level=console_log_level or CONSOLE_LOG_LEVEL, log_file=None)
    try:
        result: ProjectSetupResult = initialize_tuflow_project(
            config=TuflowProjectConfig(
                output_dir=output_dir,
                project_name=project_name,
                scenario_name=scenario_name,
                prj_file=prj_file,
                tuflow_executable=tuflow_executable,
                templates_dir=_find_templates_dir(),
                copy_utilities=copy_utilities,
                overwrite=overwrite,
            )
        )
    except Exception:
        logger.exception("TUFLOW project setup failed")
        return 1

    logger.success("Project setup complete: {}", result.project_dir)
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a TUFLOW project and derive starter GIS layers from TUFLOW-generated empty GeoPackages.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--output", type=Path, help="Project directory. Defaults to DEFAULT_OUTPUT_DIR.")
    parser.add_argument("--name", help="Project name. Defaults to DEFAULT_PROJECT_NAME.")
    parser.add_argument("--scenario", help="Initial scenario name. Defaults to DEFAULT_SCENARIO_NAME.")
    parser.add_argument("--prj", type=Path, required=True, help="Required projection .prj file.")
    parser.add_argument("--tuflow-exe", type=Path, help="TUFLOW executable used to generate canonical empty files.")
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow replacement of project files created by this workflow. Defaults to DEFAULT_OVERWRITE.",
    )
    parser.add_argument(
        "--copy-utilities",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Copy the standard ryan-tools utilities into the project. Defaults to DEFAULT_COPY_UTILITIES.",
    )
    add_execution_cli_arguments(parser)
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    common_options: CommonWrapperOptions = parse_common_cli_arguments(args)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    result: int = main(
        output_dir=args.output if args.output is not None else DEFAULT_OUTPUT_DIR,
        project_name=args.name if args.name is not None else DEFAULT_PROJECT_NAME,
        scenario_name=args.scenario if args.scenario is not None else DEFAULT_SCENARIO_NAME,
        prj_file=args.prj,
        tuflow_executable=args.tuflow_exe if args.tuflow_exe is not None else DEFAULT_TUFLOW_EXE,
        overwrite=args.overwrite if args.overwrite is not None else DEFAULT_OVERWRITE,
        copy_utilities=args.copy_utilities if args.copy_utilities is not None else DEFAULT_COPY_UTILITIES,
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
