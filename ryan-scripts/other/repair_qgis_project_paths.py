r"""Create a new repaired QGS/QGZ project copy without altering its original.

This script remaps references inside QGS/QGZ projects without moving or reading
the referenced raster/vector data. It requires an explicit input project and
writes a separate `<project>_repaired.qgs/.qgz` file by default, beside the
original. Existing project and companion QGD files are never overwritten.
Use --no-write to preview without writing. Relative references are rebased
against the output project's folder. No QGIS installation is required.

For source existence checks, cold-storage one-byte pings, or read-only QGD
inspection, use the separate audit_qgis_projects.py script.

Examples:
  python repair_qgis_project_paths.py --project Moved.qgz --old-project-directory "Q:\Old\GIS" --no-write --no-pause
  python repair_qgis_project_paths.py --project Moved.qgz --replace-root "Q:\Old_Data" "Q:\New_Data" --no-pause
  python repair_qgis_project_paths.py --project Moved.qgz --old-project-directory "Q:\Old\GIS" --output Fixed.qgz --no-pause
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-31.2"

# All paths may be absolute or relative to DEFAULT_WORKING_DIR. Use raw Windows
# strings such as Path(r"Q:\Project\GIS") or Path(r"\\server\share\GIS").
# Directory containing the selected project (Path() uses the initial working directory).
DEFAULT_WORKING_DIR = Path()

# One .qgs/.qgz project is required. None means it must be supplied via --project.
# For example, select Path("Moved.qgz") or Path(r"Q:\Project\GIS\Moved.qgs").
DEFAULT_PROJECT: Path | None = None

# Previously used directory for a relocated project's RELATIVE data references.
# None means original references are based on the project's current directory.
# For example, set to Path(r"Q:\Previous_Project\GIS") after moving the QGS/QGZ.
DEFAULT_OLD_PROJECT_DIRECTORY: Path | None = None

# Optional (old data root, new data root) prefix substitution; None means no mapping.
# For example, set to (Path(r"Q:\Old_Data"), Path(r"Q:\New_Data")).
# This only rewrites references; it never moves or opens the source datasets.
DEFAULT_REPLACE_ROOT: tuple[Path, Path] | None = None

# False writes relative source paths from the output project's folder; True uses
# absolute source paths (less portable). Example: True for fixed drive locations.
DEFAULT_ABSOLUTE_PATHS = False

# Optional custom output path. None automatically names the copy <project>_repaired.qgs/.qgz
# BESIDE the selected input project, regardless of DEFAULT_WORKING_DIR.
# For example, set to Path("Moved_fixed.qgz") or Path(r"Q:\Project\GIS\Fixed.qgs").
# The output folder must exist and extension must match the input; existing files are protected.
DEFAULT_OUTPUT: Path | None = None

# True (shipped default): write a NEW output project only if at least one source
# reference changes. False: preview without writing. Never modifies the input.
# --no-write and --write override this setting on the CLI.
DEFAULT_WRITE = True

# None retains Loguru's console level; examples: "INFO", "DEBUG", "WARNING", "ERROR".
DEFAULT_CONSOLE_LOG_LEVEL: str | None = None

# True exits without pausing; False pauses only if an interactive console exists.
# --no-pause and --pause override this setting.
DEFAULT_NO_PAUSE = False

import argparse
import sys
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, LargeZipFile

from loguru import logger

from ryan_library.functions.qgis_project import plan_source_path_changes, write_project_path_changes
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def main(
    *,
    working_directory: Path | None = None,
    project_path: Path | None = DEFAULT_PROJECT,
    old_project_directory: Path | None = DEFAULT_OLD_PROJECT_DIRECTORY,
    replace_root: tuple[Path, Path] | None = DEFAULT_REPLACE_ROOT,
    absolute_paths: bool = DEFAULT_ABSOLUTE_PATHS,
    output_path: Path | None = DEFAULT_OUTPUT,
    write: bool = DEFAULT_WRITE,
) -> int:
    """Create a separate repaired project by default, or preview with write=False."""
    directory = (working_directory if working_directory is not None else DEFAULT_WORKING_DIR).absolute()
    if project_path is None:
        logger.error("Path repair requires one --project (or an edited DEFAULT_PROJECT).")
        return 1
    project = (directory / project_path).absolute()
    output = (
        (directory / output_path).absolute()
        if output_path is not None
        else project.with_name(f"{project.stem}_repaired{project.suffix}")
    )
    old_directory = (directory / old_project_directory).absolute() if old_project_directory is not None else None
    roots = (
        ((directory / replace_root[0]).absolute(), (directory / replace_root[1]).absolute())
        if replace_root is not None
        else None
    )
    if not change_working_directory(target_dir=directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        changes = plan_source_path_changes(
            project,
            original_project_directory=old_directory,
            replace_root=roots,
            relative=not absolute_paths,
            output_project_path=output,
        )
        for change in changes:
            logger.info("Layer {}: {} -> {}", change.name or change.layer_id, change.old_uri, change.new_uri)
        logger.info("Planned {} path updates for {}", len(changes), project)
        if write and changes:
            write_project_path_changes(project, output, changes)
            logger.success("Wrote repaired project copy to {}", output)
        elif write:
            logger.warning("No changes to write")
    except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError) as error:
        logger.error("Could not repair {}: {}", project, error)
        return 1
    return 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse repair-only CLI arguments; None means use the matching DEFAULT_*."""
    parser = argparse.ArgumentParser(
        description="Create a NEW repaired QGS/QGZ copy by default; use --no-write to preview only."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {WRAPPER_VERSION}")
    add_execution_cli_arguments(parser)
    parser.set_defaults(no_pause=None)
    parser.add_argument(
        "--pause",
        dest="no_pause",
        action="store_false",
        default=None,
        help="Allow interactive completion, overriding DEFAULT_NO_PAUSE=True.",
    )
    parser.add_argument("--project", type=Path, help="One QGS/QGZ project to repair (or edit DEFAULT_PROJECT).")
    parser.add_argument(
        "--old-project-directory",
        type=Path,
        help="Original directory to resolve a relocated project's prior relative sources.",
    )
    parser.add_argument(
        "--replace-root",
        nargs=2,
        metavar=("OLD_ROOT", "NEW_ROOT"),
        type=Path,
        help="Rewrite matching source-path prefixes only; do not move data.",
    )
    parser.add_argument(
        "--absolute-paths",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Write absolute instead of project-relative source references.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="New copy path (default: INPUT_repaired.qgs/.qgz beside input); existing files cannot be overwritten.",
    )
    parser.add_argument(
        "--write",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Write a new project copy by default; --no-write previews only.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_cli_arguments()
    console_log_level = args.console_log_level if args.console_log_level is not None else DEFAULT_CONSOLE_LOG_LEVEL
    if console_log_level is not None:
        logger.remove()
        logger.add(sys.stderr, level=console_log_level.upper())

    result = main(
        working_directory=args.working_directory,
        project_path=args.project if args.project is not None else DEFAULT_PROJECT,
        old_project_directory=(
            args.old_project_directory if args.old_project_directory is not None else DEFAULT_OLD_PROJECT_DIRECTORY
        ),
        replace_root=(
            (args.replace_root[0], args.replace_root[1]) if args.replace_root is not None else DEFAULT_REPLACE_ROOT
        ),
        absolute_paths=args.absolute_paths if args.absolute_paths is not None else DEFAULT_ABSOLUTE_PATHS,
        output_path=args.output if args.output is not None else DEFAULT_OUTPUT,
        write=args.write if args.write is not None else DEFAULT_WRITE,
    )
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    no_pause = args.no_pause if args.no_pause is not None else DEFAULT_NO_PAUSE
    if not no_pause:
        pause_console()
    raise SystemExit(result)
