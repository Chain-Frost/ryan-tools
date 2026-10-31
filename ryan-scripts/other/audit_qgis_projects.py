r"""Audit QGS/QGZ sources in explicit existence or cold-storage-ping modes.

The shipped DEFAULT_AUDIT_MODE is existence: check source-file metadata
without reading contents. Set DEFAULT_AUDIT_MODE to cold-storage-ping, or use
--mode cold-storage-ping, to request one unbuffered byte per distinct local
file. This can be slow or incur tiered-storage retrieval charges.
No QGIS installation or geospatial file loading is required.

--project selects one QGS/QGZ; --auxiliary-report inspects QGD SQLite data.
Path repair previews changes unless writing is enabled via DEFAULT_WRITE
or --write, and a new output path is supplied via DEFAULT_OUTPUT or --output.
Existing project files are never overwritten.

Examples:
  python audit_qgis_projects.py --mode existence --no-pause
  python audit_qgis_projects.py --mode cold-storage-ping --no-pause
  python audit_qgis_projects.py --project Model.qgz --auxiliary-report --no-pause
  python audit_qgis_projects.py --project Moved.qgz --old-project-directory Q:\Old\GIS --no-pause
  python audit_qgis_projects.py --project Moved.qgz --old-project-directory Q:\Old\GIS --output Fixed.qgz --write --no-pause
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

# User-editable configuration. CLI arguments override these values when supplied.
# Paths below may be absolute, or relative to DEFAULT_WORKING_DIR.
# For Windows mapped drives and UNC shares, use raw strings to preserve "\":
# Path(r"Q:\Project\GIS") or Path(r"\\server\share\Project\GIS").
WRAPPER_VERSION = "2026-10-31.5"

# --- Project selection ---
# Directory to scan; Path() means the current directory when the script starts.
# For example, set this directory to Path(r"Q:\Project\GIS") to scan that mapped-drive folder.
DEFAULT_WORKING_DIR = Path()

# Single .qgs/.qgz project, or None to scan all projects under the working directory recursively.
# For example, select Path("Hydraulic_Model.qgz") or Path(r"Q:\Project\GIS\Hydraulic_Model.qgs").
DEFAULT_PROJECT: Path | None = None

# --- Source-file audit ---
# Allowed: "existence" (metadata only) or "cold-storage-ping" (open/read 1 byte per local file).
# Example: "cold-storage-ping" to request network cold-storage retrieval; this can incur delays or charges.
# Do not combine cold-storage-ping with path repair; run those operations separately.
DEFAULT_AUDIT_MODE: Literal["existence", "cold-storage-ping"] = "existence"

# --- QGIS auxiliary data (.qgd) ---
# True: read QGD SQLite tables, fields and counts (may cause network I/O); False: skip QGD reads.
# Example: True when investigating saved label positions or other auxiliary attributes.
DEFAULT_AUXILIARY_REPORT = False

# Number of example rows PER QGD table: integer from 0 to 5. Zero hides all stored values.
# Example: 2; positive values require auxiliary reporting enabled (default or CLI) and may expose project data.
DEFAULT_SAMPLE_ROWS = 0

# --- Source-path repair (set DEFAULT_PROJECT or pass --project to select one project) ---
# Original directory of a MOVED project, used to interpret its old relative data-source paths.
# None: assume relative sources are based on the project's CURRENT directory.
# Example: Path(r"Q:\Original_Project\GIS") when the project file was relocated.
DEFAULT_OLD_PROJECT_DIRECTORY: Path | None = None

# Optional prefix mapping for data-source references: (OLD_ROOT, NEW_ROOT), not a file move.
# None: no root replacement. Example: (Path(r"Q:\Old_Data"), Path(r"Q:\New_Data"))
# Only matching paths are remapped; the referenced source data is never modified.
DEFAULT_REPLACE_ROOT: tuple[Path, Path] | None = None

# False: generate relative paths from the output project's directory (more portable).
# True: render absolute source paths in preview/output (less portable, but independent of project location).
DEFAULT_ABSOLUTE_PATHS = False

# Destination for a repaired project, or None for preview-only operation without a named output.
# Example: Path("Hydraulic_Model_fixed.qgz") or Path(r"Q:\Project\GIS\Hydraulic_Model_fixed.qgs").
# Directory must exist; extension must match the selected input project; existing outputs are never overwritten.
DEFAULT_OUTPUT: Path | None = None

# False: preview path changes only. True: create a NEW project file at the configured output path.
# Example: True, after selecting a project and output through defaults or CLI; never overwrites files.
# CLI --write and --no-write can override this setting.
DEFAULT_WRITE = False

# --- Logging and interactive completion ---
# None: keep Loguru's existing console level. Examples: "INFO", "DEBUG", "WARNING", "ERROR", "SUCCESS".
# CLI --console-log-level overrides this value.
DEFAULT_CONSOLE_LOG_LEVEL: str | None = None

# False: offer an interactive pause on completion when a console is attached.
# True: never pause (e.g. automated or scheduled runs); CLI --no-pause/--pause overrides this value.
DEFAULT_NO_PAUSE = False

import argparse
import os
import sqlite3
import sys
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, LargeZipFile

from loguru import logger

from ryan_library.functions.qgis_auxiliary import inspect_auxiliary_storage
from ryan_library.functions.qgis_project import (
    QgisAuditMode,
    local_source_path,
    plan_source_path_changes,
    probe_local_file,
    project_layer_sources,
    write_project_path_changes,
)
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def find_qgis_projects(start_dir: Path) -> list[Path]:
    """Find all .qgs and .qgz projects in stable order."""
    projects: list[Path] = []
    for root, _, names in os.walk(start_dir):
        projects.extend(Path(root) / name for name in names if name.lower().endswith((".qgs", ".qgz")))
    return sorted(projects)


def get_data_sources(project_path: Path) -> list[str]:
    """Keep the original deduplicated, ordered datasource query interface."""
    return sorted({layer.uri for layer in project_layer_sources(project_path) if layer.uri})


def resolve_file_source(project_path: Path, source: str) -> Path | None:
    """Compatibility helper for local datasource paths (None means non-file or unsupported)."""
    return local_source_path(project_path, source)


def _report_auxiliary(project_path: Path, sample_rows: int) -> None:
    summary = inspect_auxiliary_storage(project_path, sample_rows=sample_rows)
    if summary is None:
        logger.info("No QGD auxiliary storage associated with {}", project_path)
        return
    logger.info("QGD {}: {} bytes, {} user tables", summary.location, summary.size_bytes, len(summary.tables))
    for table in summary.tables:
        logger.info(
            "  Table {} (layer: {}): {} records; fields: {}",
            table.name,
            table.layer_name or "unmatched",
            table.row_count,
            ", ".join(table.columns),
        )
        for values in table.sample_rows:
            logger.info("    Sample: {}", values)


def _repair_project(
    project: Path,
    *,
    old_directory: Path | None,
    roots: tuple[Path, Path] | None,
    absolute_paths: bool,
    output: Path | None,
    write: bool,
    auxiliary_report: bool,
    sample_rows: int,
) -> int:
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
            if output is None:
                logger.error("An output path is required for --write")
                return 1
            write_project_path_changes(project, output, changes)
            logger.success("Wrote repaired project copy to {}", output)
        if write and not changes:
            logger.warning("No changes to write")
        if auxiliary_report:
            _report_auxiliary(project, sample_rows)
    except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError, sqlite3.DatabaseError) as error:
        logger.error("Could not repair {}: {}", project, error)
        return 1
    return 0


def _audit_projects(projects: list[Path], *, mode: QgisAuditMode, auxiliary_report: bool, sample_rows: int) -> int:
    checked: set[Path] = set()
    failed: set[Path] = set()
    project_errors = 0
    logger.info("QGIS source audit mode: {}", mode)
    if mode == "cold-storage-ping":
        logger.warning("Cold-storage ping may trigger retrieval delays or charges; reading one byte per local file")
    for project in projects:
        try:
            for layer in project_layer_sources(project):
                source = local_source_path(project, layer.uri, provider=layer.provider)
                if source is None or source in checked:
                    continue
                checked.add(source)
                success, detail = probe_local_file(source, mode=mode)
                if success:
                    logger.debug("Checked {}: {}", source, detail)
                else:
                    logger.error("Source in {} (layer {}): {} ({})", project, layer.name, source, detail)
                    failed.add(source)
            if auxiliary_report:
                _report_auxiliary(project, sample_rows)
        except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError, sqlite3.DatabaseError) as error:
            logger.error("Could not inspect {}: {}", project, error)
            project_errors += 1

    logger.info(
        "Checked {} distinct local data files; failed: {}; project errors: {}",
        len(checked),
        len(failed),
        project_errors,
    )
    if not failed and not project_errors:
        logger.success("All inspected QGIS file sources passed")
    return 1 if failed or project_errors else 0


def main(
    *,
    working_directory: Path | None = None,
    project_path: Path | None = DEFAULT_PROJECT,
    mode: QgisAuditMode = DEFAULT_AUDIT_MODE,
    auxiliary_report: bool = DEFAULT_AUXILIARY_REPORT,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    old_project_directory: Path | None = DEFAULT_OLD_PROJECT_DIRECTORY,
    replace_root: tuple[Path, Path] | None = DEFAULT_REPLACE_ROOT,
    absolute_paths: bool = DEFAULT_ABSOLUTE_PATHS,
    output_path: Path | None = DEFAULT_OUTPUT,
    write: bool = DEFAULT_WRITE,
) -> int:
    """Audit or plan repairs; return 0 on success, 1 for invalid or inaccessible inputs."""
    directory = (working_directory if working_directory is not None else DEFAULT_WORKING_DIR).absolute()
    selected = (directory / project_path).absolute() if project_path is not None else None
    output = (directory / output_path).absolute() if output_path is not None else None
    old_directory = (directory / old_project_directory).absolute() if old_project_directory is not None else None
    roots = (
        ((directory / replace_root[0]).absolute(), (directory / replace_root[1]).absolute())
        if replace_root is not None
        else None
    )

    if mode not in ("existence", "cold-storage-ping"):
        logger.error("Unsupported audit mode: {}", mode)
        return 1
    if not 0 <= sample_rows <= 5 or (sample_rows and not auxiliary_report):
        logger.error("Valid --sample-rows (1-5) requires --auxiliary-report")
        return 1

    repair = old_directory is not None or roots is not None or absolute_paths or output is not None or write
    if repair and (selected is None or (write and output is None)):
        logger.error("Path repair requires --project; --write also requires --output")
        return 1
    if repair and mode == "cold-storage-ping":
        logger.error("Cold-storage ping is an audit mode; run path repair separately")
        return 1
    if not change_working_directory(target_dir=directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    projects = [selected] if selected is not None else find_qgis_projects(directory)
    if not projects:
        logger.warning("No QGS or QGZ project files found in {}", directory)
        return 0
    if repair:
        return _repair_project(
            projects[0],
            old_directory=old_directory,
            roots=roots,
            absolute_paths=absolute_paths,
            output=output,
            write=write,
            auxiliary_report=auxiliary_report,
            sample_rows=sample_rows,
        )
    return _audit_projects(
        projects,
        mode=mode,
        auxiliary_report=auxiliary_report,
        sample_rows=sample_rows,
    )


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse explicit CLI overrides; None means use the editable DEFAULT_* setting."""
    parser = argparse.ArgumentParser(
        description="Audit and repair QGIS source paths without loading QGIS or referenced geospatial data."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {WRAPPER_VERSION}")
    add_execution_cli_arguments(parser)
    # The shared --no-pause switch defaults to False; use None here so an
    # edited DEFAULT_NO_PAUSE can be distinguished from a CLI override.
    parser.set_defaults(no_pause=None)
    parser.add_argument(
        "--pause",
        dest="no_pause",
        action="store_false",
        default=None,
        help="Allow interactive completion, overriding DEFAULT_NO_PAUSE=True.",
    )
    parser.add_argument("--project", type=Path, help="One QGS/QGZ project (required for path repair).")
    parser.add_argument(
        "--mode",
        choices=("existence", "cold-storage-ping"),
        default=None,
        help="existence (default) checks metadata; cold-storage-ping explicitly reads one byte (may incur charges).",
    )
    parser.add_argument(
        "--auxiliary-report",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Report QGD SQLite tables, fields and counts without modifying auxiliary data.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        choices=range(6),
        default=None,
        help="Show up to five example rows per QGD table; zero hides stored values.",
    )
    parser.add_argument(
        "--old-project-directory",
        type=Path,
        help="Folder containing the original project before relocation.",
    )
    parser.add_argument(
        "--replace-root",
        nargs=2,
        metavar=("OLD_ROOT", "NEW_ROOT"),
        type=Path,
        help="Map a directory prefix with component-boundary matching.",
    )
    parser.add_argument(
        "--absolute-paths",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use absolute paths for repaired file sources instead of relative paths.",
    )
    parser.add_argument("--output", type=Path, help="New output project path; existing files are never overwritten.")
    parser.add_argument(
        "--write",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Create a new output project; --no-write overrides DEFAULT_WRITE=True.",
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
        mode=args.mode if args.mode is not None else DEFAULT_AUDIT_MODE,
        auxiliary_report=(args.auxiliary_report if args.auxiliary_report is not None else DEFAULT_AUXILIARY_REPORT),
        sample_rows=args.sample_rows if args.sample_rows is not None else DEFAULT_SAMPLE_ROWS,
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
