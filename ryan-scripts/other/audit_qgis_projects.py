r"""Audit QGS/QGZ sources without changing projects or their data files.

The shipped existence mode checks local file metadata without opening source
contents. Cold-storage-ping explicitly reads one unbuffered byte per distinct
local source: it may trigger network retrieval delays or charges.
Optional read-only QGD inspection reports auxiliary tables and counts; sample
rows are included only when enabled. No QGIS installation is required.

For source-path relocation, root remapping and repaired project copies, use
the separate repair_qgis_project_paths.py script.

Examples:
  python audit_qgis_projects.py --working-directory "Q:\Project\GIS" --no-pause
  python audit_qgis_projects.py --mode cold-storage-ping --no-pause
  python audit_qgis_projects.py --project Model.qgz --auxiliary-report --sample-rows 0 --no-pause
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-10-31.6"

# Project directory; Path() uses the directory from which the script was run.
# For example, set Path(r"Q:\Project\GIS") or Path(r"\\server\share\GIS").
DEFAULT_WORKING_DIR = Path()

# One .qgs/.qgz project, or None to scan every project recursively.
# For example, choose Path("Model.qgz") relative to DEFAULT_WORKING_DIR.
DEFAULT_PROJECT: Path | None = None

# "existence" checks metadata only; "cold-storage-ping" reads one byte per file.
# Cold-storage retrieval can be slow or chargeable; never enabled implicitly.
DEFAULT_AUDIT_MODE: Literal["existence", "cold-storage-ping"] = "existence"

# Explicit drive-to-UNC aliases for equivalent network shares (empty means none).
# Example: (("Q:", r"\\server\share"),) or add ("R:", r"\\other\gis").
# This does not probe mapped drives or modify the original QGIS project.
DEFAULT_DRIVE_MAPPINGS: tuple[tuple[str, str], ...] = ()

# True: read QGD table/column/record metadata; False: skip QGD inspection.
DEFAULT_AUXILIARY_REPORT = False

# Number of QGD example rows PER table (0-5). 0 does not expose stored values.
# For example, 2 requires auxiliary reporting and may disclose project data.
DEFAULT_SAMPLE_ROWS = 0

# None: retain Loguru's current console verbosity. Examples: "INFO", "DEBUG", "WARNING".
DEFAULT_CONSOLE_LOG_LEVEL: str | None = None

# True prevents an interactive completion pause; False only pauses with a console.
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
    QgisDriveMappings,
    local_source_path,
    probe_local_file,
    project_layer_sources,
    validate_drive_mappings,
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


def resolve_file_source(project_path: Path, source: str, *, drive_mappings: QgisDriveMappings = ()) -> Path | None:
    """Compatibility helper for local datasource paths (None means non-file or unsupported)."""
    return local_source_path(project_path, source, drive_mappings=drive_mappings)


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


def _audit_projects(
    projects: list[Path],
    *,
    mode: QgisAuditMode,
    auxiliary_report: bool,
    sample_rows: int,
    drive_mappings: QgisDriveMappings = (),
) -> int:
    checked: set[Path] = set()
    failed: set[Path] = set()
    project_errors = 0
    logger.info("QGIS source audit mode: {}", mode)
    if mode == "cold-storage-ping":
        logger.warning("Cold-storage ping may trigger retrieval delays or charges; reading one byte per local file")
    for project in projects:
        try:
            for layer in project_layer_sources(project):
                source = local_source_path(project, layer.uri, provider=layer.provider, drive_mappings=drive_mappings)
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
    drive_mappings: QgisDriveMappings = DEFAULT_DRIVE_MAPPINGS,
) -> int:
    """Read-only source audit; return 0 on success and 1 on processing errors."""
    directory = (working_directory if working_directory is not None else DEFAULT_WORKING_DIR).absolute()
    selected = (directory / project_path).absolute() if project_path is not None else None

    if mode not in ("existence", "cold-storage-ping"):
        logger.error("Unsupported audit mode: {}", mode)
        return 1
    if not 0 <= sample_rows <= 5 or (sample_rows and not auxiliary_report):
        logger.error("Valid --sample-rows (1-5) requires --auxiliary-report")
        return 1
    try:
        validate_drive_mappings(drive_mappings)
    except ValueError as error:
        logger.error("Invalid drive-to-UNC mapping: {}", error)
        return 1
    if not change_working_directory(target_dir=directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    projects = [selected] if selected is not None else find_qgis_projects(directory)
    if not projects:
        logger.warning("No QGS or QGZ project files found in {}", directory)
        return 0
    return _audit_projects(
        projects,
        mode=mode,
        auxiliary_report=auxiliary_report,
        sample_rows=sample_rows,
        drive_mappings=drive_mappings,
    )


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse audit-only CLI options; None means use the editable DEFAULT_* value."""
    parser = argparse.ArgumentParser(
        description="Read-only QGS/QGZ source-file audit and optional QGD report (no path repair)."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {WRAPPER_VERSION}")
    add_execution_cli_arguments(parser)
    parser.set_defaults(no_pause=None)
    parser.add_argument(
        "--pause",
        dest="no_pause",
        action="store_false",
        default=None,
        help="Permit interactive completion even when DEFAULT_NO_PAUSE=True.",
    )
    parser.add_argument("--project", type=Path, help="One QGS/QGZ project; otherwise scan the working directory.")
    parser.add_argument(
        "--drive-map",
        nargs=2,
        action="append",
        metavar=("DRIVE", "UNC_ROOT"),
        help=r"Declare mapped-drive alias, e.g. --drive-map Q: \\server\share (repeatable).",
    )
    parser.add_argument(
        "--mode",
        choices=("existence", "cold-storage-ping"),
        default=None,
        help="existence checks metadata; cold-storage-ping reads one byte per file (may incur charges).",
    )
    parser.add_argument(
        "--auxiliary-report",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Read-only QGD table, field and record counts.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        choices=range(6),
        default=None,
        help="Show 0-5 example rows per QGD table; positive values require --auxiliary-report.",
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
        drive_mappings=(
            tuple((drive, unc) for drive, unc in args.drive_map)
            if args.drive_map is not None
            else DEFAULT_DRIVE_MAPPINGS
        ),
    )
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    no_pause = args.no_pause if args.no_pause is not None else DEFAULT_NO_PAUSE
    if not no_pause:
        pause_console()
    raise SystemExit(result)
