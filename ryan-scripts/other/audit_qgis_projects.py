"""Audit QGS/QGZ sources, request one byte per local file, inspect QGD and repair paths.

Defaults: recursively inspect projects under the working directory and open/read
exactly one byte from each distinct local source file. This can rehydrate files
on tiered network storage; use --exists-only for a metadata-only audit.
Use --project for one project; --auxiliary-report inspects its QGD read-only.
Path repair is preview-only unless --write and --output are both supplied.

Examples:
  python audit_qgis_projects.py --no-pause
  python audit_qgis_projects.py --exists-only --no-pause
  python audit_qgis_projects.py --project Model.qgz --auxiliary-report --no-pause
  python audit_qgis_projects.py --project Moved.qgz --old-project-directory Q:\Old\GIS --no-pause
  python audit_qgis_projects.py --project Moved.qgz --old-project-directory Q:\Old\GIS --output Fixed.qgz --write --no-pause
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, LargeZipFile

from loguru import logger

from ryan_library.functions.qgis_auxiliary import inspect_auxiliary_storage
from ryan_library.functions.qgis_project import (
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

WRAPPER_VERSION = "2026-10-31.1"
DEFAULT_WORKING_DIR = Path()


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit and repair QGIS source paths without loading QGIS or referenced geospatial data."
    )
    add_execution_cli_arguments(parser)
    parser.add_argument("--project", type=Path, help="One QGS/QGZ project (required for path repair).")
    parser.add_argument(
        "--exists-only", action="store_true",
        help="Check file existence without opening one byte. Default opens one byte to request cold-storage retrieval.",
    )
    parser.add_argument(
        "--auxiliary-report", action="store_true",
        help="Describe auxiliary QGD SQLite tables, fields and record counts (no changes to QGD).",
    )
    parser.add_argument(
        "--sample-rows", type=int, choices=range(6), default=0,
        help="Optionally show up to five example rows per QGD table; zero omits stored values.",
    )
    parser.add_argument(
        "--old-project-directory", type=Path,
        help="Original folder of a moved project; rebase its relative file sources to their correct location.",
    )
    parser.add_argument(
        "--replace-root", nargs=2, metavar=("OLD_ROOT", "NEW_ROOT"), type=Path,
        help="Replace a file-path directory prefix, with component-boundary matching.",
    )
    parser.add_argument(
        "--absolute-paths", action="store_true",
        help="Write absolute paths for repaired file sources; default generates relative paths where possible.",
    )
    parser.add_argument("--output", type=Path, help="New project path for --write; existing files are never overwritten.")
    parser.add_argument("--write", action="store_true", help="Write approved repairs to --output; otherwise preview only.")
    return parser.parse_args(argv)


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
    logger.info(
        "QGD {}: {} bytes, {} user tables", summary.location, summary.size_bytes, len(summary.tables)
    )
    for table in summary.tables:
        logger.info(
            "  Table {} (layer: {}): {} records; fields: {}",
            table.name, table.layer_name or "unmatched", table.row_count, ", ".join(table.columns),
        )
        for values in table.sample_rows:
            logger.info("    Sample: {}", values)


def main(
    *,
    working_directory: Path | None = None,
    project_path: Path | None = None,
    exists_only: bool = False,
    auxiliary_report: bool = False,
    sample_rows: int = 0,
    old_project_directory: Path | None = None,
    replace_root: tuple[Path, Path] | None = None,
    absolute_paths: bool = False,
    output_path: Path | None = None,
    write: bool = False,
) -> int:
    """Audit or plan repairs; return 0 on success, 1 for invalid or inaccessible inputs."""
    # Interpret configured relative paths before the wrapper changes directory.
    directory = (working_directory if working_directory is not None else DEFAULT_WORKING_DIR).absolute()
    selected = project_path.absolute() if project_path is not None else None
    output = output_path.absolute() if output_path is not None else None
    old_directory = old_project_directory.absolute() if old_project_directory is not None else None
    roots = tuple(path.absolute() for path in replace_root) if replace_root is not None else None

    if not 0 <= sample_rows <= 5:
        logger.error("sample_rows must be 0 through 5")
        return 1
    if sample_rows and not auxiliary_report:
        logger.error("--sample-rows requires --auxiliary-report")
        return 1
    repair = old_directory is not None or roots is not None or absolute_paths or output is not None or write
    if repair and (selected is None or (write and output is None)):
        logger.error("Path repair requires --project; --write also requires --output")
        return 1
    if not change_working_directory(target_dir=directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    projects = [selected] if selected is not None else find_qgis_projects(directory)
    if not projects:
        logger.warning("No QGS or QGZ project files found in {}", directory)
        return 0

    if repair:
        project = projects[0]
        try:
            changes = plan_source_path_changes(
                project,
                original_project_directory=old_directory,
                replace_root=roots,
                relative=not absolute_paths,
            )
            for change in changes:
                logger.info("Layer {}: {} -> {}", change.name or change.layer_id, change.old_uri, change.new_uri)
            logger.info("Planned {} path updates for {}", len(changes), project)
            if write:
                if not changes:
                    logger.warning("No changes to write")
                    return 0
                assert output is not None
                write_project_path_changes(project, output, changes)
                logger.success("Wrote repaired project copy to {}", output)
            if auxiliary_report:
                _report_auxiliary(project, sample_rows)
            return 0
        except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError, sqlite3.DatabaseError) as error:
            logger.error("Could not repair {}: {}", project, error)
            return 1

    checked: set[Path] = set()
    failed: set[Path] = set()
    parse_failures = 0
    for project in projects:
        try:
            for layer in project_layer_sources(project):
                source = local_source_path(project, layer.uri, provider=layer.provider)
                if source is None or source in checked:
                    continue
                checked.add(source)
                success, detail = probe_local_file(source, read_one_byte=not exists_only)
                if success:
                    logger.debug("Checked {}: {}", source, detail)
                else:
                    logger.error("Source in {} (layer {}): {} ({})", project, layer.name, source, detail)
                    failed.add(source)
            if auxiliary_report:
                _report_auxiliary(project, sample_rows)
        except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError, sqlite3.DatabaseError) as error:
            logger.error("Could not inspect {}: {}", project, error)
            parse_failures += 1

    logger.info("Checked {} distinct local data files; failed: {}; project errors: {}", len(checked), len(failed), parse_failures)
    if not failed and not parse_failures:
        logger.success("All inspected QGIS file sources passed")
    return 1 if failed or parse_failures else 0


if __name__ == "__main__":
    args = _parse_cli_arguments()
    if args.console_log_level:
        logger.remove()
        logger.add(sys.stderr, level=args.console_log_level.upper())

    result = main(
        working_directory=args.working_directory,
        project_path=args.project,
        exists_only=args.exists_only,
        auxiliary_report=args.auxiliary_report,
        sample_rows=args.sample_rows,
        old_project_directory=args.old_project_directory,
        replace_root=tuple(args.replace_root) if args.replace_root is not None else None,
        absolute_paths=args.absolute_paths,
        output_path=args.output,
        write=args.write,
    )
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not getattr(args, "no_pause", False):
        pause_console()
    raise SystemExit(result)
