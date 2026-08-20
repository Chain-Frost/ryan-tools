"""
Audit QGIS Project (.qgz) files to find broken data source paths.

This script scans a directory for .qgz files, extracts the layer data sources,
and checks if they exist on disk. It reports any missing or broken paths.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree
from zipfile import ZipFile

WRAPPER_VERSION = "2026-08-20.1"
DEFAULT_WORKING_DIR = Path(".")

from loguru import logger
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit QGIS Project files (.qgz, .qgs) to find broken data source paths."
    )
    add_execution_cli_arguments(parser)
    return parser.parse_args(argv)


def find_qgis_projects(start_dir: Path) -> list[Path]:
    """Find all .qgz and .qgs files in the directory tree."""
    qgis_files: list[Path] = []
    for root, _, files in os.walk(start_dir):
        for f in files:
            lower_f = f.lower()
            if (lower_f.endswith(".qgz") or lower_f.endswith(".qgs")) and "ss" not in lower_f:
                qgis_files.append(Path(root) / f)
    return qgis_files


def get_data_sources(project_path: Path) -> list[str]:
    """Extract all data sources from a .qgz or .qgs file."""
    data_sources: set[str] = set()
    if project_path.suffix.lower() == ".qgz":
        with ZipFile(project_path, "r") as in_qgz:
            qgs_members = [member for member in in_qgz.infolist() if member.filename.lower().endswith(".qgs")]
            if not qgs_members:
                raise ValueError(f"No .qgs document found inside {project_path}")
            tree = ElementTree.XML(in_qgz.read(qgs_members[0].filename))
    else:
        tree = ElementTree.parse(project_path).getroot()

    for element in tree.findall("./projectlayers/maplayer/datasource"):
        if element.text:
            data_sources.add(element.text)

    return list(data_sources)


def resolve_file_source(project_path: Path, source: str) -> Path | None:
    """Resolve a local QGIS data source, or return None for non-file providers."""
    raw_path = source.split("|", maxsplit=1)[0].strip()
    lowered = raw_path.lower()
    if lowered.startswith(("memory?", "crs=", "dbname=", "url=", "http://", "https://")):
        return None

    if lowered.startswith("file://"):
        parsed_path = unquote(urlparse(raw_path).path)
        if len(parsed_path) >= 3 and parsed_path[0] == "/" and parsed_path[2] == ":":
            parsed_path = parsed_path[1:]
        candidate = Path(parsed_path)
    else:
        candidate = Path(raw_path)

    if not candidate.is_absolute():
        candidate = project_path.parent / candidate
    return candidate.resolve()


def main(*, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    qgis_files = find_qgis_projects(target_directory)
    if not qgis_files:
        logger.warning("No .qgz or .qgs files found in {}", target_directory)
        return 0

    logger.info("Found {} QGIS project files. Auditing...", len(qgis_files))

    broken_sources: set[Path] = set()
    parse_failures = 0

    for project_path in qgis_files:
        try:
            sources = get_data_sources(project_path)
        except Exception as error:
            logger.error("Could not inspect {}: {}", project_path, error)
            parse_failures += 1
            continue

        for src in sources:
            source_path = resolve_file_source(project_path, src)
            if source_path is None:
                continue

            if not source_path.exists():
                logger.error("Missing file in {}: {}", project_path.name, source_path)
                broken_sources.add(source_path)

    if not broken_sources:
        logger.success("All QGIS project data sources are valid!")
    else:
        logger.error("Found {} missing data sources across all projects.", len(broken_sources))

    if parse_failures:
        logger.error("Failed to inspect {} QGIS project files.", parse_failures)
    return 1 if broken_sources or parse_failures else 0


if __name__ == "__main__":
    args = _parse_cli_arguments()

    # Apply optional console log level if provided
    if args.console_log_level:
        logger.remove()
        logger.add(sys.stderr, level=args.console_log_level.upper())

    result = main(working_directory=args.working_directory)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)

    if not getattr(args, "no_pause", False):
        pause_console()

    raise SystemExit(result)
