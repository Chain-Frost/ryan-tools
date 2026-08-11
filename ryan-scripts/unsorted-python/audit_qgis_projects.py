"""
Audit QGIS Project (.qgz) files to find broken data source paths.

This script scans a directory for .qgz files, extracts the layer data sources,
and checks if they exist on disk. It reports any missing or broken paths.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from xml.etree import ElementTree
from zipfile import ZipFile

WRAPPER_VERSION = "2026-08-11.1"
DEFAULT_WORKING_DIR = Path(".")

from loguru import logger
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)

def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
    description="Audit QGIS Project files (.qgz, .qgs) to find broken data source paths."
    )
    add_execution_cli_arguments(parser)
    return parser.parse_args()

def find_qgis_projects(start_dir: Path) -> list[Path]:
    """Find all .qgz and .qgs files in the directory tree."""
    qgis_files = []
    for root, _, files in os.walk(start_dir):
        for f in files:
            lower_f = f.lower()
            if (lower_f.endswith('.qgz') or lower_f.endswith('.qgs')) and 'ss' not in lower_f:
                qgis_files.append(Path(root) / f)
    return qgis_files

def get_data_sources(project_path: Path) -> list[str]:
    """Extract all data sources from a .qgz or .qgs file."""
    data_sources = set()
    try:
        if project_path.suffix.lower() == '.qgz':
            with ZipFile(project_path, 'r') as in_qgz:
                for f in in_qgz.infolist():
                    if f.filename.endswith('.qgs'):
                        xml_content = in_qgz.read(f.filename)
                        tree = ElementTree.XML(xml_content)
                        
                        for element in tree.findall("./projectlayers/maplayer/datasource"):
                            if element.text:
                                data_sources.add(element.text)
        elif project_path.suffix.lower() == '.qgs':
            tree = ElementTree.parse(project_path)
            for element in tree.findall("./projectlayers/maplayer/datasource"):
                if element.text:
                    data_sources.add(element.text)
    except Exception as e:
        logger.error(f"Error extracting data sources from {project_path.name}: {e}")
        
    return list(data_sources)

def main(*, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    qgis_files = find_qgis_projects(target_directory)
    if not qgis_files:
        logger.warning(f"No .qgz or .qgs files found in {target_directory}")
        return 0

    logger.info(f"Found {len(qgis_files)} QGIS project files. Auditing...")

    broken_sources = set()

    for project_path in qgis_files:
        sources = get_data_sources(project_path)
        
        for src in sources:
            # Handle delimited paths (e.g., CSVs or vectors with specific layer configs)
            adj_src = src.split("|")[0]
            
            # Skip memory layers and EPSG definitions
            if adj_src.startswith("memory?") or adj_src.startswith("crs=EPSG"):
                continue
            
            if not Path(adj_src).exists():
                logger.error(f"Missing file in {project_path.name}: {adj_src}")
                broken_sources.add(adj_src)

    if not broken_sources:
        logger.success("All QGIS project data sources are valid!")
    else:
        logger.error(f"Found {len(broken_sources)} missing data sources across all projects.")

    return 0

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
