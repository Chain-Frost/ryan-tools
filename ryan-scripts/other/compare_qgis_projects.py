"""Compare two QGS/QGZ projects without modifying them or opening data layers.

Compare layer IDs, names, provider names, source URIs and CRS authids. Unknown
providers are reported without attempting to interpret their URIs. The output
is structural, not a comparison of GIS data, QGIS symbology or rendered maps.

Examples:
  python compare_qgis_projects.py --original old.qgz --updated new.qgz --no-pause
  python compare_qgis_projects.py --working-directory "Q:\\GIS" --original a.qgs --updated b.qgz --format json --no-pause

Exit codes: 0 = structurally identical, 2 = differences, 1 = invalid input.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-11-08.1"

DEFAULT_WORKING_DIR = Path()
DEFAULT_ORIGINAL_PROJECT: Path | None = None
DEFAULT_UPDATED_PROJECT: Path | None = None
DEFAULT_REPORT_FORMAT: Literal["text", "json"] = "text"
DEFAULT_NO_PAUSE = True
DEFAULT_CONSOLE_LOG_LEVEL: str | None = None

import argparse
import json
import sys
from dataclasses import asdict
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, LargeZipFile

from loguru import logger

from ryan_library.functions.qgis_project_comparison import QgisProjectComparison, compare_qgis_projects
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    pause_console,
    print_wrapper_banner,
)


def _print_text(comparison: QgisProjectComparison) -> None:
    print(f"Original: {comparison.original_project}")
    print(f"Updated:  {comparison.updated_project}")
    if comparison.project_crs_changed:
        print(f"Project CRS: {comparison.original_crs!r} -> {comparison.updated_crs!r}")
    for change in comparison.layer_changes:
        print(f"{change.kind.upper()}: {change.layer_id}")
        if change.before is not None and change.after is not None:
            before = asdict(change.before)
            after = asdict(change.after)
            for field in change.changed_fields:
                print(f"  {field}: {before[field]!r} -> {after[field]!r}")
        else:
            layer = change.before if change.before is not None else change.after
            if layer is not None:
                print(
                    f"  {layer.name!r}: provider={layer.provider!r}, "
                    f"category={layer.provider_category!r}, source={layer.datasource!r}, crs={layer.crs!r}"
                )
    if not comparison.has_changes:
        print("No structural differences in the supported comparison fields")


def main(
    *,
    working_directory: Path | None = None,
    original_project: Path | None = DEFAULT_ORIGINAL_PROJECT,
    updated_project: Path | None = DEFAULT_UPDATED_PROJECT,
    report_format: Literal["text", "json"] = DEFAULT_REPORT_FORMAT,
) -> int:
    """Print a deterministic read-only comparison; return 0, 1 or 2."""
    if original_project is None or updated_project is None:
        logger.error("Both original and updated QGIS project paths are required")
        return 1
    if report_format not in ("text", "json"):
        logger.error("Unsupported comparison report format: {}", report_format)
        return 1
    directory = (working_directory if working_directory is not None else DEFAULT_WORKING_DIR).absolute()
    original = (directory / original_project).absolute()
    updated = (directory / updated_project).absolute()
    try:
        comparison = compare_qgis_projects(original, updated)
    except (OSError, ValueError, BadZipFile, LargeZipFile, ET.ParseError) as error:
        logger.error("Cannot compare QGIS projects: {}", error)
        return 1
    if report_format == "json":
        print(json.dumps(asdict(comparison), indent=2, ensure_ascii=False))
    else:
        _print_text(comparison)
    return 2 if comparison.has_changes else 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only structural QGS/QGZ project comparison (no QGIS required).")
    parser.add_argument("--version", action="version", version=f"%(prog)s {WRAPPER_VERSION}")
    add_execution_cli_arguments(parser)
    parser.set_defaults(no_pause=None)
    parser.add_argument("--original", type=Path, help="Original QGS/QGZ project path")
    parser.add_argument("--updated", type=Path, help="Updated QGS/QGZ project path")
    parser.add_argument("--format", choices=("text", "json"), default=None, help="Report format")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_cli_arguments()
    level = args.console_log_level if args.console_log_level is not None else DEFAULT_CONSOLE_LOG_LEVEL
    if level is not None:
        logger.remove()
        logger.add(sys.stderr, level=level.upper())
    report_format = args.format if args.format is not None else DEFAULT_REPORT_FORMAT
    if report_format == "text":
        print_wrapper_banner(
            wrapper_source="other/compare_qgis_projects.py",
            wrapper_file=Path(__file__),
            wrapper_version=WRAPPER_VERSION,
        )
    result = main(
        working_directory=args.working_directory,
        original_project=args.original if args.original is not None else DEFAULT_ORIGINAL_PROJECT,
        updated_project=args.updated if args.updated is not None else DEFAULT_UPDATED_PROJECT,
        report_format=report_format,
    )
    no_pause = args.no_pause if args.no_pause is not None else DEFAULT_NO_PAUSE
    if not no_pause and report_format == "text":
        pause_console()
    raise SystemExit(result)
