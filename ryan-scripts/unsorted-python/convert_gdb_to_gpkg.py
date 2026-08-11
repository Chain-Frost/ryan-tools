"""
Convert Esri File Geodatabases into a selected GDAL vector format.

By default, each source layer is written to a separate output file. For
multi-layer formats such as GeoPackage and SQLite, ``--single-database`` writes
one output database per input GDB and retains all source layers together.
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-11.2"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
DEFAULT_OUTPUT_DIR = Path(".")
DEFAULT_OUTPUT_FORMAT = "gpkg"
DEFAULT_SINGLE_DATABASE = False
DEFAULT_WORKERS: int | None = None
# ==============================================================================

import argparse

from loguru import logger

from ryan_library.functions.gdal.vector_conversion import VECTOR_FORMATS
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathLike, PathOrList, to_path_list, to_single_path
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.gdal.file_geodatabase_export import (
    FileGeodatabaseExportSummary,
    export_file_geodatabases,
)


def main(
    *,
    input_paths: PathOrList | None = None,
    output_directory: PathLike | None = None,
    output_format: str | None = None,
    single_database: bool | None = None,
    workers: int | None = None,
) -> int:
    targets: list[Path] = (
        [Path(DEFAULT_INPUT).resolve()]
        if input_paths is None
        else [path.resolve() for path in to_path_list(input_paths)]
    )
    if not targets:
        logger.error("At least one input path is required.")
        return 1

    first_target: Path = targets[0]
    working_directory: Path = first_target.parent if first_target.suffix.lower() == ".gdb" else first_target
    if not change_working_directory(target_dir=working_directory):
        return 1

    raw_output_directory: Path = to_single_path(output_directory or DEFAULT_OUTPUT_DIR)
    output_root: Path = (
        raw_output_directory.resolve()
        if raw_output_directory.is_absolute()
        else (working_directory / raw_output_directory).resolve()
    )
    selected_format: str = output_format or DEFAULT_OUTPUT_FORMAT
    combine_layers: bool = DEFAULT_SINGLE_DATABASE if single_database is None else single_database
    selected_workers: int | None = DEFAULT_WORKERS if workers is None else workers

    try:
        summary: FileGeodatabaseExportSummary = export_file_geodatabases(
            input_paths=targets,
            output_root=output_root,
            output_format=selected_format,
            single_database=combine_layers,
            max_workers=selected_workers,
        )
    except Exception:
        logger.exception("File Geodatabase export failed.")
        return 1

    if summary.source_count == 0:
        logger.warning("No .gdb folders found across the input paths.")
        return 0

    for error in summary.errors:
        logger.error("{}", error)
    logger.success(
        "Finished: {} outputs converted, {} skipped, {} errors.",
        summary.converted,
        summary.skipped,
        len(summary.errors),
    )
    return 0 if summary.succeeded else 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Esri File Geodatabases using GDAL.")
    parser.add_argument(
        "-i",
        "--input-paths",
        "--input_paths",
        type=Path,
        nargs="+",
        default=None,
        help="Input directories to search, or specific .gdb folders.",
    )
    parser.add_argument(
        "-o",
        "--output-directory",
        type=Path,
        default=None,
        help="Output directory. Relative paths are resolved from the first input location.",
    )
    parser.add_argument(
        "-f",
        "--output-format",
        choices=sorted(VECTOR_FORMATS),
        default=None,
        help="Output format. Defaults to the editable DEFAULT_OUTPUT_FORMAT.",
    )
    output_mode = parser.add_mutually_exclusive_group()
    output_mode.add_argument(
        "--single-database",
        dest="single_database",
        action="store_true",
        default=None,
        help="For GPKG or SQLite, retain all layers in one database per input GDB.",
    )
    output_mode.add_argument(
        "--separate-files",
        dest="single_database",
        action="store_false",
        help="Write one output file per layer, overriding DEFAULT_SINGLE_DATABASE.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Maximum concurrent GDB conversions. Defaults to half the available CPUs.",
    )
    parser.add_argument("--no-pause", action="store_true", help="Do not pause the console after execution.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    with setup_logger(console_log_level="SUCCESS", log_file="convert_gdb.log", file_log_level="DEBUG"):
        result = main(
            input_paths=args.input_paths,
            output_directory=args.output_directory,
            output_format=args.output_format,
            single_database=args.single_database,
            workers=args.workers,
        )

    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
