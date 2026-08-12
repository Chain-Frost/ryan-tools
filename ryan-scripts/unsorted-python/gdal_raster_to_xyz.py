"""
Convert rasters to XYZ/CSV point data.

This wrapper uses GDAL's native gdal2xyz utility to export raster pixels to
X, Y, Z coordinate files. It safely processes files block-by-block, making it
memory-safe for massive rasters. It can optionally skip NoData values to reduce output size.
"""

from __future__ import annotations

import argparse
import sys
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

WRAPPER_VERSION = "2026-08-11.1"
DEFAULT_WORKING_DIR = Path(".")
DEFAULT_PATTERNS = ["*.tif"]
DEFAULT_RECURSIVE = False
DEFAULT_FORMAT = "csv"  # csv or xyz
DEFAULT_KEEP_NODATA = False

from loguru import logger
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert raster files to XYZ or CSV point data."
    )
    
    parser.add_argument(
        "input_directory",
        type=Path,
        help="Directory containing the input rasters.",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=DEFAULT_PATTERNS,
        help="Glob patterns to match input files.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        default=DEFAULT_RECURSIVE,
        help="Search for input files recursively.",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "xyz"],
        default=DEFAULT_FORMAT,
        help="Output format. CSV uses comma delimiters, XYZ uses space delimiters.",
    )
    parser.add_argument(
        "--keep-nodata",
        action="store_true",
        default=DEFAULT_KEEP_NODATA,
        help="If set, includes NoData pixels in the output. By default, NoData pixels are skipped.",
    )
    
    add_execution_cli_arguments(parser)
    return parser.parse_args()


def process_file(input_file: Path, output_file: Path, is_csv: bool, skip_nodata: bool) -> bool:
    """Run gdal2xyz on a single file."""
    cmd = [sys.executable, "-m", "osgeo_utils.gdal2xyz"]
    
    if is_csv:
        cmd.append("-csv")
    if skip_nodata:
        cmd.append("-skipnodata")
        
    cmd.extend([str(input_file), str(output_file)])
    
    logger.debug("Running: {}", " ".join(cmd))
    
    try:
        # Run gdal2xyz directly through the current python environment
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        logger.error("Failed to process {}: {}", input_file.name, e.stderr)
        return False


def main(*, working_directory: Path | None = None) -> int:
    args = _parse_cli_arguments()
    target_directory = (working_directory or args.input_directory).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    input_files = []
    for pattern in args.patterns:
        if args.recursive:
            input_files.extend(list(target_directory.rglob(pattern)))
        else:
            input_files.extend(list(target_directory.glob(pattern)))

    if not input_files:
        logger.warning("No files matching {} found in {}", args.patterns, target_directory)
        return 0
        
    # Deduplicate in case patterns overlap
    input_files = list(set(input_files))

    logger.info("Found {} raster files to convert.", len(input_files))
    
    # User requested skip by default, so --keep-nodata toggles it off
    skip_nodata = not args.keep_nodata
    is_csv = args.format == "csv"
    
    success_count = 0
    
    # Process files concurrently (like the original script) but safely relying on GDAL chunking
    with ProcessPoolExecutor() as executor:
        futures = {}
        for input_file in input_files:
            output_file = input_file.with_suffix(f".{args.format}")
            futures[executor.submit(process_file, input_file, output_file, is_csv, skip_nodata)] = input_file
            
        for future in as_completed(futures):
            input_file = futures[future]
            try:
                if future.result():
                    logger.success("Successfully exported {}", input_file.name)
                    success_count += 1
            except Exception as e:
                logger.error("Error converting {}: {}", input_file.name, e)

    logger.info("Successfully converted {} out of {} files.", success_count, len(input_files))
    return 0 if success_count == len(input_files) else 1


if __name__ == "__main__":
    args = _parse_cli_arguments()
    
    # Apply optional console log level if provided
    if args.console_log_level:
        logger.remove()
        logger.add(sys.stderr, level=args.console_log_level.upper())
        
    result = main()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    
    if not getattr(args, "no_pause", False):
        pause_console()
        
    raise SystemExit(result)
