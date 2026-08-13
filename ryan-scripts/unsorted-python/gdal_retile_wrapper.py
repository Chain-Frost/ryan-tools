"""Retile raster files with editable defaults and optional CLI overrides."""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-13.1"
DEFAULT_INPUTS = [Path("input.tif")]
DEFAULT_OUTPUT_DIR = Path("tiles")
DEFAULT_TILE_SIZE = (5000, 5000)
DEFAULT_OVERLAP = 10
DEFAULT_FORMAT = "GTiff"
DEFAULT_RESAMPLING = "near"

import argparse
import shutil
import subprocess
import sys

from loguru import logger

from ryan_library.functions.path_stuff import to_path_list, to_single_path
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner


def main(args: argparse.Namespace) -> int:
    input_values = args.inputs if args.inputs else DEFAULT_INPUTS
    output_value = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR
    tile_size = tuple(args.tile_size) if args.tile_size is not None else DEFAULT_TILE_SIZE
    overlap = args.overlap if args.overlap is not None else DEFAULT_OVERLAP
    output_format = args.format if args.format is not None else DEFAULT_FORMAT
    resampling = args.resampling if args.resampling is not None else DEFAULT_RESAMPLING
    input_paths = to_path_list(input_values)
    output_dir = to_single_path(output_value)

    missing = [path for path in input_paths if not path.is_file()]
    if missing:
        logger.error("Input file not found: {}", missing[0])
        return 1
    if tile_size[0] < 1 or tile_size[1] < 1 or overlap < 0:
        logger.error("Tile dimensions must be positive and overlap cannot be negative.")
        return 2
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "osgeo_utils.gdal_retile",
        "-v",
        "-of",
        output_format,
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "PREDICTOR=2",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "SPARSE_OK=TRUE",
        "-co",
        "BIGTIFF=IF_SAFER",
        "-ps",
        str(tile_size[0]),
        str(tile_size[1]),
        "-overlap",
        str(overlap),
        "-r",
        resampling,
        "-targetDir",
        str(output_dir),
        *(str(path) for path in input_paths),
    ]
    logger.debug("Command: {}", subprocess.list2cmdline(command))
    try:
        result = subprocess.run(command, check=False, text=True, capture_output=True)
        if result.returncode == 0:
            logger.success("Retiling complete.")
            return 0
        logger.error("gdal_retile failed with code {}: {}", result.returncode, result.stderr.strip())
        fallback = shutil.which("gdal_retile.py")
        if fallback is None:
            return 1
        fallback_command = [fallback, *command[3:]]
        fallback_result = subprocess.run(fallback_command, check=False, text=True, capture_output=True)
        if fallback_result.returncode != 0:
            logger.error(
                "gdal_retile.py failed with code {}: {}", fallback_result.returncode, fallback_result.stderr.strip()
            )
            return 1
        logger.success("Retiling complete using gdal_retile.py.")
        return 0
    except OSError as error:
        logger.error("Failed to execute gdal_retile: {}", error)
        return 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"GDAL retile wrapper (v{WRAPPER_VERSION}).")
    parser.add_argument("inputs", nargs="*", type=Path, help="Override DEFAULT_INPUTS.")
    parser.add_argument("--output-dir", "-o", type=Path, help="Override DEFAULT_OUTPUT_DIR.")
    parser.add_argument("--tile-size", "-ps", nargs=2, type=int, default=None, help="Override DEFAULT_TILE_SIZE.")
    parser.add_argument("--overlap", type=int, default=None, help="Override DEFAULT_OVERLAP.")
    parser.add_argument("--format", "-of", default=None, help="Override DEFAULT_FORMAT.")
    parser.add_argument("--resampling", default=None, help="Override DEFAULT_RESAMPLING.")
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        result = main(cli_args)
    except Exception:
        logger.exception("Wrapper failed.")
        result = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not cli_args.no_pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
