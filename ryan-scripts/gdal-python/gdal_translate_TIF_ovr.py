r"""Mutable wrapper for terrain-raster conversion and external overviews.

Edit the constants near the top for routine interactive use, or supply CLI
options to override them for one run. The implementation lives in
``ryan_library.orchestrators.gdal.raster_workflows``.

Examples::

    python gdal_translate_TIF_ovr.py "D:\Model\Grid"
    python gdal_translate_TIF_ovr.py "D:\Model\Grid\surface.asc"
    python gdal_translate_TIF_ovr.py --working-directory "D:\Model\Grid" --profile efficient

Common scenarios::

    # Recursively convert FLT/ASC/RST inputs beside their sources.
    python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst

    # Rewrite supported rasters as <source-stem>_compress.tif.
    python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst tif tiff --output-suffix _compress

    # Import XYZ files and create GeoTIFFs under another output root.
    python gdal_translate_TIF_ovr.py "D:\XYZ" --extensions xyz --output-directory "D:\GeoTIFF"
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-28.1"

# Editable defaults for normal double-click or IDE execution.
INPUT_FILE: Path | None = None
WORKING_DIR: Path = Path(__file__).absolute().parent
CONSOLE_LOG_LEVEL = "INFO"
PROFILE: RasterProfile = "tuflow"
SOURCE_EXTENSIONS: tuple[str, ...] = ("flt", "asc", "rst", "xyz")
OUTPUT_DIRECTORY: Path | None = None
OUTPUT_SUFFIX: str = ""
OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)
OVERVIEW_RESAMPLING: OverviewResampling = "nearest"
WORKERS: int | None = None
RECURSIVE = True
BUILD_OVERVIEWS = True
OVERWRITE = False

import argparse

from loguru import logger

from ryan_library.functions.gdal.raster_processing import OverviewResampling, RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_workflows import convert_rasters


def main(
    *,
    input_file: Path | None = None,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    profile: RasterProfile | None = None,
    extensions: tuple[str, ...] | None = None,
    output_directory: Path | None = None,
    output_suffix: str | None = None,
    overview_levels: tuple[int, ...] | None = None,
    overview_resampling: OverviewResampling | None = None,
    workers: int | None = None,
    recursive: bool | None = None,
    build_overviews: bool | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared raster-conversion workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    # Resolve before chdir so relative CLI paths remain relative to the launch directory.
    selected_file: Path | None = (
        input_file if input_file is not None else INPUT_FILE if working_directory is None else None
    )
    if selected_file is not None:
        input_path: Path = selected_file.absolute()
        if not input_path.is_file():
            logger.error("Single-file input must be an existing file: {}", input_path)
            return 1
    else:
        input_path = (working_directory or WORKING_DIR).absolute()
    target_directory: Path = input_path.parent if input_path.is_file() else input_path
    configured_output_directory: Path | None = output_directory or OUTPUT_DIRECTORY
    resolved_output_directory: Path | None = (
        configured_output_directory.absolute() if configured_output_directory else None
    )
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    with setup_logger(console_log_level=effective_log_level):
        try:
            outputs: list[Path] = convert_rasters(
                directory=input_path,
                extensions=extensions or SOURCE_EXTENSIONS,
                output_directory=resolved_output_directory,
                output_suffix=OUTPUT_SUFFIX if output_suffix is None else output_suffix,
                recursive=RECURSIVE if recursive is None else recursive,
                profile=profile or PROFILE,
                build_overviews=BUILD_OVERVIEWS if build_overviews is None else build_overviews,
                overview_levels=overview_levels or OVERVIEW_LEVELS,
                overview_resampling=overview_resampling or OVERVIEW_RESAMPLING,
                workers=workers if workers is not None else WORKERS,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Processing complete: {len(outputs)} GeoTIFF(s).")
        except Exception:
            logger.exception("Raster conversion failed.")
            return 1

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Convert FLT, ASC, RST, and XYZ rasters to GeoTIFF and build external overviews.",
        epilog=r"""Common examples:
  python gdal_translate_TIF_ovr.py "D:\Model\Grid"
  python gdal_translate_TIF_ovr.py "D:\Model\Grid\surface.asc"
  python gdal_translate_TIF_ovr.py "D:\Model\Grid" --profile efficient

Processing scenarios:
  Convert FLT, ASC, and RST to TIFF beside each source:
    python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst

  Create *_compress.tif outputs, including from existing TIFFs:
    python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst tif tiff --output-suffix _compress

  Import XYZ and create TIFF files in another output tree:
    python gdal_translate_TIF_ovr.py "D:\XYZ" --extensions xyz --output-directory "D:\GeoTIFF"

XYZ is an input format for this tool; outputs are always GeoTIFF.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "path", nargs="?", type=Path, help="Raster file or directory to process (positional shorthand)."
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--input-file", type=Path, help="Process one explicitly selected raster file.")
    input_group.add_argument("--working-directory", type=Path, help="Directory to process instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument(
        "--extensions",
        nargs="+",
        metavar="EXT",
        help="Input formats to discover, such as flt asc rst xyz or tif; outputs are always GeoTIFF.",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        help="Optional output root; recursive source subdirectories are preserved below it.",
    )
    parser.add_argument("--output-suffix", help="Text appended to output stems, for example _compress.")
    parser.add_argument("--levels", nargs="+", type=int, metavar="LEVEL")
    parser.add_argument("--resampling", choices=("nearest", "average", "bilinear", "cubic", "mode"))
    parser.add_argument("--workers", type=int, help="Maximum concurrent raster jobs.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-recursive", action="store_false", dest="recursive", default=None)
    parser.add_argument("--no-overviews", action="store_false", dest="build_overviews", default=None)
    parser.add_argument("--no-pause", action="store_true", help="Do not pause an interactive console on completion.")
    args: argparse.Namespace = parser.parse_args()
    if args.path is not None and (args.input_file is not None or args.working_directory is not None):
        parser.error("the positional path cannot be combined with --input-file or --working-directory")
    return args


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(
        input_file=args.input_file,
        working_directory=args.working_directory or args.path,
        console_log_level=args.console_log_level,
        profile=args.profile,
        extensions=tuple(args.extensions) if args.extensions else None,
        output_directory=args.output_directory,
        output_suffix=args.output_suffix,
        overview_levels=tuple(args.levels) if args.levels else None,
        overview_resampling=args.resampling,
        workers=args.workers,
        recursive=args.recursive,
        build_overviews=args.build_overviews,
        overwrite=args.overwrite,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
