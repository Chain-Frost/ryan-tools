r"""Mutable wrapper for TUFLOW flood-extent raster and polygon generation.

The default input pattern is ``*_d_HR_Max.tif``, but patterns, recursion, and
the source band are configurable. Each cutoff produces a Byte GeoTIFF mask and
a vector dataset beside the source. Optional sieving removes connected regions
smaller than a chosen pixel count. GeoPackage is the default vector format.

Examples::

    python gdal_flood_extent.py --working-directory "D:\Model\Results"
    python gdal_flood_extent.py "D:\Model\Results" --cutoff 0.05 0.30 --vector-format shp
    python gdal_flood_extent.py "D:\Results" --patterns "*.ecw" --input-band 4 --sieve-pixels 8

Common scenarios::

    # Recursively threshold band 1 and write GPKG.
    python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1

    # Process band 4 of ECW inputs at cutoff 50 and write Shapefile output.
    python gdal_flood_extent.py "D:\Imagery" --patterns "*.ecw" --recursive --input-band 4 --cutoff 50 --vector-format shp

    # Remove regions below 8 pixels using 8-connectivity.
    python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1 --sieve-pixels 8 --connectedness 8 --keep-intermediate-masks
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-08-02.4"

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
PATHS_TO_PROCESS: tuple[Path, ...] = ()
CONSOLE_LOG_LEVEL = "INFO"
CUTOFF_VALUES: tuple[float, ...] = (0.0,)
FILE_PATTERNS: tuple[str, ...] = ("*_d_HR_Max.tif",)
RECURSIVE = False
INPUT_BAND = 1
SIEVE_PIXELS: int | None = None
CONNECTEDNESS: Literal[4, 8] = 8
KEEP_INTERMEDIATE_MASKS = False
PROFILE: RasterProfile = "tuflow"
VECTOR_FORMAT: VectorFormat = "gpkg"
WORKERS: int | None = None
OVERWRITE = False

import argparse

from ryan_library.functions.gdal.raster_processing import RasterProfile, VectorFormat
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.gdal_flood_extent import main_processing


def main(
    *,
    working_directory: Path | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    console_log_level: str | None = None,
    cutoff_values: tuple[float, ...] | None = None,
    file_patterns: tuple[str, ...] | None = None,
    recursive: bool | None = None,
    input_band: int | None = None,
    sieve_pixels: int | None = None,
    connectedness: Literal[4, 8] | None = None,
    keep_intermediate_masks: bool | None = None,
    profile: RasterProfile | None = None,
    vector_format: VectorFormat | None = None,
    workers: int | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared flood-extent workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    # Resolve every user path before chdir so relative paths retain their original meaning.
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    configured_paths: tuple[Path, ...] | tuple[Path] = paths_to_process or PATHS_TO_PROCESS or (target_directory,)
    effective_paths: tuple[Path, ...] = tuple(path.resolve() for path in configured_paths)
    if not change_working_directory(target_dir=target_directory):
        return 1

    try:
        outputs: list[Path] = main_processing(
            paths_to_process=list(effective_paths),
            console_log_level=console_log_level or CONSOLE_LOG_LEVEL,
            cutoff_values=cutoff_values or CUTOFF_VALUES,
            file_patterns=file_patterns or FILE_PATTERNS,
            recursive=RECURSIVE if recursive is None else recursive,
            input_band=input_band or INPUT_BAND,
            sieve_pixels=SIEVE_PIXELS if sieve_pixels is None else sieve_pixels,
            connectedness=connectedness or CONNECTEDNESS,
            keep_intermediate_masks=(
                KEEP_INTERMEDIATE_MASKS if keep_intermediate_masks is None else keep_intermediate_masks
            ),
            profile=profile or PROFILE,
            vector_format=vector_format or VECTOR_FORMAT,
            workers=workers if workers is not None else WORKERS,
            overwrite=OVERWRITE if overwrite is None else overwrite,
        )
        print(f"Processing complete: {len(outputs)} flood-extent output file(s).")
    except Exception as exc:
        # main_processing owns and closes its Loguru context, so report at the wrapper boundary with print.
        print(f"Flood-extent processing failed: {exc}")
        return 1

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Create flood-extent TIFFs and vector polygons from matching rasters.",
        epilog=r"""Common examples:
  python gdal_flood_extent.py --working-directory "D:\Model\Results"
  python gdal_flood_extent.py "D:\Model\Results" --cutoff 0.05 0.30 --vector-format shp

Processing scenarios:
  Recursively process band-1 TIFFs at cutoff 0.1:
    python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1

  Process band 4 of ECW files at cutoff 50 and create SHP:
    python gdal_flood_extent.py "D:\Imagery" --patterns "*.ecw" --recursive --input-band 4 --cutoff 50 --vector-format shp

  Remove connected regions smaller than 8 pixels and retain raw masks:
    python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1 --sieve-pixels 8 --connectedness 8 --keep-intermediate-masks""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directories", nargs="*", type=Path, help="Directories containing depth rasters.")
    parser.add_argument("--working-directory", type=Path, help="Working directory instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--cutoff", nargs="+", type=float, metavar="DEPTH")
    parser.add_argument("--patterns", nargs="+", metavar="GLOB", help="Input globs; default: *_d_HR_Max.tif.")
    parser.add_argument("--recursive", action="store_true", default=None, help="Search subdirectories.")
    parser.add_argument("--input-band", type=int, help="One-based source raster band; default: 1.")
    parser.add_argument("--sieve-pixels", type=int, help="Remove connected regions smaller than this many pixels.")
    parser.add_argument("--connectedness", type=int, choices=(4, 8), help="Sieve neighbourhood; default: 8.")
    parser.add_argument(
        "--keep-intermediate-masks",
        action="store_true",
        default=None,
        help="Retain the unsieved classification mask beside each source.",
    )
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument(
        "--vector-format",
        choices=("gpkg", "shp"),
        help="Polygon output format (script default: gpkg).",
    )
    parser.add_argument("--workers", type=int, help="Maximum concurrent depth rasters.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true", help="Do not pause an interactive console on completion.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(
        working_directory=args.working_directory,
        paths_to_process=tuple(args.directories) if args.directories else None,
        console_log_level=args.console_log_level,
        cutoff_values=tuple(args.cutoff) if args.cutoff else None,
        file_patterns=tuple(args.patterns) if args.patterns else None,
        recursive=args.recursive,
        input_band=args.input_band,
        sieve_pixels=args.sieve_pixels,
        connectedness=args.connectedness,
        keep_intermediate_masks=args.keep_intermediate_masks,
        profile=args.profile,
        vector_format=args.vector_format,
        workers=args.workers,
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
