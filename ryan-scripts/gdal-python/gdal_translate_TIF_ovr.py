"""Mutable wrapper for terrain-raster conversion and external overviews.

Edit the constants near the top for routine interactive use, or supply CLI
options to override them for one run. The implementation lives in
``ryan_library.orchestrators.gdal.raster_workflows``.

Examples::

    python gdal_translate_TIF_ovr.py "D:\\Model\\Grid"
    python gdal_translate_TIF_ovr.py --working-directory "D:\\Model\\Grid" --profile efficient
"""

from __future__ import annotations

import argparse
import gc
from pathlib import Path
import sys

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
PROFILE: RasterProfile = "tuflow"
SOURCE_EXTENSIONS: tuple[str, ...] = ("flt", "asc", "rst", "xyz")
OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)
OVERVIEW_RESAMPLING: OverviewResampling = "nearest"
WORKERS: int | None = None
RECURSIVE = True
BUILD_OVERVIEWS = True
OVERWRITE = False

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
# Allow direct execution from a source checkout before the wheel is installed.
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from loguru import logger

from ryan_library.functions.gdal.raster_processing import OverviewResampling, RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_library_version
from ryan_library.orchestrators.gdal.raster_workflows import convert_rasters


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    profile: RasterProfile | None = None,
    extensions: tuple[str, ...] | None = None,
    overview_levels: tuple[int, ...] | None = None,
    overview_resampling: OverviewResampling | None = None,
    workers: int | None = None,
    recursive: bool | None = None,
    build_overviews: bool | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared raster-conversion workflow."""
    print_library_version()
    # Resolve before chdir so relative CLI paths remain relative to the launch directory.
    target_directory = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_log_level = console_log_level or CONSOLE_LOG_LEVEL
    with setup_logger(console_log_level=effective_log_level):
        try:
            outputs = convert_rasters(
                target_directory,
                extensions=extensions or SOURCE_EXTENSIONS,
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

    print()
    print_library_version()
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Convert FLT, ASC, RST, and XYZ rasters to GeoTIFF and build external overviews.",
        epilog=(
            "Examples:\n"
            '  python gdal_translate_TIF_ovr.py "D:\\Model\\Grid"\n'
            '  python gdal_translate_TIF_ovr.py --working-directory "D:\\Model\\Grid" --profile efficient'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory to process (positional shorthand).")
    parser.add_argument("--working-directory", type=Path, help="Directory to process instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument("--extensions", nargs="+", metavar="EXT")
    parser.add_argument("--levels", nargs="+", type=int, metavar="LEVEL")
    parser.add_argument("--resampling", choices=("nearest", "average", "bilinear", "cubic", "mode"))
    parser.add_argument("--workers", type=int, help="Maximum concurrent raster jobs.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-recursive", action="store_false", dest="recursive", default=None)
    parser.add_argument("--no-overviews", action="store_false", dest="build_overviews", default=None)
    parser.add_argument("--no-pause", action="store_true", help="Do not pause an interactive console on completion.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.working_directory or args.directory,
        console_log_level=args.console_log_level,
        profile=args.profile,
        extensions=tuple(args.extensions) if args.extensions else None,
        overview_levels=tuple(args.levels) if args.levels else None,
        overview_resampling=args.resampling,
        workers=args.workers,
        recursive=args.recursive,
        build_overviews=args.build_overviews,
        overwrite=args.overwrite,
    )
    gc.collect()
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
