r"""Mutable wrapper for external GeoTIFF overview generation.

Edit the defaults below for interactive use. CLI options override them without
changing the file. Overview levels are always written to ``.tif.ovr`` sidecars;
the source TIFFs are opened read-only.

Examples::

    python gdaladdo_tif_pyramids.py "D:\Model\Results"
    python gdaladdo_tif_pyramids.py --working-directory "D:\Model\Results" --refresh

Common scenarios::

    # Recursively add external, DEFLATE-compressed pyramids to existing TIFFs.
    python gdaladdo_tif_pyramids.py "D:\Model\Results"

    # Rebuild existing .ovr sidecars after a source raster changes.
    python gdaladdo_tif_pyramids.py "D:\Model\Results" --refresh
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.4"

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
PROFILE: RasterProfile = "tuflow"
OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)
OVERVIEW_RESAMPLING: OverviewResampling = "nearest"
WORKERS: int | None = None
RECURSIVE = True
REFRESH = False

import argparse

from loguru import logger

from ryan_library.functions.gdal.raster_processing import OverviewResampling, RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_workflows import add_overviews


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    profile: RasterProfile | None = None,
    levels: tuple[int, ...] | None = None,
    resampling: OverviewResampling | None = None,
    workers: int | None = None,
    recursive: bool | None = None,
    refresh: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared external-overview workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    # Resolve before chdir so relative CLI paths remain relative to the launch directory.
    target_directory = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_log_level = console_log_level or CONSOLE_LOG_LEVEL
    with setup_logger(console_log_level=effective_log_level):
        try:
            overviews = add_overviews(
                target_directory,
                recursive=RECURSIVE if recursive is None else recursive,
                profile=profile or PROFILE,
                levels=levels or OVERVIEW_LEVELS,
                resampling=resampling or OVERVIEW_RESAMPLING,
                workers=workers if workers is not None else WORKERS,
                refresh=REFRESH if refresh is None else refresh,
            )
            logger.info(f"Processing complete: {len(overviews)} overview file(s).")
        except Exception:
            logger.exception("External overview generation failed.")
            return 1

    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Create or refresh external .ovr files for GeoTIFF rasters.",
        epilog=r"""Processing scenarios:
  Create external overviews recursively:
  python gdaladdo_tif_pyramids.py "D:\Model\Results"

  Force existing .ovr sidecars to be rebuilt:
  python gdaladdo_tif_pyramids.py "D:\Model\Results" --refresh

The TIFF remains read-only and overview levels are written to <file>.tif.ovr.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory to process (positional shorthand).")
    parser.add_argument("--working-directory", type=Path, help="Directory to process instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument("--levels", nargs="+", type=int, metavar="LEVEL")
    parser.add_argument("--resampling", choices=("nearest", "average", "bilinear", "cubic", "mode"))
    parser.add_argument("--workers", type=int, help="Maximum concurrent raster jobs.")
    parser.add_argument("--refresh", action="store_true", default=None)
    parser.add_argument("--no-recursive", action="store_false", dest="recursive", default=None)
    parser.add_argument("--no-pause", action="store_true", help="Do not pause an interactive console on completion.")
    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(
        working_directory=args.working_directory or args.directory,
        console_log_level=args.console_log_level,
        profile=args.profile,
        levels=tuple(args.levels) if args.levels else None,
        resampling=args.resampling,
        workers=args.workers,
        recursive=args.recursive,
        refresh=args.refresh,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
