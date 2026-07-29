"""Mutable wrapper for grouped GeoTIFF mosaic creation.

Input stems are split on underscores and plus signs. The selected one-based
field is removed to form a group name. Each group is assembled through a
temporary VRT and retained as ``merged_<group>.tif`` with external overviews.

I don't know what this actually does. Too long ago.

Example::

    python build_VRT.py --working-directory "D:\\Model\\Results" --remove-field 2
"""

from __future__ import annotations

import argparse
import gc
from pathlib import Path
import sys

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
CONSOLE_LOG_LEVEL = "INFO"
GROUP_REMOVE_INDEX = 2
ALLOWED_SUFFIXES: tuple[str, ...] = ("d_HR_Max", "h_HR_Max", "V_Max", "DEM_Z_HR")
PROFILE: RasterProfile = "tuflow"
OVERVIEW_LEVELS: tuple[int, ...] = (2, 4, 8, 16, 32)
OVERVIEW_RESAMPLING: OverviewResampling = "nearest"
WORKERS: int | None = None
OVERWRITE = False

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
# Allow direct execution from a source checkout before the wheel is installed.
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from loguru import logger

from ryan_library.functions.gdal.raster_processing import OverviewResampling, RasterProfile
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_library_version
from ryan_library.orchestrators.gdal.raster_mosaic import create_grouped_mosaics


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    group_remove_index: int | None = None,
    allowed_suffixes: tuple[str, ...] | None = None,
    profile: RasterProfile | None = None,
    overview_levels: tuple[int, ...] | None = None,
    overview_resampling: OverviewResampling | None = None,
    workers: int | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared grouped-mosaic workflow."""
    print_library_version()
    # Resolve before chdir so relative CLI paths remain relative to the launch directory.
    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_log_level = console_log_level or CONSOLE_LOG_LEVEL
    with setup_logger(console_log_level=effective_log_level):
        try:
            outputs = create_grouped_mosaics(
                directory=target_directory,
                group_remove_index=group_remove_index or GROUP_REMOVE_INDEX,
                allowed_suffixes=allowed_suffixes or ALLOWED_SUFFIXES,
                profile=profile or PROFILE,
                overview_levels=overview_levels or OVERVIEW_LEVELS,
                overview_resampling=overview_resampling or OVERVIEW_RESAMPLING,
                workers=workers if workers is not None else WORKERS,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Processing complete: {len(outputs)} mosaic(s).")
        except Exception:
            logger.exception("Grouped mosaic creation failed.")
            return 1

    print()
    print_library_version()
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Group matching TIFFs, mosaic each group, and create external overviews.",
        epilog='Example:\n  python build_VRT.py --working-directory "D:\\Model\\Results" --remove-field 2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory to process (positional shorthand).")
    parser.add_argument("--working-directory", type=Path, help="Directory to process instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--remove-field", type=int, help="One-based filename field removed when grouping.")
    parser.add_argument("--suffixes", nargs="+", metavar="SUFFIX")
    parser.add_argument("--profile", choices=("tuflow", "efficient"))
    parser.add_argument("--levels", nargs="+", type=int, metavar="LEVEL")
    parser.add_argument("--resampling", choices=("nearest", "average", "bilinear", "cubic", "mode"))
    parser.add_argument("--workers", type=int, help="Maximum concurrent mosaic groups.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true", help="Do not pause an interactive console on completion.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.working_directory or args.directory,
        console_log_level=args.console_log_level,
        group_remove_index=args.remove_field,
        allowed_suffixes=tuple(args.suffixes) if args.suffixes else None,
        profile=args.profile,
        overview_levels=tuple(args.levels) if args.levels else None,
        overview_resampling=args.resampling,
        workers=args.workers,
        overwrite=args.overwrite,
    )
    gc.collect()
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
