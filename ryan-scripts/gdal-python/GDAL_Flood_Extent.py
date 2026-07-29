"""Mutable wrapper for TUFLOW flood-extent raster and polygon generation.

Inputs must match ``*_d_HR_Max.tif``. Each cutoff produces a Byte GeoTIFF mask
and a vector dataset beside the source. GeoPackage is the default vector
format; set ``VECTOR_FORMAT`` or use ``--vector-format shp`` for Shapefile.

Examples::

    python GDAL_Flood_Extent.py --working-directory "D:\\Model\\Results"
    python GDAL_Flood_Extent.py "D:\\Model\\Results" --cutoff 0.05 0.30 --vector-format shp
"""

from __future__ import annotations

import argparse
import gc
from pathlib import Path
import sys

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
PATHS_TO_PROCESS: tuple[Path, ...] = ()
CONSOLE_LOG_LEVEL = "INFO"
CUTOFF_VALUES: tuple[float, ...] = (0.0,)
PROFILE: RasterProfile = "tuflow"
VECTOR_FORMAT: VectorFormat = "gpkg"
WORKERS: int | None = None
OVERWRITE = False

REPOSITORY_ROOT: Path = Path(__file__).resolve().parents[2]
# Allow direct execution from a source checkout before the wheel is installed.
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ryan_library.functions.gdal.raster_processing import RasterProfile, VectorFormat
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_library_version
from ryan_library.orchestrators.gdal.gdal_flood_extent import main_processing


def main(
    *,
    working_directory: Path | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    console_log_level: str | None = None,
    cutoff_values: tuple[float, ...] | None = None,
    profile: RasterProfile | None = None,
    vector_format: VectorFormat | None = None,
    workers: int | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and run the shared flood-extent workflow."""
    print_library_version()
    # Resolve every user path before chdir so relative paths retain their original meaning.
    target_directory = (working_directory or WORKING_DIR).resolve()
    configured_paths = paths_to_process or PATHS_TO_PROCESS or (target_directory,)
    effective_paths = tuple(path.resolve() for path in configured_paths)
    if not change_working_directory(target_dir=target_directory):
        return 1

    try:
        outputs = main_processing(
            paths_to_process=list(effective_paths),
            console_log_level=console_log_level or CONSOLE_LOG_LEVEL,
            cutoff_values=cutoff_values or CUTOFF_VALUES,
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

    print()
    print_library_version()
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse CLI overrides for the editable constants above."""
    parser = argparse.ArgumentParser(
        description="Create flood-extent TIFFs and vector polygons from *_d_HR_Max.tif files.",
        epilog=(
            "Examples:\n"
            '  python GDAL_Flood_Extent.py --working-directory "D:\\Model\\Results"\n'
            '  python GDAL_Flood_Extent.py "D:\\Model\\Results" --cutoff 0.05 0.30 --vector-format shp'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directories", nargs="*", type=Path, help="Directories containing depth rasters.")
    parser.add_argument("--working-directory", type=Path, help="Working directory instead of WORKING_DIR.")
    parser.add_argument("--console-log-level", help="Log verbosity such as INFO or DEBUG.")
    parser.add_argument("--cutoff", nargs="+", type=float, metavar="DEPTH")
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
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.working_directory,
        paths_to_process=tuple(args.directories) if args.directories else None,
        console_log_level=args.console_log_level,
        cutoff_values=tuple(args.cutoff) if args.cutoff else None,
        profile=args.profile,
        vector_format=args.vector_format,
        workers=args.workers,
        overwrite=args.overwrite,
    )
    gc.collect()
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
