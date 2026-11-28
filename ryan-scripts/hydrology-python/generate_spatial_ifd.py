"""Generate an experimental spatial IFD raster or compare with a reference.

This is an offline-first research wrapper. Verify the real ARR source key,
baseline, units, datum and permitted request budget before downloading data.
Set MAX_NEW_POINTS or --max-new-points explicitly for any network access.
The supplied reference raster must be depth in mm for the selected event.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

WRAPPER_VERSION = "2026-11-28.1"
WORKING_DIR = Path.cwd()
REFERENCE: Path | None = None
ASSUME_WGS84 = False
NORTH_LAT: Decimal | None = None
WEST_LON: Decimal | None = None
ROWS: int | None = None
COLS: int | None = None
SPACING = Decimal("0.025")
SOURCE_PATH: list[str] = []
SNAPSHOT = ""
CACHE = Path.home() / ".ryan-tools" / "spatial-ifd" / "cache.sqlite3"
DURATION_MIN = 60
AEP_PERCENT = 1.0
MAX_NEW_POINTS = 0
OUTPUT: Path | None = None
REFERENCE_DEPTH_MM: float | None = None
PAUSE_ON_FINISH = False

import argparse
import sqlite3
from collections.abc import Sequence

from rasterio.errors import RasterioError

from ryan_library.functions.hydrology.spatial_ifd import GridSpec, IFDSource, PointCache, collect_grid, write_ascii_grid
from ryan_library.functions.hydrology.spatial_ifd_rasters import (
    ReferenceGrid,
    compare_reference,
    read_reference_raster,
    write_geotiff,
)
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner

_WRAPPER_SOURCE = "hydrology-python/generate_spatial_ifd.py"


class _Options(argparse.Namespace):
    working_dir: Path
    reference: Path | None
    assume_wgs84: bool
    north_lat: Decimal | None
    west_lon: Decimal | None
    rows: int | None
    cols: int | None
    spacing: Decimal
    source_path: list[str]
    snapshot: str
    cache: Path
    duration_min: int
    aep_percent: float
    max_new_points: int
    output: Path | None
    reference_depth_mm: float | None
    pause: bool


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=WRAPPER_VERSION)
    parser.add_argument("--working-dir", type=Path, default=WORKING_DIR)
    parser.add_argument("--reference", type=Path, default=REFERENCE, help="Reference depth raster for the selected event")
    parser.add_argument("--assume-wgs84", action=argparse.BooleanOptionalAction, default=ASSUME_WGS84)
    parser.add_argument("--north-lat", type=Decimal, default=NORTH_LAT, help="Latitude of north-west cell centre")
    parser.add_argument("--west-lon", type=Decimal, default=WEST_LON, help="Longitude of north-west cell centre")
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--cols", type=int, default=COLS)
    parser.add_argument("--spacing", type=Decimal, default=SPACING, help="Angular spacing; ignored with --reference")
    parser.add_argument("--source-path", nargs="+", default=SOURCE_PATH, help="Exact layer/table keys")
    parser.add_argument("--snapshot", default=SNAPSHOT, help="Local immutable cache namespace")
    parser.add_argument("--cache", type=Path, default=CACHE)
    parser.add_argument("--duration-min", type=int, default=DURATION_MIN)
    parser.add_argument("--aep-percent", type=float, default=AEP_PERCENT)
    parser.add_argument("--max-new-points", type=int, default=MAX_NEW_POINTS, help="Default zero: no network access")
    parser.add_argument("--output", type=Path, default=OUTPUT, help="New .asc or .tif file")
    parser.add_argument("--reference-depth-mm", type=float, default=REFERENCE_DEPTH_MM, help="Export depth ratios instead")
    parser.add_argument("--pause", action=argparse.BooleanOptionalAction, default=PAUSE_ON_FINISH)
    return parser


def _spec(options: _Options) -> tuple[GridSpec, ReferenceGrid | None]:
    if options.reference is not None:
        if any(value is not None for value in (options.north_lat, options.west_lon, options.rows, options.cols)):
            msg = "Choose the reference raster OR explicit grid geometry, not both"
            raise ValueError(msg)
        reference = read_reference_raster(
            (options.working_dir / options.reference).absolute(), assume_wgs84=options.assume_wgs84
        )
        return reference.spec, reference
    if options.north_lat is None or options.west_lon is None or options.rows is None or options.cols is None:
        msg = "Set a reference raster or all four explicit grid geometry settings"
        raise ValueError(msg)
    return GridSpec(options.north_lat, options.west_lon, options.rows, options.cols, options.spacing), None


def _output_path(options: _Options) -> Path | None:
    if options.output is None:
        if options.reference is None:
            msg = "Set an output or reference raster"
            raise ValueError(msg)
        return None
    path = (options.working_dir / options.output).absolute()
    if path.exists() or path.is_symlink():
        raise FileExistsError(path)
    if path.suffix.lower() not in {".asc", ".tif", ".tiff"} or not path.parent.is_dir():
        msg = "Output must be .asc or .tif/.tiff in an existing directory"
        raise ValueError(msg)
    return path


def _run(options: _Options) -> None:
    spec, reference = _spec(options)
    output = _output_path(options)
    source = IFDSource(tuple(options.source_path), options.snapshot)
    cache = PointCache((options.working_dir / options.cache).absolute())
    grid = collect_grid(
        spec,
        source,
        cache,
        options.duration_min,
        options.aep_percent,
        max_new_points=options.max_new_points,
    )
    print(f"Completed {spec.rows} x {spec.cols} grid; cached={grid.cached_cells}, fetched={grid.fetched_cells}")
    if reference is not None:
        print(compare_reference(grid, reference))
        print("Numerical comparison only; source equivalence and acceptance tolerance require independent review.")
    if output is not None:
        if output.suffix.lower() == ".asc":
            values = grid.depths_mm if options.reference_depth_mm is None else grid.multipliers(options.reference_depth_mm)
            write_ascii_grid(output, spec, values)
        else:
            write_geotiff(output, grid, reference_depth_mm=options.reference_depth_mm)
        print(f"Created {output}")


def main(argv: Sequence[str] | None = None) -> int:
    options = _Options()
    _parser().parse_args(argv, namespace=options)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, wrapper_source=_WRAPPER_SOURCE)
    try:
        _run(options)
    except (OSError, ValueError, ArithmeticError, RuntimeError, sqlite3.Error, RasterioError) as exc:
        print(f"Spatial IFD failed: {exc}")
        return 1
    finally:
        print_wrapper_banner(
            wrapper_file=Path(__file__),
            wrapper_version=WRAPPER_VERSION,
            wrapper_source=_WRAPPER_SOURCE,
            leading_blank_line=True,
        )
        if options.pause:
            pause_console()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
