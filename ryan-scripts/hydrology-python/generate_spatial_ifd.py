"""Generate an experimental spatial IFD raster or compare with a reference.

This is an offline-first research wrapper. Verify the real ARR source key,
baseline, units, datum and permitted request budget before downloading data.
Set MAX_NEW_POINTS or --max-new-points explicitly for any network access.
The supplied reference raster must be depth in mm for the selected event.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

WRAPPER_VERSION = "2026-11-28.4"
WORKING_DIR = Path.cwd()
REFERENCE: Path | None = None
ASSUME_WGS84 = False  # Legacy option for a confirmed WGS84 reference.
GRID_CRS: str | None = None  # Explicit grids default to GDA94; references retain their CRS.
REQUEST_CRS: str | None = None  # Explicit ARR scalar-coordinate convention for GDA94 grids.
NORTH_LAT: Decimal | None = None
WEST_LON: Decimal | None = None
ROWS: int | None = None
COLS: int | None = None
SPACING = Decimal("0.025")
SOURCE_PATH: list[str] = []
SNAPSHOT = ""
CACHE = Path.home() / ".ryan-tools" / "spatial-ifd" / "cache.sqlite3"
DURATION_MIN = [60]
AEP_PERCENT = [1.0]
INSPECT_CACHE = False
MAX_NEW_POINTS = 0
OUTPUT: Path | None = None
REFERENCE_DEPTH_MM: float | None = None
CATCHMENT: Path | None = None  # Polygon layer in grid CRS; normalize to each event's catchment mean.
CATCHMENT_LAYER: str | None = None
PAUSE_ON_FINISH = False

import argparse
import sqlite3
from collections.abc import Sequence
from math import isfinite

from fiona.errors import FionaError
from rasterio.errors import RasterioError
from shapely.errors import ShapelyError

from ryan_library.functions.hydrology.spatial_ifd import (
    GDA94,
    SUPPORTED_GRID_CRS,
    GridSpec,
    IFDEvent,
    IFDSource,
    PointCache,
    SpatialIFDGrid,
    collect_grids,
    inspect_cache,
    write_ascii_grid,
)
from ryan_library.functions.hydrology.spatial_ifd_catchments import (
    CatchmentWeights,
    catchment_weights,
    read_catchment_polygon,
)
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
    crs: str | None
    request_crs: str | None
    north_lat: Decimal | None
    west_lon: Decimal | None
    rows: int | None
    cols: int | None
    spacing: Decimal
    source_path: list[str]
    snapshot: str
    cache: Path
    duration_min: list[int]
    aep_percent: list[float]
    inspect_cache: bool
    max_new_points: int
    output: Path | None
    reference_depth_mm: float | None
    catchment: Path | None
    catchment_layer: str | None
    pause: bool


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=WRAPPER_VERSION)
    parser.add_argument("--working-dir", type=Path, default=WORKING_DIR)
    parser.add_argument(
        "--reference", type=Path, default=REFERENCE, help="Reference depth raster for the selected event"
    )
    parser.add_argument("--assume-wgs84", action=argparse.BooleanOptionalAction, default=ASSUME_WGS84)
    parser.add_argument(
        "--crs",
        choices=SUPPORTED_GRID_CRS,
        default=GRID_CRS,
        help="Explicit grid CRS (default EPSG:4283), or confirmed CRS for an unreferenced raster",
    )
    parser.add_argument(
        "--request-crs", choices=SUPPORTED_GRID_CRS, default=REQUEST_CRS, help="ARR point datum assumption"
    )
    parser.add_argument("--north-lat", type=Decimal, default=NORTH_LAT, help="Latitude of north-west cell centre")
    parser.add_argument("--west-lon", type=Decimal, default=WEST_LON, help="Longitude of north-west cell centre")
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--cols", type=int, default=COLS)
    parser.add_argument("--spacing", type=Decimal, default=SPACING, help="Angular spacing; ignored with --reference")
    parser.add_argument("--source-path", nargs="+", default=SOURCE_PATH, help="Exact layer/table keys")
    parser.add_argument("--snapshot", default=SNAPSHOT, help="Local immutable cache namespace")
    parser.add_argument("--cache", type=Path, default=CACHE)
    parser.add_argument("--duration-min", type=int, nargs="+", default=DURATION_MIN)
    parser.add_argument("--aep-percent", type=float, nargs="+", default=AEP_PERCENT)
    parser.add_argument("--inspect-cache", action=argparse.BooleanOptionalAction, default=INSPECT_CACHE)
    parser.add_argument("--max-new-points", type=int, default=MAX_NEW_POINTS, help="Default zero: no network access")
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help="New .asc/.tif file (one event) or existing output directory (batch)",
    )
    parser.add_argument(
        "--reference-depth-mm", type=float, default=REFERENCE_DEPTH_MM, help="Export depth ratios instead"
    )
    parser.add_argument("--catchment", type=Path, default=CATCHMENT, help="Polygon in grid CRS for ratios")
    parser.add_argument("--catchment-layer", default=CATCHMENT_LAYER, help="Required when the file has multiple layers")
    parser.add_argument("--pause", action=argparse.BooleanOptionalAction, default=PAUSE_ON_FINISH)
    return parser


def _spec(options: _Options) -> tuple[GridSpec, ReferenceGrid | None]:
    if options.reference is not None:
        if any(value is not None for value in (options.north_lat, options.west_lon, options.rows, options.cols)):
            msg = "Choose the reference raster OR explicit grid geometry, not both"
            raise ValueError(msg)
        reference = read_reference_raster(
            (options.working_dir / options.reference).absolute(),
            assume_crs=options.crs,
            assume_wgs84=options.assume_wgs84,
        )
        return reference.spec, reference
    if options.north_lat is None or options.west_lon is None or options.rows is None or options.cols is None:
        msg = "Set a reference raster or all four explicit grid geometry settings"
        raise ValueError(msg)
    if options.assume_wgs84:
        msg = "--assume-wgs84 applies to a reference raster; use --crs for explicit grid geometry"
        raise ValueError(msg)
    return (
        GridSpec(
            options.north_lat, options.west_lon, options.rows, options.cols, options.spacing, options.crs or GDA94
        ),
        None,
    )


def _events(options: _Options) -> tuple[IFDEvent, ...]:
    events = tuple(IFDEvent(duration, aep) for duration in options.duration_min for aep in options.aep_percent)
    if not events or len(set(events)) != len(events):
        msg = "Choose nonempty, unique durations and AEPs"
        raise ValueError(msg)
    if options.reference is not None and len(events) != 1:
        msg = "A reference depth raster is specific to one event"
        raise ValueError(msg)
    return events


def _output_paths(options: _Options, events: tuple[IFDEvent, ...]) -> tuple[Path, ...]:
    if options.output is None:
        if options.reference is None and not options.inspect_cache:
            msg = "Set an output or reference raster, or inspect the cache"
            raise ValueError(msg)
        return ()
    path = (options.working_dir / options.output).absolute()
    if len(events) == 1:
        outputs = (path,)
    else:
        if not path.is_dir():
            msg = "Batch output must be an existing directory"
            raise ValueError(msg)
        prefix = "depth" if options.reference_depth_mm is None and options.catchment is None else "factor"
        outputs = tuple(path / f"{prefix}_{event.aep_percent}pct_{event.duration_min}min.tif" for event in events)
    for output in outputs:
        if output.exists() or output.is_symlink():
            raise FileExistsError(output)
        if output.suffix.lower() not in {".asc", ".tif", ".tiff"} or not output.parent.is_dir():
            msg = "Output must be .asc or .tif/.tiff in an existing directory"
            raise ValueError(msg)
    return outputs


def _write_output(path: Path, grid: SpatialIFDGrid, reference_depth_mm: float | None, area_m2: float | None) -> None:
    if path.suffix.lower() == ".asc":
        values = grid.depths_mm if reference_depth_mm is None else grid.multipliers(reference_depth_mm)
        write_ascii_grid(path, grid.spec, values)
    else:
        write_geotiff(path, grid, reference_depth_mm=reference_depth_mm, catchment_area_m2=area_m2)
    print(f"Created {path}")


def _weights(options: _Options, spec: GridSpec) -> CatchmentWeights | None:
    if options.catchment is None:
        if options.catchment_layer is not None:
            msg = "A catchment layer requires a catchment file"
            raise ValueError(msg)
        return None
    if options.reference_depth_mm is not None:
        msg = "Choose either a fixed reference depth or catchment normalization"
        raise ValueError(msg)
    polygon = read_catchment_polygon(
        (options.working_dir / options.catchment).absolute(), layer=options.catchment_layer, crs=spec.crs
    )
    return catchment_weights(spec, polygon)


def _run(options: _Options) -> None:
    spec, reference = _spec(options)
    events = _events(options)
    weights = _weights(options, spec)
    source = IFDSource(tuple(options.source_path), options.snapshot, options.request_crs)
    print(f"Grid CRS: {spec.crs}; ARR request CRS assumption: {source.effective_request_crs(spec)}")
    cache = PointCache((options.working_dir / options.cache).absolute())
    if options.inspect_cache:
        coverage = inspect_cache(spec, source, cache, events)
        print(f"Cached: {coverage.cached_cells}; missing: {len(coverage.missing_points)}; network disabled")
        for point in coverage.missing_points:
            print(f"Missing request coordinates: {point.latitude}, {point.longitude}")
        return
    outputs = _output_paths(options, events)
    if options.reference_depth_mm is not None and (
        not isfinite(options.reference_depth_mm) or options.reference_depth_mm <= 0
    ):
        msg = "Reference depth must be finite and positive"
        raise ValueError(msg)
    grids = collect_grids(spec, source, cache, events, max_new_points=options.max_new_points)
    print(f"Completed {spec.rows} x {spec.cols} grid for {len(grids)} events")
    print(f"Cached={grids[0].cached_cells}, fetched={grids[0].fetched_cells}")
    if reference is not None:
        print(compare_reference(grids[0], reference))
        print("Numerical comparison only; source equivalence and acceptance tolerance require independent review.")
    if outputs:
        for output, grid in zip(outputs, grids, strict=True):
            depth = options.reference_depth_mm if weights is None else weights.mean_depth(grid)
            area = None if weights is None else weights.catchment_area_m2
            if weights is not None:
                print(f"{grid.aep_percent}% / {grid.duration_min} min: catchment mean = {depth} mm, area = {area} m²")
            _write_output(output, grid, depth, area)


def main(argv: Sequence[str] | None = None) -> int:
    options = _Options()
    _parser().parse_args(argv, namespace=options)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, wrapper_source=_WRAPPER_SOURCE)
    try:
        _run(options)
    except (
        OSError,
        ValueError,
        ArithmeticError,
        RuntimeError,
        sqlite3.Error,
        RasterioError,
        FionaError,
        ShapelyError,
    ) as exc:
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
